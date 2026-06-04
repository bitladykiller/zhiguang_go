// Package counter counter 包实现基于 Redis 的 SDS（Simple Dynamic String）计数系统，
// 并利用位图片段支持点赞/收藏/关注等状态的原子切换。
//
// 架构概述：
//
//	┌──────────┐    ┌──────────────┐    ┌─────────────┐
//	│ Lua 原子 │    │ Bitmap shard │    │ SDS binary  │
//	│ 切换操作  │ -> │ (用户位图)    │ -> │ (聚合计数)   │
//	└──────────┘    └──────────────┘    └─────────────┘
//
//	Bitmap shards：每个用户按 userID % ChunkSize 映射到一个分片中的某个 bit，
//	用于预聚合和用户维度状态查询。
//
//	SDS binary：由 5 个 32 位大端整数构成
//	（like、fav、follower、following、posts），总计 20 字节。
//	存储计数聚合结果，用于快速读取。
//
//	Lua atomic toggle：在一次 Redis 调用中完成 GETBIT+SETBIT，
//	并同步刷新 Kafka 事件用于异步聚合。
//
//	Rebuild：当 SDS 缺失或通过校验长度检查发现损坏时，从所有位图片段重新统计，
//	并配合指数退避与分布式锁控制重建频率，防止重建风暴。
package counter

import "fmt"

// CounterEvent 表示发往 Kafka、供异步聚合消费的计数变更事件。
type CounterEvent struct {
	EntityType string `json:"entity_type"`
	EntityID   string `json:"entity_id"`
	Metric     string `json:"metric"`
	Index      int    `json:"index"`
	UserID     uint64 `json:"user_id"`
	Delta      int    `json:"delta"` // +1 表示增加，-1 表示减少
}

// SDS 布局常量：共 5 个指标，每个指标占 4 字节（大端 uint32），总计 20 字节。
// 使用大端序（Big Endian）写入 Redis 字符串，方便通过 BITFIELD 命令做局部读写。
const (
	SchemaLen    = 5 // 指标总数
	FieldSize    = 4 // 单指标字节数（uint32）
	IdxLike      = 0 // like 槽位（点赞数）
	IdxFav       = 1 // fav 槽位（收藏数）
	IdxFollower  = 2 // follower 槽位（粉丝数）
	IdxFollowing = 3 // following 槽位（关注数）
	IdxPosts     = 4 // posts 槽位（文章数）
)

// NameToIdx 把指标名称映射到其在 SDS 中的槽位索引。
// 从 0 开始计数（IdxLike = 0、IdxFav = 1 ...），
// Lua 脚本中会做 idx+1 转换（Lua 数组索引从 1 开始）。
var NameToIdx = map[string]int{
	"like": IdxLike, "fav": IdxFav,
	"follower": IdxFollower, "following": IdxFollowing,
	"posts": IdxPosts,
}

// 位图片段常量：每个分片最多容纳 65536 个用户。
// WHY：分片是为了避免单个 bitmap 过大（超出 Redis 字符串最大值 512MB）。
// 每个 bit 位表示一个用户对某个实体的操作状态（已点赞 / 已收藏等）。
// 当用户总数超过 65536 时，ChunkOf 会返回递增的分片编号，自动分配到新分片中。
const ChunkSize = 1 << 16

// ChunkOf 返回某个用户 ID 对应的分片索引。
// 使用整数除法，保证同一用户的所有操作集中在同一个分片中。
func ChunkOf(userID uint64) uint64 { return userID / ChunkSize }

// BitOf 返回某个用户 ID 在分片内的位偏移。
// 使用取模运算，保证用户到分片内的位位置均匀分布。
func BitOf(userID uint64) uint64 { return userID % ChunkSize }

// Redis 键生成函数。
func BitmapKey(metric, entityType, entityID string, chunk uint64) string {
	return fmt.Sprintf("bm:%s:%s:%s:%d", metric, entityType, entityID, chunk)
}
func SdsKey(entityType, entityID string) string {
	return fmt.Sprintf("cnt:%s:%s", entityType, entityID)
}
func AggKey(entityType, entityID string) string {
	return fmt.Sprintf("agg:%s:%s", entityType, entityID)
}
