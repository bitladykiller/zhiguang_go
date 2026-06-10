// Package counter counter 包实现基于 Redis 的 SDS（Simple Dynamic String）计数系统，
// 并利用位图片段支持点赞/收藏/关注等状态的原子切换。
//
// 架构概述：
//
//	┌──────────┐    ┌──────────────┐    ┌──────────────┐    ┌─────────────┐
//	│ Lua 原子 │    │ Bitmap shard │    │ Kafka / MQ   │    │ SDS binary  │
//	│ 切换操作  │ -> │ (用户位图)    │ -> │ (批量增量消费) │ -> │ (正式快照)   │
//	└──────────┘    └──────────────┘    └──────────────┘    └─────────────┘
//
//	Bitmap shards：每个用户按 userID % ChunkSize 映射到一个分片中的某个 bit，
//	用于预聚合和用户维度状态查询。
//
//	SDS binary：由 5 个 32 位大端整数构成
//	（like、fav、follower、following、posts），总计 20 字节。
//	它是 Redis 中的正式快照，用于快速读取实体计数。
//
//	Lua atomic toggle：在一次 Redis 调用中完成 GETBIT+SETBIT，
//	并在状态确实变化时发送 Kafka 事件用于异步聚合。
//
//	Repair / Rebuild：当 MQ 发布或批量 apply 失败时，
//	失败任务会先落到 MySQL，后台 worker 再从位图重新统计并修正 cnt:*。
//	读路径在发现 SDS 缺失或损坏时，也会触发按需重建。
package counter

import (
	"fmt"
	"strings"
)

// CounterEvent 表示发往 Kafka、供异步聚合消费的计数变更事件。
type CounterEvent struct {
	MessageID  uint64 `json:"message_id"`
	EntityType string `json:"entity_type"`
	EntityID   string `json:"entity_id"`
	Metric     string `json:"metric"`
	Index      int    `json:"index"` // SDS schema index（IdxLike / IdxFav ...）
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

// CounterEntityMember 把实体类型和实体 ID 拼成批量消费阶段使用的稳定键。
func CounterEntityMember(entityType, entityID string) string {
	return entityType + ":" + entityID
}

// ParseCounterEntityMember 从批量消费阶段使用的稳定键中解析出实体类型和实体 ID。
func ParseCounterEntityMember(member string) (string, string, error) {
	parts := strings.SplitN(member, ":", 2)
	if len(parts) != 2 || parts[0] == "" || parts[1] == "" {
		return "", "", fmt.Errorf("invalid counter entity member: %s", member)
	}
	return parts[0], parts[1], nil
}
