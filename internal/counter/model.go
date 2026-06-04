// counter 包实现基于 Redis 的 SDS（Simple Dynamic String）计数系统，
// 并利用位图片段支持点赞/收藏/关注等状态的原子切换。
//
// 架构：
//   - Bitmap shards：每个用户被映射到一个 chunk+bit，用于预聚合
//   - SDS binary：由 5 个 32 位大端整数构成（like、fav、follower、following、posts）
//   - Lua atomic toggle：在一次 Redis 调用中完成 GETBIT+SETBIT，并产出 Kafka 事件
//   - Rebuild：当 SDS 缺失或损坏时，从位图片段重建，并配合退避与锁控制
package counter

import "fmt"

// CounterEvent 表示发往 Kafka、供异步聚合消费的计数变更事件。
type CounterEvent struct {
	EntityType string `json:"entity_type"`
	EntityID   string `json:"entity_id"`
	Metric     string `json:"metric"`
	Index      int    `json:"index"`
	UserID     uint64 `json:"user_id"`
	Delta      int    `json:"delta"` // +1 or -1
}

// SDS 布局常量：共 5 个指标，每个指标占 4 字节（大端 uint32）。
const (
	SchemaLen    = 5
	FieldSize    = 4
	IdxLike      = 0
	IdxFav       = 1
	IdxFollower  = 2
	IdxFollowing = 3
	IdxPosts     = 4
)

// NameToIdx 把指标名称映射到其在 SDS 中的槽位索引。
var NameToIdx = map[string]int{
	"like": IdxLike, "fav": IdxFav,
	"follower": IdxFollower, "following": IdxFollowing,
	"posts": IdxPosts,
}

// 位图片段常量：每个分片最多容纳 65536 个用户。
const ChunkSize = 1 << 16

// ChunkOf 返回某个用户 ID 对应的分片索引。
func ChunkOf(userID uint64) uint64 { return userID / ChunkSize }

// BitOf 返回某个用户 ID 在分片内的位偏移。
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
