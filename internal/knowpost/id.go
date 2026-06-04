package knowpost

import (
	"sync"

	"github.com/bwmarrin/snowflake"
)

// SnowflakeIdGenerator 对 bwmarrin/snowflake 做了一层封装，
// 用于生成全局唯一且按时间有序的 64 位 ID，供知文主键和 outbox 事件 ID 使用。
//
// 雪花 ID 的优点：
//   - 全局唯一，无需依赖数据库自增主键。
//   - 按时间有序，天然适合作为时间排序的索引。
//   - 64 位整数，在 MySQL BIGINT 范围内，索引性能好。
//
// WHY 加锁（sync.Mutex）：
// bwmarrin/snowflake 的 Generate() 在并发调用下也是线程安全的，
// 但双重加锁可以确保在多核环境下绝对不会出现 ID 冲突。
// 在目前 QPS 不高的情况下，加锁的性能损耗可忽略不计。
type SnowflakeIdGenerator struct {
	node *snowflake.Node
	mu   sync.Mutex
}

// NewSnowflakeIdGenerator 创建一个 worker ID 为 1 的雪花 ID 生成器。
// 在分布式部署时，不同实例应使用不同的 worker ID。
func NewSnowflakeIdGenerator() (*SnowflakeIdGenerator, error) {
	node, err := snowflake.NewNode(1)
	if err != nil {
		return nil, err
	}
	return &SnowflakeIdGenerator{node: node}, nil
}

// NextID 返回下一个雪花 ID。
func (g *SnowflakeIdGenerator) NextID() uint64 {
	g.mu.Lock()
	defer g.mu.Unlock()
	return uint64(g.node.Generate())
}
