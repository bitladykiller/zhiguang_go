package knowpost

import (
	"sync"

	"github.com/bwmarrin/snowflake"
)

// SnowflakeIdGenerator 对 `bwmarrin/snowflake` 做了一层封装，
// 用于生成全局唯一且按时间有序的 64 位 ID，供知文主键和 outbox 事件 ID 使用。
type SnowflakeIdGenerator struct {
	node *snowflake.Node
	mu   sync.Mutex
}

// NewSnowflakeIdGenerator 创建一个 worker ID 为 1 的雪花 ID 生成器。
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
