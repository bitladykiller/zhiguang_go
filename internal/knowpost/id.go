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
//
// 功能：初始化 bwmarrin/snowflake 库的 Node 实例。
// 在分布式部署时，不同服务实例应使用不同的 worker ID（通过配置注入），
// 以确保 ID 生成的唯一性。
//
// bwmarrin/snowflake 库说明：
//   - snowflake.NewNode(node int64) 创建一个 worker 节点。
//   - 雪花 ID 由 41 位时间戳 + 10 位 worker ID + 12 位序列号组成。
//   - Generate() 方法返回一个 int64 类型的唯一 ID。
//   - 线程安全（内部使用原子操作加锁）。
//
// 参数：
//   - 无（当前硬编码 worker ID 为 1，后续可扩展为从配置读取）。
//
// 返回值：
//   - *SnowflakeIdGenerator: 创建成功的生成器实例。
//   - error: 如果 snowflake.NewNode 失败（极少情况，通常是 worker ID 超出范围）。
//
// 边界情况：
//   - 如果 NewNode(1) 失败，说明 worker ID 1 超出范围（理论不可能，除非库 bug）。
func NewSnowflakeIdGenerator() (*SnowflakeIdGenerator, error) {
	node, err := snowflake.NewNode(1)
	if err != nil {
		return nil, err
	}
	return &SnowflakeIdGenerator{node: node}, nil
}

// NextID 返回下一个雪花算法生成的唯一 ID。
//
// 功能：调用 bwmarrin/snowflake 的 Generate() 生成 int64 ID，
// 然后转换为 uint64 返回。
//
// WHY 加 sync.Mutex：
// bwmarrin/snowflake 的 Generate() 方法内部使用原子操作，
// 本身是线程安全的。此处额外加锁是为了确保在多核高并发场景下
// 绝对不会出现 ID 重复。在知文的 QPS 不高的情况下，加锁的性能损耗完全可以忽略。
//
// 返回值类型选择（uint64 而非 int64）：
// 知乎的知文 ID 在业务层定义为 uint64（无符号 64 位整数），
// 这与 MySQL BIGINT UNSIGNED 类型对齐。
// snowflake.Generate() 返回 int64，转换为 uint64 不会丢失信息
// 因为雪花 ID 的最高位始终为 0（正数）。
//
// 返回值：
//   - uint64: 全局唯一且按时间递增的 64 位 ID。
func (g *SnowflakeIdGenerator) NextID() uint64 {
	g.mu.Lock()
	defer g.mu.Unlock()
	return uint64(g.node.Generate())
}
