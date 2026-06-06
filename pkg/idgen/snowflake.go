package idgen

import (
	"fmt"
	"sync"

	"github.com/bwmarrin/snowflake"

	"github.com/zhiguang/app/pkg/config"
)

const (
	maxMachineID = 31
	maxWorkerID  = 31
	workerBits   = 5
)

// SnowflakeGenerator 封装一个本地雪花 ID 生成器。
//
// 设计约束：
//   - ID 在本地内存中直接生成，不依赖任何远程中间件参与发号。
//   - 多实例唯一性由配置中的 machine_id + worker_id 保证。
//   - 当前实现把 10 位 node id 固定拆成 5 位 machine + 5 位 worker，
//     与常见的 datacenter/worker 拆分思路一致。
type SnowflakeGenerator struct {
	node      *snowflake.Node
	nodeID    int64
	machineID int
	workerID  int
	mu        sync.Mutex
}

// NewSnowflakeGenerator 根据 machine_id 和 worker_id 创建本地雪花生成器。
func NewSnowflakeGenerator(cfg *config.IDGeneratorConfig) (*SnowflakeGenerator, error) {
	if cfg == nil {
		return nil, fmt.Errorf("id generator config is nil")
	}
	if cfg.MachineID < 0 || cfg.MachineID > maxMachineID {
		return nil, fmt.Errorf("machine_id must be in [0,%d]", maxMachineID)
	}
	if cfg.WorkerID < 0 || cfg.WorkerID > maxWorkerID {
		return nil, fmt.Errorf("worker_id must be in [0,%d]", maxWorkerID)
	}

	nodeID := int64((cfg.MachineID << workerBits) | cfg.WorkerID)
	node, err := snowflake.NewNode(nodeID)
	if err != nil {
		return nil, err
	}

	return &SnowflakeGenerator{
		node:      node,
		nodeID:    nodeID,
		machineID: cfg.MachineID,
		workerID:  cfg.WorkerID,
	}, nil
}

// NextID 返回一个本地生成的全局唯一且按时间有序的 64 位 ID。
func (g *SnowflakeGenerator) NextID() uint64 {
	g.mu.Lock()
	defer g.mu.Unlock()
	return uint64(g.node.Generate())
}

// NodeID 返回 machine_id 和 worker_id 组合后的 10 位节点号。
func (g *SnowflakeGenerator) NodeID() int64 {
	if g == nil {
		return -1
	}
	return g.nodeID
}

// MachineID 返回当前生成器绑定的机器号。
func (g *SnowflakeGenerator) MachineID() int {
	if g == nil {
		return -1
	}
	return g.machineID
}

// WorkerID 返回当前生成器绑定的工作节点号。
func (g *SnowflakeGenerator) WorkerID() int {
	if g == nil {
		return -1
	}
	return g.workerID
}
