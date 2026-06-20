package bootstrap

import (
	"go.uber.org/zap"

	"github.com/zhiguang/app/pkg/config"
	"github.com/zhiguang/app/pkg/idgen"
)

// initIDGenerator 创建雪花算法 ID 生成器。
//
// 这是 counter 和 knowpost 模块的共同依赖，在依赖拓扑中优先创建。
// 返回 *idgen.SnowflakeGenerator，同时实现了 counter.MessageIDGenerator 接口
// 和 knowpost.IDGenerator 别名。
func initIDGenerator(cfg *config.Config, logger *zap.Logger) (*idgen.SnowflakeGenerator, error) {
	idGen, err := idgen.NewSnowflakeGenerator(&cfg.IDGenerator)
	if err != nil {
		return nil, err
	}
	logger.Info("snowflake generator initialized",
		zap.Int("machine_id", idGen.MachineID()),
		zap.Int("worker_id", idGen.WorkerID()),
		zap.Int64("node_id", idGen.NodeID()),
	)
	return idGen, nil
}
