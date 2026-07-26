package knowpost

import (
	"github.com/zhiguang/app/pkg/config"
	"github.com/zhiguang/app/pkg/idgen"
)

// SnowflakeIdGenerator 是公共本地雪花生成器在 knowpost 域内的类型别名。
//
// WHY 使用类型别名：
//   - knowpost 仍然保留原有 `SnowflakeIdGenerator` 语义，最小化业务代码改动。
//   - 真正的 machine_id + worker_id 组合逻辑沉到 `pkg/idgen`，供 relation 等其他业务域复用。
type SnowflakeIdGenerator = idgen.SnowflakeGenerator

// NewSnowflakeIdGenerator 创建一个基于本地 machine_id + worker_id 的雪花 ID 生成器。
func NewSnowflakeIdGenerator(cfg *config.IDGeneratorConfig) (*SnowflakeIdGenerator, error) {
	return idgen.NewSnowflakeGenerator(cfg)
}
