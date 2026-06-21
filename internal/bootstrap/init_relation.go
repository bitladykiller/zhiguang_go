package bootstrap

import (
	"github.com/jmoiron/sqlx"
	"github.com/redis/go-redis/v9"

	"github.com/zhiguang/app/internal/relation"
	"github.com/zhiguang/app/pkg/idgen"
)

// initRelation 创建关系模块的完整服务栈。
//
// 创建顺序：
//   1. RelationService（关注/取关 + 列表查询 + 事务内 outbox）
//   2. RelationHandler（HTTP 请求适配）
//
// 返回：
//   - *relation.RelationHandler: HTTP handler
//   - *relation.RelationService: 关系服务（供 EventProcessor 使用）
func initRelation(
	db *sqlx.DB,
	redisClient *redis.Client,
	idGen *idgen.SnowflakeGenerator,
) (*relation.RelationHandler, *relation.RelationService) {
	relSvc := relation.NewRelationService(db, redisClient, 10*1024*1024, idGen)
	relHandler := relation.NewRelationHandler(relSvc)
	return relHandler, relSvc
}
