package bootstrap

import (
	"github.com/zhiguang/app/internal/messaging"
	"github.com/zhiguang/app/internal/outbox"
	"github.com/zhiguang/app/internal/relation"
	"github.com/zhiguang/app/internal/server"
)

const relationL1CacheSize = 10 * 1024 * 1024

// RelationModule 汇总关系模块的装配结果。
//
// relation 领域既有同步的关注/取关 HTTP 接口，也有异步的 outbox 消费者，
// 用于把关系变更投影到 Redis 和计数模块。
type RelationModule struct {
	Handler      server.RouteRegistrar
	OutboxRunner server.BackgroundRunner
}

// BuildRelationModule 构建关系领域。
//
// 这样设计后，InitializeApp 可以显式决定是否随着 Canal/Kafka 链路一起启动
// 关系 outbox consumer，而不是让领域内部偷偷自启 goroutine。
func BuildRelationModule(infra *InfraDeps, counterUpdater relation.UserCounterUpdater) RelationModule {
	relSvc := relation.NewRelationService(infra.DB, infra.Redis, relationL1CacheSize, infra.IDGen)
	processor := relation.NewEventProcessor(infra.Redis, counterUpdater)
	consumer := relation.NewOutboxConsumer(
		messaging.NewKafkaReaderWithGroup(&infra.Config.Kafka, outbox.CanalOutboxTopic, outbox.RelationOutboxConsumerGroup),
		processor,
		infra.Redis,
		infra.Logger,
	)

	return RelationModule{
		Handler:      relation.NewRelationHandler(relSvc),
		OutboxRunner: consumer,
	}
}
