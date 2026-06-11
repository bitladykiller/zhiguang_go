package bootstrap

import (
	"github.com/zhiguang/app/internal/messaging"
	"github.com/zhiguang/app/internal/outbox"
	"github.com/zhiguang/app/internal/search"
	"github.com/zhiguang/app/internal/server"
)

// searchCounterDeps 描述搜索领域真正需要的计数能力。
//
// 这里故意使用窄接口，而不是直接依赖整个 counter service，
// 避免搜索领域被不相关的写路径行为耦合进去。
type searchCounterDeps interface {
	search.CounterReader
	search.SearchCounterClient
}

// SearchModule 汇总搜索模块的装配结果。
//
// 搜索模块同时包含同步查询 handler 和异步索引投影消费者。
type SearchModule struct {
	Handler      server.RouteRegistrar
	OutboxRunner server.BackgroundRunner
}

// BuildSearchModule 构建搜索领域及其 outbox 消费者。
//
// 搜索链路拆成两个部分：
//   - SearchService：面向用户的查询接口
//   - KnowPostProjector：消费 outbox 并把帖子状态投影到 ES
func BuildSearchModule(infra *InfraDeps, counterReader searchCounterDeps) SearchModule {
	searchSvc := buildSearchService(infra.Config, infra.Logger, counterReader)

	var handlerSvc search.SearchUseCase
	if searchSvc != nil {
		handlerSvc = searchSvc
	}

	projector := search.NewKnowPostProjector(infra.DB, searchSvc, counterReader)
	consumer := search.NewOutboxConsumer(
		messaging.NewKafkaReaderWithGroup(&infra.Config.Kafka, outbox.CanalOutboxTopic, outbox.SearchOutboxConsumerGroup),
		projector,
		infra.Redis,
		infra.Logger,
	)

	return SearchModule{
		Handler:      search.NewSearchHandler(handlerSvc),
		OutboxRunner: consumer,
	}
}
