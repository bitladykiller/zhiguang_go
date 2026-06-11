package bootstrap

import (
	"github.com/zhiguang/app/internal/counter"
	"github.com/zhiguang/app/internal/knowpost"
	"github.com/zhiguang/app/internal/messaging"
	"github.com/zhiguang/app/internal/relation"
	"github.com/zhiguang/app/internal/search"
	"github.com/zhiguang/app/internal/server"
)

// counterCrossDomainDeps 描述其他领域真正会从 counter 领域消费的能力子集。
//
// bootstrap 只把这组接口往外传，而不是直接暴露 *counter.CounterService，
// 这样能更明确表达“别的领域依赖的是能力，不是实现细节”。
type counterCrossDomainDeps interface {
	knowpost.CounterClient
	relation.UserCounterUpdater
	search.CounterReader
	search.SearchCounterClient
}

// CounterModule 汇总计数模块的装配结果。
//
// Counter 领域既有同步 HTTP 入口，也有 Kafka 聚合消费和失败补偿两个后台任务，
// 因此这里需要同时暴露 handler、跨领域依赖和 runners。
type CounterModule struct {
	Handler server.RouteRegistrar
	Deps    counterCrossDomainDeps
	Runners []server.BackgroundRunner
}

// BuildCounterModule 构建计数领域。
//
// 当前计数链路分成三层：
//   - CounterService：位图状态判定、计数读取、消息生产
//   - AggregationConsumer：MQ 批量消费并折叠 delta 到 `cnt:*`
//   - CounterFailureWorker：处理 flush/apply 失败后落到 MySQL 的补偿任务
func BuildCounterModule(infra *InfraDeps) CounterModule {
	counterProducer := counter.NewCounterEventProducer(infra.KafkaWriter)
	counterFailureStore := counter.NewCounterFailedMessageRepository(infra.DB)
	counterSvc := counter.NewCounterService(counter.CounterServiceDeps{
		Redis:              infra.Redis,
		Producer:           counterProducer,
		Config:             &infra.Config.Counter,
		FailureRecorder:    counterFailureStore,
		FailureTopic:       infra.Config.Kafka.Topics.CounterEvents,
		MessageIDGenerator: infra.IDGen,
	})

	counterAggConsumer := counter.NewAggregationConsumer(
		messaging.NewKafkaReaderWithGroup(&infra.Config.Kafka, infra.Config.Kafka.Topics.CounterEvents, infra.Config.Kafka.ConsumerGroup),
		counterSvc,
		infra.Logger,
		&infra.Config.Counter,
	)
	counterFailureWorker := counter.NewCounterFailureWorker(counterFailureStore, counterSvc, infra.Logger, &infra.Config.Counter)

	runners := make([]server.BackgroundRunner, 0, 2)
	if counterAggConsumer != nil {
		runners = append(runners, counterAggConsumer)
	}
	if counterFailureWorker != nil {
		runners = append(runners, counterFailureWorker)
	}

	return CounterModule{
		Handler: counter.NewCounterHandler(counterSvc),
		Deps:    counterSvc,
		Runners: runners,
	}
}
