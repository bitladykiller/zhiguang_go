package bootstrap

import (
	"github.com/jmoiron/sqlx"
	"github.com/redis/go-redis/v9"
	"github.com/segmentio/kafka-go"
	"go.uber.org/zap"

	"github.com/zhiguang/app/internal/counter"
	"github.com/zhiguang/app/internal/messaging"
	"github.com/zhiguang/app/pkg/config"
	"github.com/zhiguang/app/pkg/idgen"
)

// CounterComponents 聚合计数器模块初始化的全部返回值。
//
// 用结构体包装替代 4 个离散返回值，调用方按字段名取值，
// 避免依赖参数位置顺序，提升可读性。
type CounterComponents struct {
	Handler     *counter.CounterHandler
	Service     *counter.CounterService
	AggConsumer *counter.AggregationConsumer
}

// initCounter 创建计数器模块的完整服务栈。
//
// 创建顺序：
//   1. CounterEventProducer（Kafka 事件发布）
//   2. CounterService（Redis 位图 + SDS + Lua 脚本，构造时注入所有依赖）
//   3. AggregationConsumer（Kafka 消费者，批量聚合计数器事件）
//   4. CounterHandler（HTTP 请求适配）
func initCounter(
	db *sqlx.DB,
	redisClient *redis.Client,
	kafkaWriter *kafka.Writer,
	idGen *idgen.SnowflakeGenerator,
	cfg *config.Config,
	logger *zap.Logger,
) (*CounterComponents, error) {
	counterProducer := counter.NewCounterEventProducer(kafkaWriter)
	counterSvc := counter.NewCounterService(
		redisClient,
		counterProducer,
		counter.WithRebuildLockConfig(&cfg.Counter),
		counter.WithFailureRecorder(counter.NewCounterFailedMessageRepository(db)),
		counter.WithFailureTopic(cfg.Kafka.Topics.CounterEvents),
		counter.WithMessageIDGenerator(idGen),
		counter.WithLogger(logger),
	)
	counterAggConsumer := counter.NewAggregationConsumer(
		messaging.NewKafkaReaderWithGroup(&cfg.Kafka, cfg.Kafka.Topics.CounterEvents, cfg.Kafka.ConsumerGroup),
		counterSvc,
		logger,
		&cfg.Counter,
	)
	counterHandler := counter.NewCounterHandler(counterSvc)

	return &CounterComponents{
		Handler:     counterHandler,
		Service:     counterSvc,
		AggConsumer: counterAggConsumer,
	}, nil
}
