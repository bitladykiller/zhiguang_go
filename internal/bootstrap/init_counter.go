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

// initCounter 创建计数器模块的完整服务栈。
//
// 创建顺序：
//   1. CounterEventProducer（Kafka 事件发布）
//   2. CounterService（Redis 位图 + SDS + Lua 脚本，构造时注入所有依赖）
//   3. AggregationConsumer（Kafka 消费者，批量聚合计数器事件）
//   4. CounterHandler（HTTP 请求适配）
//
// 返回：
//   - *counter.CounterHandler: HTTP handler
//   - *counter.CounterService: 计数器服务（供 SetCounterClient 注入到其他模块）
//   - *counter.AggregationConsumer: 后台消费者（需 Start）
//   - error: 初始化失败时返回
func initCounter(
	db *sqlx.DB,
	redisClient *redis.Client,
	kafkaWriter *kafka.Writer,
	idGen *idgen.SnowflakeGenerator,
	cfg *config.Config,
	logger *zap.Logger,
) (*counter.CounterHandler, *counter.CounterService, *counter.AggregationConsumer, error) {
	counterProducer := counter.NewCounterEventProducer(kafkaWriter)
	counterSvc := counter.NewCounterService(
		redisClient,
		counterProducer,
		&cfg.Counter,
		counter.NewCounterFailedMessageRepository(db),
		cfg.Kafka.Topics.CounterEvents,
		idGen,
		logger,
	)
	counterAggConsumer := counter.NewAggregationConsumer(
		messaging.NewKafkaReaderWithGroup(&cfg.Kafka, cfg.Kafka.Topics.CounterEvents, cfg.Kafka.ConsumerGroup),
		counterSvc,
		logger,
		&cfg.Counter,
	)
	counterHandler := counter.NewCounterHandler(counterSvc)

	return counterHandler, counterSvc, counterAggConsumer, nil
}