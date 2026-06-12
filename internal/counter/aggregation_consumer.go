package counter

import (
	"context"
	"errors"
	"time"

	"github.com/segmentio/kafka-go"
	"go.uber.org/zap"

	"github.com/zhiguang/app/pkg/config"
)

const (
	defaultCounterFlushMaxAttempts = 3
	defaultCounterFlushRetryDelay  = time.Second
	counterShutdownFlushTimeout    = 5 * time.Second
)

var errCounterBatchCommit = errors.New("counter batch commit failed")

// AggregationConsumer 消费 counter-events，并按批次把增量折叠到 cnt:*。
//
// 当前职责按文件拆分：
//   - aggregation_consumer.go: 结构体、构造函数、启动入口
//   - aggregation_consumer_loop.go: 消费循环与批次调度
//   - aggregation_consumer_flush.go: flush/apply/commit/重试
//   - aggregation_consumer_batch.go: 批次模型与事件解析
//
// 这样拆分的目的是把“消息拉取”“批次状态机”“Redis apply 与 Kafka commit”
// 分开维护，避免异步链路继续堆成一个超大文件。
type AggregationConsumer struct {
	reader           *kafka.Reader
	service          *CounterService
	logger           *zap.Logger
	commitFn         func(ctx context.Context, msgs ...kafka.Message) error
	groupID          string
	topic            string
	batchSize        int
	flushInterval    time.Duration
	flushRetryDelay  time.Duration
	flushMaxAttempts int
}

func NewAggregationConsumer(
	reader *kafka.Reader,
	service *CounterService,
	logger *zap.Logger,
	cfg *config.CounterConfig,
) *AggregationConsumer {
	if reader == nil || service == nil || service.redis == nil {
		return nil
	}

	batchSize := 100
	flushInterval := time.Second
	if cfg != nil {
		if cfg.Consumer.BatchSize > 0 {
			batchSize = cfg.Consumer.BatchSize
		}
		if cfg.Consumer.FlushIntervalMs > 0 {
			flushInterval = time.Duration(cfg.Consumer.FlushIntervalMs) * time.Millisecond
		}
	}

	readerCfg := reader.Config()
	return &AggregationConsumer{
		reader:           reader,
		service:          service,
		logger:           logger,
		commitFn:         reader.CommitMessages,
		groupID:          readerCfg.GroupID,
		topic:            readerCfg.Topic,
		batchSize:        batchSize,
		flushInterval:    flushInterval,
		flushRetryDelay:  defaultCounterFlushRetryDelay,
		flushMaxAttempts: defaultCounterFlushMaxAttempts,
	}
}

func (c *AggregationConsumer) Start(ctx context.Context) {
	if c == nil {
		return
	}
	defer c.reader.Close()

	c.consumeLoop(ctx)
}
