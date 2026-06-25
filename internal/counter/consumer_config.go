package counter

import (
	"time"

	"github.com/segmentio/kafka-go"

	"github.com/zhiguang/app/pkg/config"
)

// consumerConfig 封装 AggregationConsumer 的配置参数。
type consumerConfig struct {
	groupID          string
	topic            string
	batchSize        int
	flushInterval    time.Duration
	flushRetryDelay  time.Duration
	flushMaxAttempts int
	repairEnabled    bool
	repairInterval   time.Duration
	repairBatch      int
}

// newConsumerConfig 从 kafka.Reader 和 config.CounterConfig 构建消费者配置。
//
// 参数:
//   - reader: *kafka.Reader，用于提取 groupID 和 topic
//   - cfg: *config.CounterConfig，可选配置；为 nil 时使用默认值
//
// 返回值:
//   - *consumerConfig: 已填充的消费者配置
func newConsumerConfig(reader *kafka.Reader, cfg *config.CounterConfig) *consumerConfig {
	batchSize := defaultBatchSize
	flushInterval := defaultFlushInterval
	repairEnabled := false
	repairInterval := defaultRepairInterval
	repairBatch := batchSize

	if cfg != nil {
		if cfg.Consumer.BatchSize > 0 {
			batchSize = cfg.Consumer.BatchSize
		}
		if cfg.Consumer.FlushIntervalMs > 0 {
			flushInterval = time.Duration(cfg.Consumer.FlushIntervalMs) * time.Millisecond
		}
		repairEnabled = cfg.Repair.Enabled
		if cfg.Repair.IntervalMs > 0 {
			repairInterval = time.Duration(cfg.Repair.IntervalMs) * time.Millisecond
		}
		if cfg.Repair.BatchSize > 0 {
			repairBatch = cfg.Repair.BatchSize
		}
	}

	readerCfg := reader.Config()

	return &consumerConfig{
		groupID:          readerCfg.GroupID,
		topic:            readerCfg.Topic,
		batchSize:        batchSize,
		flushInterval:    flushInterval,
		flushRetryDelay:  defaultCounterFlushRetryDelay,
		flushMaxAttempts: defaultCounterFlushMaxAttempts,
		repairEnabled:    repairEnabled,
		repairInterval:   repairInterval,
		repairBatch:      repairBatch,
	}
}
