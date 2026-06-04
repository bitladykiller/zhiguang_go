// Package messaging 提供 Kafka 生产者与消费者的创建工厂。
//
// 封装 segmentio/kafka-go 库的初始化逻辑，确保所有模块都能以一致的方式
// 创建 Writer 和 Reader。同一模块内的多个 writer 可以共享同一个 KafkaConfig。
//
// 设计决策：
//   - 计数事件写入使用异步模式（async=true）以提升吞吐，
//     因为计数事件可以容忍偶尔丢失（位图是权威数据源）。
//   - outbox Canal 主题的写入使用同步模式（async=false），
//     因为 outbox 的消费可靠性要求更高。
//   - 消费者使用 consumer group 做协调，支持水平扩展消费者实例。
package messaging

import (
	"time"

	"github.com/segmentio/kafka-go"
	"github.com/zhiguang/app/internal/outbox"
	"github.com/zhiguang/app/pkg/config"
)

const (
	counterWriterMaxAttempts = 3
	counterBackoffMin        = 100 * time.Millisecond
	counterBackoffMax        = 500 * time.Millisecond
	outboxWriterMaxAttempts  = 10
	outboxBackoffMin         = 200 * time.Millisecond
	outboxBackoffMax         = 2 * time.Second
)

// NewKafkaWriter 创建一个 Kafka Writer，默认用于计数事件 topic。
// 这里启用异步写入，以提升吞吐能力。
func NewKafkaWriter(cfg *config.KafkaConfig) *kafka.Writer {
	return NewTopicWriter(cfg, cfg.Topics.CounterEvents, true)
}

// NewTopicWriter 为指定 topic 创建 Kafka Writer。
func NewTopicWriter(cfg *config.KafkaConfig, topic string, async bool) *kafka.Writer {
	writer := &kafka.Writer{
		Addr:     kafka.TCP(cfg.Brokers...),
		Topic:    topic,
		Balancer: &kafka.LeastBytes{},
		Async:    async,
	}

	if topic == outbox.CanalOutboxTopic {
		// outbox 事件是异步同步链路源头，显式要求 ISR 全确认并保留默认级别的重试能力。
		writer.RequiredAcks = kafka.RequireAll
		writer.MaxAttempts = outboxWriterMaxAttempts
		writer.WriteBackoffMin = outboxBackoffMin
		writer.WriteBackoffMax = outboxBackoffMax
		writer.AllowAutoTopicCreation = false
		return writer
	}

	// counter 事件允许低保证策略：异步发送、不等待副本确认、较小重试窗口。
	writer.RequiredAcks = kafka.RequireNone
	writer.MaxAttempts = counterWriterMaxAttempts
	writer.WriteBackoffMin = counterBackoffMin
	writer.WriteBackoffMax = counterBackoffMax
	writer.AllowAutoTopicCreation = false
	return writer
}

// NewKafkaReader 为给定 topic 创建 Kafka Reader。
// 它会使用配置中的 consumer group 进行协调消费。
func NewKafkaReader(cfg *config.KafkaConfig, topic string) *kafka.Reader {
	return NewKafkaReaderWithGroup(cfg, topic, cfg.ConsumerGroup)
}

// NewKafkaReaderWithGroup 为指定 topic 和 consumer group 创建 Kafka Reader。
func NewKafkaReaderWithGroup(cfg *config.KafkaConfig, topic, groupID string) *kafka.Reader {
	return kafka.NewReader(kafka.ReaderConfig{
		Brokers:  cfg.Brokers,
		GroupID:  groupID,
		Topic:    topic,
		MinBytes: 10e3, // 10KB
		MaxBytes: 10e6, // 10MB
	})
}
