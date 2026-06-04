// Package messaging 提供 Kafka 生产者与消费者的创建工厂。
package messaging

import (
	"github.com/segmentio/kafka-go"
	"github.com/zhiguang/app/pkg/config"
)

// NewKafkaWriter 创建一个 Kafka Writer，默认用于计数事件 topic。
// 这里启用异步写入，以提升吞吐能力。
func NewKafkaWriter(cfg *config.KafkaConfig) *kafka.Writer {
	return NewTopicWriter(cfg, cfg.Topics.CounterEvents, true)
}

// NewTopicWriter 为指定 topic 创建 Kafka Writer。
func NewTopicWriter(cfg *config.KafkaConfig, topic string, async bool) *kafka.Writer {
	return &kafka.Writer{
		Addr:     kafka.TCP(cfg.Brokers...),
		Topic:    topic,
		Balancer: &kafka.LeastBytes{},
		Async:    async,
	}
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
