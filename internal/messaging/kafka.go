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
//
// 功能：
//   创建一个配置了计数事件 topic 的 Kafka 生产者。
//   启用异步写入模式以提升吞吐能力。
//
// 参数：
//   - cfg: Kafka 全局配置（Broker 地址列表、topic 名称映射等）
//
// 返回值：
//   - *kafka.Writer: 用于向计数事件 topic 写入消息的生产者实例
//
// 函数调用说明：
//   - NewTopicWriter(cfg, cfg.Topics.CounterEvents, true):
//     委托 NewTopicWriter 创建 writer。async=true 表示异步写入。
//
// 设计决策：
//   计数事件采用异步模式（不等待 broker 确认），
//   因为位图是计数数据的权威数据源，Kafka 事件只是辅助异步聚合。
//   异步丢失计数事件不会造成数据不一致。
func NewKafkaWriter(cfg *config.KafkaConfig) *kafka.Writer {
	return NewTopicWriter(cfg, cfg.Topics.CounterEvents, true)
}

// NewTopicWriter 为指定 topic 创建 Kafka Writer。
//
// 功能：
//   根据 topic 类型选择不同的写入保证策略：
//
//   outbox 主题（高可靠性）：
//     - RequiredAcks = RequireAll: 等待所有 ISR 副本确认
//     - MaxAttempts = 10: 最多重试 10 次
//     - WriteBackoff = 200ms~2s: 重试间隔
//     - Async = false: 同步写入，确保数据不丢
//
//   counter 主题（低保证）：
//     - RequiredAcks = RequireNone: 不等待确认
//     - MaxAttempts = 3: 少量重试
//     - WriteBackoff = 100ms~500ms: 短重试间隔
//     - Async = true: 异步写入（由调用方传入）
//
// 参数：
//   - cfg:   Kafka 全局配置
//   - topic: 目标 topic 名称
//   - async: 是否启用异步模式
//
// 返回值：
//   - *kafka.Writer: 配置完毕的 Kafka 生产者
//
// 函数调用说明（kafka-go Writer）：
//   - kafka.Writer{...}:
//     kafka-go 的 Writer 结构体，用于写入消息到 Kafka。
//     通过字段配置控制写入行为：
//     - Addr: 通过 kafka.TCP(brokers...) 创建 broker 地址列表
//     - Topic: 默认写入的 topic
//     - Balancer: 分区选择器，LeastBytes 选择数据量最小的分区
//     - Async: true=异步（Send 返回后不等确认），false=同步
//     - RequiredAcks: 写入确认级别
//       * RequireNone (0): 不需要确认（最快，可能丢数据）
//       * RequireOne (1): leader 确认（默认）
//       * RequireAll (-1): 所有 ISR 副本确认（最安全）
//     - MaxAttempts: 写入失败时的最大重试次数
//     - WriteBackoffMin/Max: 重试退避的间隔范围
//     - AllowAutoTopicCreation: 是否允许自动创建 topic
//   - kafka.TCP(brokers...):
//     创建 TCP 网络地址，用于连接 Kafka broker。
//
// 设计决策：
//   不同类型 topic 的不同保证策略体现了"为不同数据选择不同可靠性级别"的设计原则：
//   outbox 链路是搜索索引和关系数据的消费源头，必须至少一次交付。
//   计数事件可以偶尔丢失，因为位图是权威数据源，重启后会自动同步。
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

// NewKafkaReader 为给定 topic 创建 Kafka Reader（使用默认 consumer group）。
//
// 功能：
//   从配置中获取默认的 ConsumerGroup，委托 NewKafkaReaderWithGroup 创建 Reader。
//
// 参数：
//   - cfg:   Kafka 全局配置
//   - topic: 需要消费的 topic 名称
//
// 返回值：
//   - *kafka.Reader: Kafka 消费者实例
//
// 函数调用说明：
//   - NewKafkaReaderWithGroup(cfg, topic, cfg.ConsumerGroup):
//     使用配置中的默认 consumer group 创建 Reader。
func NewKafkaReader(cfg *config.KafkaConfig, topic string) *kafka.Reader {
	return NewKafkaReaderWithGroup(cfg, topic, cfg.ConsumerGroup)
}

// NewKafkaReaderWithGroup 为指定 topic 和 consumer group 创建 Kafka Reader。
//
// 功能：
//   使用 kafka-go 的 NewReader 创建消费者实例，通过 consumer group 进行协调消费。
//   同一 group 内的消费者实例会分担分区消费（水平扩展）。
//
// 参数：
//   - cfg:     Kafka 全局配置
//   - topic:   需要消费的 topic 名称
//   - groupID: consumer group ID（同一 group 的消费者共享分区分配）
//
// 返回值：
//   - *kafka.Reader: Kafka 消费者实例
//
// 函数调用说明（kafka-go Reader）：
//   - kafka.NewReader(config):
//     创建 Kafka 消费者。kafka.ReaderConfig 包含：
//     - Brokers: broker 地址列表
//     - GroupID: consumer group ID。不为空时使用 consumer group 协调模式，
//       为空时使用简单消费模式（需要自行管理分区和偏移量）。
//     - Topic: 消费的 topic 名称
//     - MinBytes: 每次 Fetch 请求的最小字节数（默认 1）。
//       设为 10KB 避免频繁的 Fetch 请求（减少网络往返）。
//     - MaxBytes: 每次 Fetch 请求的最大字节数（默认 10MB）。
//       设为 10MB 可以批量获取较大消息。
//
// 设计决策：
//   MinBytes=10KB 是一种权衡：增大 MinBytes 减少请求频率（降低负载），
//   但同时可能增加单条消息的延迟（因为要等缓冲区积累够 10KB 才返回）。
//   对于非实时消费场景（如搜索索引同步），这种取舍是合理的。
func NewKafkaReaderWithGroup(cfg *config.KafkaConfig, topic, groupID string) *kafka.Reader {
	return kafka.NewReader(kafka.ReaderConfig{
		Brokers:  cfg.Brokers,
		GroupID:  groupID,
		Topic:    topic,
		MinBytes: 10e3, // 10KB
		MaxBytes: 10e6, // 10MB
	})
}
