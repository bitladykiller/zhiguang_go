package counter

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/segmentio/kafka-go"
)

// CounterEventPublisher 抽象计数事件发布能力，便于测试时注入 stub。
type CounterEventPublisher interface {
	Publish(event *CounterEvent) error
}

// CounterEventProducer 负责把计数变化事件发布到 Kafka。
//
// 计数器事件用于异步聚合。当用户点赞/收藏时，会先在 Redis 位图中完成原子切换，
// 然后把 delta 发往 Kafka；消费者按批次在内存中聚合后，再批量 flush 到 cnt:*。
//
// 消息键设计：使用 `{entityType}:{entityID}` 作为消息键，
// 以保证同实体的所有事件进入同一分区并保持消费顺序。
// 这对于计数的一致性至关重要：如果不同分区中的点赞和取消赞乱序到达，
// 下游聚合结果可能出现偏差。
type CounterEventProducer struct {
	writer *kafka.Writer
}

// NewCounterEventProducer 创建计数事件 Kafka 生产者。
//
// 参数：
//   - writer: *kafka.Writer 实例，由 messaging 包统一创建。
//     当前采用同步等待 broker 确认的 writer，由调用方决定是否异步调用。
//
// 注意：
//
//	如果传入 nil writer，Publish 会返回 error，因此调用方应确保
//	writer 在 CounterService 的整个生命周期内有效。
func NewCounterEventProducer(writer *kafka.Writer) *CounterEventProducer {
	return &CounterEventProducer{writer: writer}
}

// Publish 将计数变更事件序列化为 JSON 并写入 Kafka。
//
// 功能：
//  1. 将 CounterEvent 结构体通过 json.Marshal 序列化为 JSON 字节。
//  2. 使用 {entityType}:{entityID} 作为消息键（确保同实体的事件进入同一分区）。
//  3. 通过 kafka-go Writer 的 WriteMessages 方法发送。
//
// 参数：
//   - event: 包含实体类型、实体 ID、指标、用户 ID、增量的计数变更事件
//
// 返回值：
//   - error: JSON 序列化失败或 Kafka 写入失败时返回
//
// 函数调用说明：
//   - p.writer.WriteMessages(ctx, msgs...):
//     kafka-go 库的 Writer.WriteMessages 方法将消息写入 Kafka 主题。
//     可以一次传入多条消息做批量发送。
//     在计数场景中，每条消息独立发送。当前调用方会在 goroutine 中调用 Publish，
//     因此不会把 broker ACK 延迟直接叠加到主请求路径上。
//   - kafka.Message{Key, Value}:
//     Key 用于分区路由：同一 Key 的消息进入同一分区，保证顺序消费。
//     Value 是消息体，由业务消费端反序列化使用。
//
// 设计决策：
//   - 使用 {entityType}:{entityID} 作为消息键：
//     保证同一实体的所有计数变更消息按顺序进入同一分区，
//     下游消费者可以按顺序处理 Like/Unlike 事件，避免乱序导致计数偏差。
//   - 消息体使用 JSON 序列化：
//     与 Java 版本的序列化格式保持一致，方便跨语言消费。
//
// 边界情况：
//   - event 中的 EntityID 或 EntityType 为空时，消息仍然会发送
//     （kafka-go 不会校验内容）
//   - Kafka broker 不可用时，WriteMessages 会返回连接错误，
//     调用方会把对应实体标记到 dirty set，交给后台位图修复兜底。
func (p *CounterEventProducer) Publish(event *CounterEvent) error {
	if p == nil || p.writer == nil {
		return fmt.Errorf("counter kafka writer is nil")
	}
	data, err := json.Marshal(event)
	if err != nil {
		return err
	}
	return p.writer.WriteMessages(context.Background(), kafka.Message{
		Key:   []byte(event.EntityType + ":" + event.EntityID),
		Value: data,
	})
}
