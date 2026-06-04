package counter

import (
	"context"
	"encoding/json"

	"github.com/segmentio/kafka-go"
)

// CounterEventProducer 负责把计数变化事件发布到 Kafka。
//
// 计数器事件用于异步聚合。当用户点赞/收藏时，会先在 Redis 位图中完成原子切换，
// 并让 SDS 立即失效（这样下次读取计数时从位图重建），同时通过 Kafka 事件
// 做最终一致的异步聚合。
//
// 消息键设计：使用 `{entityType}:{entityID}` 作为消息键，
// 以保证同实体的所有事件进入同一分区并保持消费顺序。
// 这对于计数的一致性至关重要：如果不同分区中的点赞和取消赞乱序到达，
// 下游聚合结果可能出现偏差。
type CounterEventProducer struct {
	writer *kafka.Writer
}

func NewCounterEventProducer(writer *kafka.Writer) *CounterEventProducer {
	return &CounterEventProducer{writer: writer}
}

func (p *CounterEventProducer) Publish(event *CounterEvent) error {
	data, err := json.Marshal(event)
	if err != nil {
		return err
	}
	return p.writer.WriteMessages(context.Background(), kafka.Message{
		Key:   []byte(event.EntityType + ":" + event.EntityID),
		Value: data,
	})
}
