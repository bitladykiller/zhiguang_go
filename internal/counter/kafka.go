package counter

import (
	"context"
	"encoding/json"

	"github.com/segmentio/kafka-go"
)

// CounterEventProducer 负责把计数变化事件发布到 Kafka。
// 它使用 `{entityType}:{entityID}` 作为消息键，以保证同实体事件进入同一分区并保持顺序。
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
