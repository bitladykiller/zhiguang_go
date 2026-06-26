package counter

import (
	"context"
	"encoding/json"
	"time"

	"github.com/segmentio/kafka-go"
)

func (s *CounterService) publishCounterEvent(ctx context.Context, event *CounterEvent) {
	if s == nil || s.producer == nil || event == nil {
		return
	}

	if err := s.producer.Publish(ctx, event); err != nil {
		recordCtx, cancel := context.WithTimeout(ctx, time.Second)
		defer cancel()
		_ = s.markDirty(recordCtx, event.EntityType, event.EntityID)
		_ = s.recordFailedEvent(recordCtx, counterFailureStagePublish, event, err)
	}
}

func (s *CounterService) markDirty(ctx context.Context, entityType, entityID string) error {
	return s.redis.SAdd(ctx, DirtySetKey(), DirtyMember(entityType, entityID)).Err()
}

func (s *CounterService) markDirtyMembers(ctx context.Context, members []string) error {
	if len(members) == 0 {
		return nil
	}

	args := make([]any, 0, len(members))
	for _, member := range members {
		args = append(args, member)
	}
	return s.redis.SAdd(ctx, DirtySetKey(), args...).Err()
}

func (s *CounterService) clearDirtyMembers(ctx context.Context, members []string) error {
	if len(members) == 0 {
		return nil
	}

	args := make([]any, 0, len(members))
	for _, member := range members {
		args = append(args, member)
	}
	return s.redis.SRem(ctx, DirtySetKey(), args...).Err()
}

func (s *CounterService) recordFailedEvent(ctx context.Context, stage string, event *CounterEvent, cause error) error {
	if s == nil || s.failureRecorder == nil || event == nil {
		return nil
	}

	payload := string(MarshalCounterEventJSON(event))

	return s.failureRecorder.Create(ctx, &CounterFailedMessage{
		Stage:        stage,
		Topic:        s.failureTopic,
		MessageKey:   event.EntityType + ":" + event.EntityID,
		EntityType:   event.EntityType,
		EntityID:     event.EntityID,
		Metric:       event.Metric,
		Delta:        event.Delta,
		Payload:      payload,
		ErrorMessage: failureErrorMessage(cause),
		RetryCount:   0,
		Status:       counterFailureStatusPending,
	})
}

func (s *CounterService) recordFailedKafkaMessages(ctx context.Context, stage string, messages []kafka.Message, cause error) error {
	if s == nil || s.failureRecorder == nil || len(messages) == 0 {
		return nil
	}

	records := make([]*CounterFailedMessage, 0, len(messages))
	for _, message := range messages {
		var event CounterEvent
		if err := json.Unmarshal(message.Value, &event); err != nil {
			continue
		}
		records = append(records, &CounterFailedMessage{
			Stage:        stage,
			Topic:        s.failureTopic,
			MessageKey:   string(message.Key),
			EntityType:   event.EntityType,
			EntityID:     event.EntityID,
			Metric:       event.Metric,
			Delta:        event.Delta,
			Payload:      string(message.Value),
			ErrorMessage: failureErrorMessage(cause),
			RetryCount:   0,
			Status:       counterFailureStatusPending,
		})
	}

	return s.failureRecorder.CreateBatch(ctx, records)
}

func failureErrorMessage(cause error) string {
	if cause == nil {
		return ""
	}
	message := cause.Error()
	if len(message) > 1024 {
		return message[:1024]
	}
	return message
}

func (s *CounterService) nextMessageID() uint64 {
	if s == nil || s.messageIDGenerator == nil {
		return 0
	}
	return s.messageIDGenerator.NextID()
}
