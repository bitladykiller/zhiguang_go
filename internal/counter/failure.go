package counter

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/segmentio/kafka-go"
	"go.uber.org/zap"
)

func (s *CounterService) publishCounterEvent(ctx context.Context, event *CounterEvent) {
	if s == nil || s.producer == nil || event == nil {
		return
	}

	if err := s.producer.Publish(ctx, event); err != nil {
		recordCtx, cancel := context.WithTimeout(ctx, time.Second)
		defer cancel()
		if mdErr := s.markDirty(recordCtx, event.EntityType, event.EntityID); mdErr != nil {
			zap.L().Warn("counter: mark dirty failed",
				zap.String("entity_type", event.EntityType),
				zap.String("entity_id", event.EntityID),
				zap.Error(mdErr))
		}
		if rfErr := s.recordFailedEvent(recordCtx, counterFailureStagePublish, event, err); rfErr != nil {
			zap.L().Warn("counter: record failed event failed",
				zap.String("entity_type", event.EntityType),
				zap.String("entity_id", event.EntityID),
				zap.Error(rfErr))
		}
	}
}

func (s *CounterService) markDirty(ctx context.Context, entityType, entityID string) error {
	return s.redis.SAdd(ctx, DirtySetKey(), DirtyMember(entityType, entityID)).Err()
}

// toStringArgs 将字符串切片转换为 any 切片，用于 Redis SAdd/SRem 的参数传递。
func toStringArgs(members []string) []any {
	args := make([]any, len(members))
	for i, m := range members {
		args[i] = m
	}
	return args
}

func (s *CounterService) markDirtyMembers(ctx context.Context, members []string) error {
	if len(members) == 0 {
		return nil
	}
	return s.redis.SAdd(ctx, DirtySetKey(), toStringArgs(members)...).Err()
}

func (s *CounterService) clearDirtyMembers(ctx context.Context, members []string) error {
	if len(members) == 0 {
		return nil
	}
	return s.redis.SRem(ctx, DirtySetKey(), toStringArgs(members)...).Err()
}

func (s *CounterService) recordFailedEvent(ctx context.Context, stage string, event *CounterEvent, cause error) error {
	if s == nil || s.failureRecorder == nil || event == nil {
		return nil
	}

	payload, err := json.Marshal(event)
	if err != nil {
		return fmt.Errorf("record failed event: marshal: %w", err)
	}

	return s.failureRecorder.Create(ctx, &CounterFailedMessage{
		Stage:        stage,
		Topic:        s.failureTopic,
		MessageKey:   event.EntityType + ":" + event.EntityID,
		EntityType:   event.EntityType,
		EntityID:     event.EntityID,
		Metric:       event.Metric,
		Delta:        event.Delta,
		Payload:      string(payload),
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

const maxErrorMessageLen = 1024

func failureErrorMessage(cause error) string {
	if cause == nil {
		return ""
	}
	message := cause.Error()
	if len(message) > maxErrorMessageLen {
		return message[:maxErrorMessageLen]
	}
	return message
}

func (s *CounterService) nextMessageID() uint64 {
	if s == nil || s.messageIDGenerator == nil {
		return 0
	}
	return s.messageIDGenerator.NextID()
}
