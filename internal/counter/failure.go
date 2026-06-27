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
		if markErr := s.markDirty(recordCtx, event.EntityType, event.EntityID); markErr != nil {
			s.logger.Warn("mark dirty after publish failed", zap.String("entityType", event.EntityType), zap.String("entityID", event.EntityID), zap.Error(markErr))
		}
		if recErr := s.recordFailedEvent(recordCtx, counterFailureStagePublish, event, err); recErr != nil {
			s.logger.Warn("record failed event after publish failed", zap.String("entityType", event.EntityType), zap.String("entityID", event.EntityID), zap.Error(recErr))
		}
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

// ReplayFailedMessages 补偿重放 pending 状态的失败消息。
//
// 流程：
//  1. 查询 pending 状态的失败记录
//  2. 对每条记录，从位图重建 SDS 快照并写回 Redis
//  3. 根据重建结果更新记录状态为 "recovered" 或 "failed"
//
// 参数:
//   - ctx: context.Context，上下文
//   - limit: int，单次处理上限
//
// 返回值:
//   - error: 查询失败时返回错误；单条重建失败不中断，只更新状态
func (s *CounterService) ReplayFailedMessages(ctx context.Context, limit int) error {
	if s == nil || s.failureRecorder == nil {
		return nil
	}

	pending, err := s.failureRecorder.ListPending(ctx, limit, 0)
	if err != nil {
		return fmt.Errorf("list pending failures: %w", err)
	}

	for _, record := range pending {
		if record == nil {
			continue
		}

		snapshot, err := s.buildSnapshotFromBitmap(ctx, record.EntityType, record.EntityID)
		if err != nil {
			_ = s.failureRecorder.UpdateStatus(ctx, record.ID, "failed", err.Error())
			continue
		}

		sdsKey := SdsKey(record.EntityType, record.EntityID)
		mapHSet := make(map[string]any, len(snapshot))
		for k, v := range snapshot {
			mapHSet[k] = v
		}
		if err := s.redis.HSet(ctx, sdsKey, mapHSet).Err(); err != nil {
			_ = s.failureRecorder.UpdateStatus(ctx, record.ID, "failed", err.Error())
			continue
		}

		_ = s.failureRecorder.UpdateStatus(ctx, record.ID, "recovered", "")
	}

	return nil
}
