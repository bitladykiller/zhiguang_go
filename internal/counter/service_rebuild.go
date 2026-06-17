package counter

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/segmentio/kafka-go"
	"github.com/zhiguang/app/pkg/redislock"
)

func (s *CounterService) rebuildSds(ctx context.Context, entityType, entityID string) ([]byte, error) {
	sdsKey := SdsKey(entityType, entityID)

	if s.inBackoff(ctx, entityType, entityID) {
		return nil, fmt.Errorf("in backoff")
	}
	if !s.allowedByRateLimiter(ctx, entityType, entityID) {
		s.escalateBackoff(ctx, entityType, entityID)
		return nil, fmt.Errorf("rate limited")
	}

	lockKey := RebuildLockKey(entityType, entityID)
	lock, err := redislock.AcquireWithRetry(ctx, s.redis, lockKey, s.rebuildLockOptions, rebuildLockRetryInterval)
	if err != nil {
		s.escalateBackoff(ctx, entityType, entityID)
		return nil, fmt.Errorf("acquire rebuild lock: %w", err)
	}
	defer lock.Release()

	raw, err := s.redis.Get(ctx, sdsKey).Bytes()
	if err == nil && len(raw) == SchemaLen*FieldSize {
		s.resetBackoff(ctx, entityType, entityID)
		return raw, nil
	}

	if !lock.IsStillValid() {
		s.escalateBackoff(ctx, entityType, entityID)
		return nil, fmt.Errorf("lock lost before epoch bump")
	}

	oldEpoch, err := s.bumpEntityEpoch(ctx, entityType, entityID)
	if err != nil {
		s.escalateBackoff(ctx, entityType, entityID)
		return nil, fmt.Errorf("bump counter epoch: %w", err)
	}

	if !lock.IsStillValid() {
		s.escalateBackoff(ctx, entityType, entityID)
		return nil, fmt.Errorf("lock lost during rebuild")
	}

	raw, err = s.buildSnapshotFromBitmap(ctx, entityType, entityID)
	if err != nil {
		s.escalateBackoff(ctx, entityType, entityID)
		return nil, err
	}

	if !lock.IsStillValid() {
		s.escalateBackoff(ctx, entityType, entityID)
		return nil, fmt.Errorf("lock lost before sds write")
	}

	if err := s.redis.Set(ctx, sdsKey, raw, 0).Err(); err != nil {
		s.escalateBackoff(ctx, entityType, entityID)
		return nil, err
	}

	s.markEntityFailureTasksDoneBestEffort(ctx, entityType, entityID, oldEpoch)

	s.resetBackoff(ctx, entityType, entityID)
	return raw, nil
}

func (s *CounterService) publishCounterEvent(event *CounterEvent) error {
	if s == nil || s.producer == nil || event == nil {
		return nil
	}
	if err := s.producer.Publish(event); err != nil {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		if recordErr := s.recordFailedEvent(ctx, counterFailureStagePublish, event, err); recordErr != nil {
			return fmt.Errorf("publish event failed: %w (also failed to record: %v)", err, recordErr)
		}
		return fmt.Errorf("publish event failed (recorded for retry): %w", err)
	}
	return nil
}

func (s *CounterService) buildSnapshotFromBitmap(ctx context.Context, entityType, entityID string) ([]byte, error) {
	metrics := []string{"like", "fav", "follower", "following", "posts"}
	raw := make([]byte, SchemaLen*FieldSize)
	for i, metric := range metrics {
		total, err := s.bitCountShards(ctx, metric, entityType, entityID)
		if err != nil {
			return nil, err
		}
		writeInt32BE(raw, i*FieldSize, int32(total))
	}
	return raw, nil
}

func (s *CounterService) repairMetricFromBitmap(ctx context.Context, entityType, entityID, metric string) error {
	if s == nil || s.redis == nil {
		return fmt.Errorf("counter redis is nil")
	}

	idx, ok := NameToIdx[metric]
	if !ok {
		return fmt.Errorf("unknown metric: %s", metric)
	}

	total, err := s.bitCountShards(ctx, metric, entityType, entityID)
	if err != nil {
		return err
	}
	if total < 0 {
		total = 0
	}
	if total > int64(^uint32(0)>>1) {
		total = int64(^uint32(0) >> 1)
	}

	return s.redis.Eval(ctx, SET_SDS_FIELD_LUA, []string{SdsKey(entityType, entityID)}, SchemaLen, FieldSize, idx+1, total).Err()
}

func (s *CounterService) recordFailedEvent(ctx context.Context, stage string, event *CounterEvent, cause error) error {
	if s == nil || s.failureRecorder == nil || event == nil {
		return nil
	}

	payload, err := json.Marshal(event)
	if err != nil {
		return err
	}

	return s.failureRecorder.Create(ctx, &CounterFailedMessage{
		Stage:        stage,
		Topic:        s.failureTopic,
		MessageKey:   event.EntityType + ":" + event.EntityID,
		EntityType:   event.EntityType,
		EntityID:     event.EntityID,
		Metric:       event.Metric,
		Epoch:        event.Epoch,
		Delta:        event.Delta,
		Payload:      string(payload),
		ErrorMessage: failureErrorMessage(cause),
		RetryCount:   0,
		Status:       counterFailureStatusPending,
		NextRetryAt:  time.Now(),
	})
}

func (s *CounterService) recordFailedKafkaMessages(ctx context.Context, stage string, messages []kafka.Message, cause error) error {
	if s == nil || s.failureRecorder == nil || len(messages) == 0 {
		return nil
	}

	now := time.Now()
	recordsByKey := make(map[string]*CounterFailedMessage, len(messages))
	records := make([]*CounterFailedMessage, 0, len(messages))
	for _, message := range messages {
		var event CounterEvent
		if err := json.Unmarshal(message.Value, &event); err != nil {
			continue
		}

		if stage == counterFailureStageApply || stage == counterFailureStageFlush {
			recordKey := CounterEntityMember(event.EntityType, event.EntityID) + ":" + event.Metric + ":" + fmt.Sprintf("%d", event.Epoch)
			if record := recordsByKey[recordKey]; record != nil {
				record.Delta += event.Delta
				continue
			}
		}

		record := &CounterFailedMessage{
			Stage:        stage,
			Topic:        s.failureTopic,
			MessageKey:   string(message.Key),
			EntityType:   event.EntityType,
			EntityID:     event.EntityID,
			Metric:       event.Metric,
			Epoch:        event.Epoch,
			Delta:        event.Delta,
			Payload:      string(message.Value),
			ErrorMessage: failureErrorMessage(cause),
			RetryCount:   0,
			Status:       counterFailureStatusPending,
			NextRetryAt:  now,
		}
		records = append(records, record)
		if stage == counterFailureStageApply || stage == counterFailureStageFlush {
			recordKey := CounterEntityMember(event.EntityType, event.EntityID) + ":" + event.Metric + ":" + fmt.Sprintf("%d", event.Epoch)
			recordsByKey[recordKey] = record
		}
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

func (s *CounterService) currentEntityEpoch(ctx context.Context, entityType, entityID string) (uint64, error) {
	if s == nil || s.redis == nil {
		return 0, fmt.Errorf("counter redis is nil")
	}
	value, err := s.redis.Get(ctx, ActiveEpochKey(entityType, entityID)).Uint64()
	if err == nil {
		return value, nil
	}
	if err == redis.Nil {
		return 0, nil
	}
	return 0, err
}

func (s *CounterService) bumpEntityEpoch(ctx context.Context, entityType, entityID string) (uint64, error) {
	if s == nil || s.redis == nil {
		return 0, fmt.Errorf("counter redis is nil")
	}

	epochKey := ActiveEpochKey(entityType, entityID)
	// 使用 Lua 脚本原子递增 epoch，消除 GET + SET 之间的竞态窗口
	oldEpoch, err := bumpEpochScript.Run(ctx, s.redis, []string{epochKey}, 1).Uint64()
	if err != nil {
		return 0, fmt.Errorf("bump epoch lua: %w", err)
	}
	return oldEpoch, nil
}

func (s *CounterService) markEntityFailureTasksDoneBestEffort(ctx context.Context, entityType, entityID string, maxEpoch uint64) {
	if s == nil || s.failureTasks == nil {
		return
	}
	_, _ = s.failureTasks.MarkEntityTasksDoneThroughEpoch(ctx, entityType, entityID, maxEpoch)
}

func (s *CounterService) bitCountShards(ctx context.Context, metric, entityType, entityID string) (int64, error) {
	pattern := fmt.Sprintf("bm:%s:%s:%s:*", metric, entityType, entityID)
	keys, err := s.redis.Keys(ctx, pattern).Result()
	if err != nil {
		return 0, err
	}
	if len(keys) == 0 {
		return 0, nil
	}

	pipe := s.redis.Pipeline()
	cmds := make([]*redis.IntCmd, len(keys))
	for i, k := range keys {
		cmds[i] = pipe.BitCount(ctx, k, nil)
	}
	if _, err := pipe.Exec(ctx); err != nil {
		return 0, err
	}

	var total int64
	for _, cmd := range cmds {
		val, err := cmd.Result()
		if err != nil {
			continue
		}
		total += val
	}
	return total, nil
}
