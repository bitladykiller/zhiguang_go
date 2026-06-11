package counter

import (
	"context"
	"fmt"
	"strconv"

	"github.com/redis/go-redis/v9"
	"github.com/zhiguang/app/pkg/config"
	"github.com/zhiguang/app/pkg/redislock"
)

// CounterService 提供原子化的计数开关操作。
type CounterService struct {
	redis              *redis.Client
	producer           CounterEventPublisher
	rebuildLockOptions redislock.Options
	failureRecorder    CounterFailureRecorder
	failureTopic       string
	messageIDGenerator MessageIDGenerator
}

// CounterServiceDeps 描述计数服务构造时的完整依赖集合。
type CounterServiceDeps struct {
	Redis              *redis.Client
	Producer           CounterEventPublisher
	Config             *config.CounterConfig
	FailureRecorder    CounterFailureRecorder
	FailureTopic       string
	MessageIDGenerator MessageIDGenerator
}

// NewCounterService 创建计数器服务实例。
func NewCounterService(deps CounterServiceDeps) *CounterService {
	return &CounterService{
		redis:              deps.Redis,
		producer:           deps.Producer,
		rebuildLockOptions: rebuildLockOptions(deps.Config),
		failureRecorder:    deps.FailureRecorder,
		failureTopic:       deps.FailureTopic,
		messageIDGenerator: deps.MessageIDGenerator,
	}
}

func (s *CounterService) Like(ctx context.Context, userID uint64, entityType, entityID string) (bool, error) {
	return s.toggle(ctx, userID, entityType, entityID, "like", "add")
}

func (s *CounterService) Unlike(ctx context.Context, userID uint64, entityType, entityID string) (bool, error) {
	return s.toggle(ctx, userID, entityType, entityID, "like", "remove")
}

func (s *CounterService) Fav(ctx context.Context, userID uint64, entityType, entityID string) (bool, error) {
	return s.toggle(ctx, userID, entityType, entityID, "fav", "add")
}

func (s *CounterService) Unfav(ctx context.Context, userID uint64, entityType, entityID string) (bool, error) {
	return s.toggle(ctx, userID, entityType, entityID, "fav", "remove")
}

func (s *CounterService) IncrementFollowings(ctx context.Context, userID uint64, delta int) error {
	return s.incrementUserMetric(ctx, userID, "following", delta)
}

func (s *CounterService) IncrementFollowers(ctx context.Context, userID uint64, delta int) error {
	return s.incrementUserMetric(ctx, userID, "follower", delta)
}

func (s *CounterService) toggle(ctx context.Context, userID uint64, entityType, entityID, metric, op string) (bool, error) {
	chunk := ChunkOf(userID)
	offset := BitOf(userID)
	bmKey := BitmapKey(metric, entityType, entityID, chunk)

	val, err := s.redis.Eval(ctx, TOGGLE_LUA, []string{bmKey}, offset, op).Int()
	if err != nil {
		return false, fmt.Errorf("lua toggle: %w", err)
	}

	if val == 1 {
		delta := 1
		if op == "remove" {
			delta = -1
		}
		event := &CounterEvent{
			MessageID:  s.nextMessageID(),
			EntityType: entityType,
			EntityID:   entityID,
			Metric:     metric,
			Index:      NameToIdx[metric],
			UserID:     userID,
			Delta:      delta,
		}
		if s.producer != nil {
			go s.publishCounterEvent(event)
		}
		return true, nil
	}
	return false, nil
}

func (s *CounterService) incrementUserMetric(ctx context.Context, userID uint64, metric string, delta int) error {
	idx, ok := NameToIdx[metric]
	if !ok {
		return fmt.Errorf("unknown metric: %s", metric)
	}
	key := SdsKey("user", strconv.FormatUint(userID, 10))
	return s.redis.Eval(ctx, INCR_SDS_FIELD_LUA, []string{key}, SchemaLen, FieldSize, idx+1, delta).Err()
}
