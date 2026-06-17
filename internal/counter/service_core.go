package counter

import (
	"context"
	"fmt"
	"strconv"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/zhiguang/app/pkg/config"
	"github.com/zhiguang/app/pkg/redislock"
	"go.uber.org/zap"
)

// CounterService 提供原子化的计数开关操作。
type CounterService struct {
	redis              *redis.Client
	producer           CounterEventPublisher
	rebuildLockOptions redislock.Options
	failureRecorder    CounterFailureRecorder
	failureTasks       CounterFailureTaskStore
	failureTopic       string
	messageIDGenerator MessageIDGenerator
}

// CounterServiceDeps 描述计数服务构造时的完整依赖集合。
type CounterServiceDeps struct {
	Redis              *redis.Client
	Producer           CounterEventPublisher
	Config             *config.CounterConfig
	FailureRecorder    CounterFailureRecorder
	FailureTasks       CounterFailureTaskStore
	FailureTopic       string
	MessageIDGenerator MessageIDGenerator
	Logger             *zap.Logger
}

// NewCounterService 创建计数器服务实例。
func NewCounterService(deps CounterServiceDeps) *CounterService {
	logger := deps.Logger
	if logger == nil {
		logger = zap.NewNop()
	}
	return &CounterService{
		redis:              deps.Redis,
		producer:           deps.Producer,
		rebuildLockOptions: rebuildLockOptions(deps.Config, logger),
		failureRecorder:    deps.FailureRecorder,
		failureTasks:       deps.FailureTasks,
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

	var (
		val   int64
		epoch uint64
		err   error
	)
	for {
		val, epoch, err = s.toggleWithEpoch(ctx, bmKey, entityType, entityID, offset, op)
		if err != nil {
			return false, err
		}
		if val != 2 {
			break
		}
		if !sleepCounterConsumer(ctx, 10*time.Millisecond) {
			return false, ctx.Err()
		}
	}
	if val == 2 {
		return false, fmt.Errorf("counter toggle blocked by rebuild lock")
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
			Epoch:      epoch,
			Index:      NameToIdx[metric],
			UserID:     userID,
			Delta:      delta,
		}
		if s.producer != nil {
			if err := s.publishCounterEvent(event); err != nil {
				return false, err
			}
		}
		return true, nil
	}
	return false, nil
}

func (s *CounterService) toggleWithEpoch(ctx context.Context, bmKey, entityType, entityID string, offset uint64, op string) (int64, uint64, error) {
	if s == nil || s.redis == nil {
		return 0, 0, fmt.Errorf("counter redis is nil")
	}

	vals, err := s.redis.Eval(ctx, TOGGLE_LUA, []string{bmKey, ActiveEpochKey(entityType, entityID), RebuildLockKey(entityType, entityID)}, offset, op).Slice()
	if err != nil {
		return 0, 0, fmt.Errorf("lua toggle: %w", err)
	}
	if len(vals) != 2 {
		return 0, 0, fmt.Errorf("lua toggle returned invalid payload")
	}

	status, err := toInt64(vals[0])
	if err != nil {
		return 0, 0, fmt.Errorf("lua toggle status: %w", err)
	}
	epoch, err := toUint64(vals[1])
	if err != nil {
		return 0, 0, fmt.Errorf("lua toggle epoch: %w", err)
	}
	return status, epoch, nil
}

func toInt64(v any) (int64, error) {
	switch n := v.(type) {
	case int64:
		return n, nil
	case uint64:
		return int64(n), nil
	case float64:
		return int64(n), nil
	case string:
		return strconv.ParseInt(n, 10, 64)
	default:
		return 0, fmt.Errorf("unexpected lua integer type %T", v)
	}
}

func toUint64(v any) (uint64, error) {
	switch n := v.(type) {
	case int64:
		return uint64(n), nil
	case uint64:
		return n, nil
	case float64:
		return uint64(n), nil
	case string:
		return strconv.ParseUint(n, 10, 64)
	default:
		return 0, fmt.Errorf("unexpected lua integer type %T", v)
	}
}

func (s *CounterService) incrementUserMetric(ctx context.Context, userID uint64, metric string, delta int) error {
	idx, ok := NameToIdx[metric]
	if !ok {
		return fmt.Errorf("unknown metric: %s", metric)
	}
	key := SdsKey("user", strconv.FormatUint(userID, 10))
	return s.redis.Eval(ctx, INCR_SDS_FIELD_LUA, []string{key}, SchemaLen, FieldSize, idx+1, delta).Err()
}
