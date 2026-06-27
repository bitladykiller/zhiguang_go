package counter

import (
	"context"
	"errors"
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
	failureTopic       string
	messageIDGenerator MessageIDGenerator
	logger             *zap.Logger
	publishTimeout     time.Duration
}

func NewCounterService(
	rdb *redis.Client,
	producer CounterEventPublisher,
	cfg *config.CounterConfig,
	failureRecorder CounterFailureRecorder,
	failureTopic string,
	messageIDGenerator MessageIDGenerator,
	logger *zap.Logger,
) *CounterService {
	publishTimeout := config.CounterConfig{}.PublishTimeout()
	if cfg != nil {
		publishTimeout = cfg.PublishTimeout()
	}
	return &CounterService{
		redis:              rdb,
		producer:           producer,
		rebuildLockOptions: rebuildLockOptions(cfg),
		publishTimeout:     publishTimeout,
		failureRecorder:    failureRecorder,
		failureTopic:       failureTopic,
		messageIDGenerator: messageIDGenerator,
		logger:             logger,
	}
}

// GetLikers 返回指定实体的点赞/收藏用户列表（分页）。
func (s *CounterService) GetLikers(ctx context.Context, entityType string, entityID uint64, metric string, cursor uint64, limit int) (*LikersResponse, error) {
	if limit <= 0 {
		limit = 20
	}
	if limit > 50 {
		limit = 50
	}

	prefix := "like"
	if metric == "favorite" {
		prefix = "fav"
	}

	cacheKey := fmt.Sprintf("likers_cache:%s:%d:%s", entityType, entityID, metric)
	results, err := s.redis.ZRangeByScore(ctx, cacheKey, &redis.ZRangeBy{
		Min:   fmt.Sprintf("(%d", cursor),
		Max:   "+inf",
		Count: int64(limit + 1),
	}).Result()
	if err == nil && len(results) > 0 {
		return s.buildLikersFromCache(ctx, entityType, entityID, results, limit, cacheKey)
	}

	return s.scanBitmapForLikers(ctx, entityType, entityID, prefix, cursor, limit, cacheKey)
}

func (s *CounterService) buildLikersFromCache(ctx context.Context, entityType string, entityID uint64, results []string, limit int, cacheKey string) (*LikersResponse, error) {
	items := make([]LikerItem, 0, len(results))
	for _, uidStr := range results {
		uid, err := strconv.ParseUint(uidStr, 10, 64)
		if err != nil {
			continue
		}
		timeKey := fmt.Sprintf("liker_time:%s:%d:%d", entityType, entityID, uid)
		likedAt, _ := s.redis.Get(ctx, timeKey).Int64()
		items = append(items, LikerItem{UserID: uid, LikedAt: likedAt})
	}

	hasMore := len(items) > limit
	if hasMore {
		items = items[:limit]
	}

	var nextCursor uint64
	if len(items) > 0 {
		nextCursor = items[len(items)-1].UserID
	}

	return &LikersResponse{Items: items, Cursor: nextCursor, HasMore: hasMore}, nil
}

func (s *CounterService) scanBitmapForLikers(ctx context.Context, entityType string, entityID uint64, prefix string, cursor uint64, limit int, cacheKey string) (*LikersResponse, error) {
	items := make([]LikerItem, 0)
	maxChunk := uint64(128)

	for chunk := uint64(0); chunk < maxChunk; chunk++ {
		bmKey := fmt.Sprintf("bm:%s:%s:%d:%d", prefix, entityType, entityID, chunk)
		bmStr, err := s.redis.Get(ctx, bmKey).Result()
		if err != nil {
			if errors.Is(err, redis.Nil) {
				continue
			}
			continue
		}

		for offset := uint64(0); offset < uint64(len(bmStr))*8; offset++ {
			byteIdx := offset / 8
			bitIdx := offset % 8
			if bmStr[byteIdx]&(1<<bitIdx) != 0 {
				userID := chunk*ChunkSize + offset
				if userID <= cursor {
					continue
				}

				timeKey := fmt.Sprintf("liker_time:%s:%d:%d", entityType, entityID, userID)
				likedAt, _ := s.redis.Get(ctx, timeKey).Int64()
				items = append(items, LikerItem{UserID: userID, LikedAt: likedAt})

				if len(items) >= limit+1 {
					break
				}
			}
		}
		if len(items) >= limit+1 {
			break
		}
	}

	hasMore := len(items) > limit
	if hasMore {
		items = items[:limit]
	}

	var nextCursor uint64
	if len(items) > 0 {
		nextCursor = items[len(items)-1].UserID
	}

	if len(items) > 0 {
		pipe := s.redis.Pipeline()
		for _, item := range items {
			pipe.ZAdd(ctx, cacheKey, redis.Z{Score: float64(item.UserID), Member: strconv.FormatUint(item.UserID, 10)})
		}
		pipe.Expire(ctx, cacheKey, 5*time.Minute)
		pipe.ZRemRangeByRank(ctx, cacheKey, 0, -501)
		_, _ = pipe.Exec(ctx)
	}

	return &LikersResponse{Items: items, Cursor: nextCursor, HasMore: hasMore}, nil
}
