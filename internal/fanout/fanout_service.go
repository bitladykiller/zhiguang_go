package fanout

import (
	"context"
	"fmt"
	"math"
	"strconv"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/zhiguang/app/internal/model"
	"go.uber.org/zap"
)

type FollowerLister interface {
	Followers(ctx context.Context, userID uint64, limit, offset int) ([]uint64, error)
}

type Config struct {
	FanoutBatchSize  int
	FanoutMaxFans    int
	TimelineTTL      time.Duration
	TimelineMaxItems int
}

func DefaultConfig() Config {
	return Config{
		FanoutBatchSize:  500,
		FanoutMaxFans:    10000,
		TimelineTTL:      7 * 24 * time.Hour,
		TimelineMaxItems: 1000,
	}
}

type Service struct {
	redisClient   redis.UniversalClient
	followerLister FollowerLister
	logger        *zap.Logger
	cfg           Config
}

func NewService(redisClient redis.UniversalClient, fl FollowerLister, logger *zap.Logger, cfg Config) *Service {
	return &Service{
		redisClient:    redisClient,
		followerLister: fl,
		logger:         logger,
		cfg:            cfg,
	}
}

func (s *Service) FanoutPost(ctx context.Context, event *model.FanoutEvent) error {
	if s.followerLister == nil || s.redisClient == nil {
		return nil
	}

	var allFans []uint64
	limit := 1000
	offset := 0
	for {
		fans, err := s.followerLister.Followers(ctx, event.CreatorID, limit, offset)
		if err != nil {
			return fmt.Errorf("fanout: get followers: %w", err)
		}
		if len(fans) == 0 {
			break
		}
		allFans = append(allFans, fans...)
		if len(fans) < limit {
			break
		}
		offset += limit
		if len(allFans) > s.cfg.FanoutMaxFans {
			allFans = allFans[:s.cfg.FanoutMaxFans]
			break
		}
	}

	if len(allFans) == 0 {
		return nil
	}

	if len(allFans) > s.cfg.FanoutMaxFans {
		s.logger.Info("fanout skipped: too many fans",
			zap.Uint64("creatorID", event.CreatorID),
			zap.Int("fanCount", len(allFans)),
			zap.Int("maxFans", s.cfg.FanoutMaxFans),
		)
		return nil
	}

	member := strconv.FormatUint(event.PostID, 10)
	score := float64(event.CreatedAt)
	batchSize := s.cfg.FanoutBatchSize
	if batchSize <= 0 {
		batchSize = 500
	}
	maxItems := s.cfg.TimelineMaxItems
	if maxItems <= 0 {
		maxItems = 1000
	}

	for i := 0; i < len(allFans); i += batchSize {
		end := int(math.Min(float64(i+batchSize), float64(len(allFans))))
		batch := allFans[i:end]

		pipe := s.redisClient.Pipeline()
		for _, followerID := range batch {
			timelineKey := fmt.Sprintf("timeline:%d", followerID)
			pipe.ZAdd(ctx, timelineKey, redis.Z{Score: score, Member: member})
			pipe.ZRemRangeByRank(ctx, timelineKey, 0, int64(-maxItems-1))
			pipe.Expire(ctx, timelineKey, s.cfg.TimelineTTL)
		}
		if _, err := pipe.Exec(ctx); err != nil {
			return fmt.Errorf("fanout: pipeline exec: %w", err)
		}
	}

	s.logger.Info("fanout post diffused",
		zap.Uint64("postID", event.PostID),
		zap.Uint64("creatorID", event.CreatorID),
		zap.Int("fanCount", len(allFans)),
	)
	return nil
}
