package counter

import (
	"context"
	"time"

	"github.com/redis/go-redis/v9"
	"go.uber.org/zap"

	"github.com/zhiguang/app/pkg/config"
	"github.com/zhiguang/app/pkg/redislock"
)

const defaultMaxChunk = uint64(128)

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
	backoffCfg         *config.BackoffConfig
	rebuildRateCfg     *config.RebuildRateConfig
	auditLog           AuditLogger
	// likersMaxChunk 是点赞者位图扫描的分片上限，决定可枚举的最大用户 ID。
	likersMaxChunk uint64
}

// AuditLogger 定义审计日志接口。
type AuditLogger interface {
	LogAction(ctx context.Context, action string, userID int64, resourceType, resourceID, detail string)
}

func NewCounterService(
	rdb *redis.Client,
	producer CounterEventPublisher,
	cfg *config.CounterConfig,
	failureRecorder CounterFailureRecorder,
	failureTopic string,
	messageIDGenerator MessageIDGenerator,
	logger *zap.Logger,
	auditLog AuditLogger,
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
		backoffCfg:         backoffConfig(cfg),
		rebuildRateCfg:     rebuildRateConfig(cfg),
		auditLog:           auditLog,
		likersMaxChunk:     likersMaxChunk(cfg),
	}
}

func backoffConfig(cfg *config.CounterConfig) *config.BackoffConfig {
	if cfg != nil {
		return &cfg.Rebuild.Backoff
	}
	return &config.BackoffConfig{BaseMs: 500, MaxMs: 30000}
}

func rebuildRateConfig(cfg *config.CounterConfig) *config.RebuildRateConfig {
	if cfg != nil {
		return &cfg.Rebuild.Rate
	}
	return &config.RebuildRateConfig{Permits: 3, WindowSeconds: 10}
}

// likersMaxChunk 解析位图扫描的分片上限。
//
// 上限 × ChunkSize 即可枚举的最大用户 ID。默认 128 × 65536 ≈ 838 万，
// 用户规模接近该值时需要调大 counter.likers_max_chunk，否则新用户的点赞不会出现在列表里。
func likersMaxChunk(cfg *config.CounterConfig) uint64 {
	if cfg != nil && cfg.LikersMaxChunk > 0 {
		return uint64(cfg.LikersMaxChunk)
	}
	return defaultMaxChunk
}
