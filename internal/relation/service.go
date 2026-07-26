package relation

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"

	"github.com/coocood/freecache"
	"github.com/jmoiron/sqlx"
	"github.com/redis/go-redis/v9"
	"go.uber.org/zap"

	"github.com/zhiguang/app/pkg/config"
)

// TOKEN_BUCKET_LUA 实现一个通用令牌桶限流器。
const TOKEN_BUCKET_LUA = `
local key = KEYS[1]
local capacity = tonumber(ARGV[1])
local rate = tonumber(ARGV[2])
local now = redis.call('TIME')[1]
local last = redis.call('HGET', key, 'last')
local tokens = redis.call('HGET', key, 'tokens')
if not last then last = now; tokens = capacity end
local elapsed = tonumber(now) - tonumber(last)
local add = elapsed * rate
tokens = math.min(capacity, tonumber(tokens) + add)
if tokens < 1 then
  redis.call('HSET', key, 'last', now)
  redis.call('HSET', key, 'tokens', tokens)
  return 0
end
tokens = tokens - 1
redis.call('HSET', key, 'last', now)
redis.call('HSET', key, 'tokens', tokens)
redis.call('PEXPIRE', key, 60000)
return 1
`

const bigVThreshold = 500

// RelationService 实现带多级缓存的关注/取关能力。
type RelationService struct {
	db             *sqlx.DB
	redis          *redis.Client
	repo           Repo
	l1             *freecache.Cache
	idGen          IDGenerator
	logger         *zap.Logger
	bigVThreshold  int64
	tokenBucketCfg *config.RelationTokenBucketConfig
	cfg            *config.RelationConfig
	auditLog       AuditLogger
	// fanoutHooks 让关注/取关能同步维护信息流收件箱，可为 nil（扩散未装配）。
	fanoutHooks FanoutHooks
}

// FanoutHooks 是关系变更需要通知扩散模块的最小契约。
//
// WHY 关注/取关必须联动信息流：
//
//	写扩散把帖子**复制**进了粉丝收件箱，这两个动作因此都有后续影响：
//	  - 关注：新关注者不在对方历史帖子的推送名单里，
//	    在对方发下一条之前信息流里看不到任何该作者的内容，用户会认为「关注没生效」。
//	  - 取关：已复制进收件箱的历史帖子不会自动消失，
//	    用户取关后仍会持续看到对方内容——这是纯写扩散最容易被投诉的一环。
//
// 用窄接口而非直接依赖 *fanout.Service，避免 relation 与扩散实现耦合。
type FanoutHooks interface {
	OnFollow(ctx context.Context, followerID, authorID uint64) error
	OnUnfollow(ctx context.Context, followerID, authorID uint64) error
}

// SetFanoutHooks 注入扩散钩子。装配后回注：扩散模块的构造依赖本服务，存在先后依赖。
func (s *RelationService) SetFanoutHooks(h FanoutHooks) {
	s.fanoutHooks = h
}

// AuditLogger 定义审计日志接口。
type AuditLogger interface {
	LogAction(ctx context.Context, action string, userID int64, resourceType, resourceID, detail string)
}

// IDGenerator 定义关系域依赖的分布式唯一 ID 生成接口。
type IDGenerator interface {
	NextID() uint64
}

// NewRelationService 创建一个带多级缓存的关系服务实例。
func NewRelationService(db *sqlx.DB, rdb *redis.Client, cacheSize int, idGen IDGenerator, logger *zap.Logger, cfg *config.RelationConfig, auditLog AuditLogger) *RelationService {
	if logger == nil {
		logger = zap.L()
	}

	var tokenBucketCfg *config.RelationTokenBucketConfig
	if cfg != nil {
		tokenBucketCfg = &cfg.TokenBucket
	}

	bigVThresh := int64(bigVThreshold)
	if cfg != nil && cfg.BigVThreshold > 0 {
		bigVThresh = int64(cfg.BigVThreshold)
	}

	return &RelationService{
		db:             db,
		redis:          rdb,
		repo:           NewRelationRepository(db),
		l1:             freecache.NewCache(cacheSize),
		idGen:          idGen,
		logger:         logger,
		tokenBucketCfg: tokenBucketCfg,
		bigVThreshold:  bigVThresh,
		cfg:            cfg,
		auditLog:       auditLog,
	}
}

func (s *RelationService) tokenBucketParams() (int, int) {
	if s.tokenBucketCfg != nil && s.tokenBucketCfg.Capacity > 0 {
		return s.tokenBucketCfg.Capacity, s.tokenBucketCfg.Rate
	}
	return 10, 1
}

// SetTokenBucketConfig 设置令牌桶限流配置。
func (s *RelationService) SetTokenBucketConfig(cfg *config.RelationTokenBucketConfig) {
	if cfg != nil {
		s.tokenBucketCfg = cfg
	}
}

// IsFollowing 判断 fromUserID 是否关注了 toUserID。
func (s *RelationService) IsFollowing(ctx context.Context, fromUserID, toUserID uint64) (bool, error) {
	cnt, err := s.repo.ExistsFollowing(ctx, fromUserID, toUserID)
	if err != nil {
		return false, fmt.Errorf("is following: %w", err)
	}
	return cnt > 0, nil
}

// RelationStatus 返回两个用户之间的关系状态。
func (s *RelationService) RelationStatus(ctx context.Context, fromUserID, toUserID uint64) (string, error) {
	following, err := s.IsFollowing(ctx, fromUserID, toUserID)
	if err != nil {
		return "", err
	}
	followedBy, err := s.IsFollowing(ctx, toUserID, fromUserID)
	if err != nil {
		return "", err
	}
	if following && followedBy {
		return "mutual", nil
	}
	if following {
		return "following", nil
	}
	if followedBy {
		return "followed", nil
	}
	return "none", nil
}

// Following 返回 userID 关注的人列表，使用 offset 分页。
func (s *RelationService) Following(ctx context.Context, userID uint64, limit, offset int) ([]uint64, error) {
	return s.getListWithOffset(ctx, userID, "following", limit, offset)
}

// Followers 返回粉丝列表，使用 offset 分页。
func (s *RelationService) Followers(ctx context.Context, userID uint64, limit, offset int) ([]uint64, error) {
	return s.getListWithOffset(ctx, userID, "followers", limit, offset)
}

// FollowingCursor 返回基于游标分页的关注列表。
func (s *RelationService) FollowingCursor(ctx context.Context, userID uint64, limit int, cursor int64) ([]uint64, int64, error) {
	return s.getListWithCursor(ctx, userID, "following", limit, cursor)
}

// FollowersCursor 返回基于游标分页的粉丝列表。
func (s *RelationService) FollowersCursor(ctx context.Context, userID uint64, limit int, cursor int64) ([]uint64, int64, error) {
	return s.getListWithCursor(ctx, userID, "followers", limit, cursor)
}

type listEntry struct {
	UserID    uint64
	CreatedAt int64
}

// l1KeyStr 生成 freecache（L1）的缓存键。
func (s *RelationService) l1KeyStr(listType string, userID uint64) string {
	return fmt.Sprintf("l1:%s:%d", listType, userID)
}

// toLongList 将 freecache 中的逗号分隔 ID 字符串解析为 uint64 切片。
func (s *RelationService) toLongList(data string) []uint64 {
	parts := strings.Split(data, ",")
	result := make([]uint64, 0, len(parts))
	for _, p := range parts {
		p = strings.TrimSpace(p)
		if v, err := strconv.ParseUint(p, 10, 64); err == nil {
			result = append(result, v)
		}
	}
	return result
}

// toIDList 将 Redis ZRevRange 返回的成员列表转换为 uint64 切片。
func (s *RelationService) toIDList(members []string) []uint64 {
	result := make([]uint64, 0, len(members))
	for _, m := range members {
		if v, err := strconv.ParseUint(m, 10, 64); err == nil {
			result = append(result, v)
		}
	}
	return result
}

// errNothingToCancel 表示取关时没有有效的关注关系。
var errNothingToCancel = errors.New("relation: nothing to cancel")
