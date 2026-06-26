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
	"github.com/zhiguang/app/pkg/config"
	"go.uber.org/zap"
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
	db              *sqlx.DB
	redis           *redis.Client
	repo            *RelationRepository
	l1              *freecache.Cache
	idGen           IDGenerator
	logger          *zap.Logger
	bigVThreshold   int64
	tokenBucketCfg  *config.RelationTokenBucketConfig
}

// IDGenerator 定义关系域依赖的分布式唯一 ID 生成接口。
type IDGenerator interface {
	NextID() uint64
}

// NewRelationService 创建一个带多级缓存的关系服务实例。
func NewRelationService(db *sqlx.DB, rdb *redis.Client, cacheSize int, idGen IDGenerator, logger *zap.Logger) *RelationService {
	if logger == nil {
		logger = zap.L()
	}
	return &RelationService{
		db:     db,
		redis:  rdb,
		repo:   NewRelationRepository(db),
		l1:     freecache.NewCache(cacheSize),
		idGen:  idGen,
		logger: logger,
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