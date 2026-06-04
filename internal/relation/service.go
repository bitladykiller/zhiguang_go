package relation

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/coocood/freecache"
	"github.com/jmoiron/sqlx"
	"github.com/redis/go-redis/v9"
)

// TOKEN_BUCKET_LUA 实现一个通用令牌桶限流器。
// KEYS[1] 是限流键；ARGV[1] 是容量；ARGV[2] 是每秒补充的令牌数。
// 返回值：1 表示允许，0 表示拒绝。
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

// BigV 阈值：粉丝数 >= 500 的用户会进入 L1 缓存。
const bigVThreshold = 500

// RelationService 实现带多级缓存的关注/取关能力。
//
// 架构：
//
//	L1（freecache）：BigV 用户的前 500 条列表，约 50ns
//	L2（Redis ZSet）：按 created_at 排序，约 1ms
//	L3（MySQL）：真实数据源
//
// 设计模式：
//   - Transactional Outbox：关注/取关与 outbox 在同一事务内落库
//   - Token Bucket：基于 Lua 的用户级限流
//   - Read-Through Cache：缓存未命中时回源 DB 并回填
type RelationService struct {
	db    *sqlx.DB
	redis *redis.Client
	repo  *RelationRepository
	l1    *freecache.Cache
}

func NewRelationService(db *sqlx.DB, rdb *redis.Client, cacheSize int) *RelationService {
	return &RelationService{
		db:    db,
		redis: rdb,
		repo:  NewRelationRepository(db),
		l1:    freecache.NewCache(cacheSize),
	}
}

// ============================================================================
// 关注与取关
// ============================================================================

// Follow 创建一条关注关系。步骤如下：
//  1. 先做令牌桶限流检查（Lua）
//  2. 在同一个数据库事务中写入 Following/Follower 正反向索引和 Outbox 事件
func (s *RelationService) Follow(ctx context.Context, fromUserID, toUserID uint64) (bool, error) {
	// 限流检查
	rlKey := fmt.Sprintf("rl:follow:%d", fromUserID)
	allowed, err := s.redis.Eval(ctx, TOKEN_BUCKET_LUA, []string{rlKey}, 10, 1).Int()
	if err != nil || allowed == 0 {
		return false, nil
	}

	id := NextID()
	reverseID := NextID()

	// 事务内同时写正向表、反向表和 outbox。
	// WHY：粉丝列表和关系状态查询依赖反向索引表，不能只写单边。
	tx, err := s.db.BeginTxx(ctx, nil)
	if err != nil {
		return false, err
	}
	txRepo := s.repo.WithDB(tx)
	defer func() {
		if r := recover(); r != nil {
			_ = tx.Rollback()
		}
	}()

	if err := txRepo.UpsertFollowing(id, fromUserID, toUserID, 1); err != nil {
		_ = tx.Rollback()
		return false, err
	}
	if err := txRepo.UpsertFollower(reverseID, toUserID, fromUserID, 1); err != nil {
		_ = tx.Rollback()
		return false, err
	}

	event := RelationEvent{EventType: "FollowCreated", FromUserID: fromUserID, ToUserID: toUserID, RelationID: &id}
	payload, _ := json.Marshal(event)
	if err := txRepo.InsertOutbox(NextID(), "following", &id, "FollowCreated", string(payload)); err != nil {
		_ = tx.Rollback()
		return false, err
	}

	if err := tx.Commit(); err != nil {
		return false, err
	}

	// 失效相关缓存
	s.invalidateCaches(ctx, fromUserID, toUserID)
	return true, nil
}

// Unfollow 取消关注关系，并写入对应 outbox 事件。
func (s *RelationService) Unfollow(ctx context.Context, fromUserID, toUserID uint64) (bool, error) {
	tx, err := s.db.BeginTxx(ctx, nil)
	if err != nil {
		return false, err
	}
	txRepo := s.repo.WithDB(tx)

	affected, err := txRepo.CancelFollowing(fromUserID, toUserID)
	if err != nil || affected == 0 {
		_ = tx.Rollback()
		return false, err
	}
	reverseAffected, err := txRepo.CancelFollower(toUserID, fromUserID)
	if err != nil {
		_ = tx.Rollback()
		return false, err
	}
	if reverseAffected == 0 {
		// 历史遗留数据可能只写入了正向 following 表。
		// WHY：在修复反向索引之前，早期写入可能是不完整的，
		// 所以恢复取关时只能把正向记录视为权威来源。
	}

	event := RelationEvent{EventType: "FollowCanceled", FromUserID: fromUserID, ToUserID: toUserID}
	payload, _ := json.Marshal(event)
	if err := txRepo.InsertOutbox(NextID(), "following", nil, "FollowCanceled", string(payload)); err != nil {
		_ = tx.Rollback()
		return false, err
	}

	if err := tx.Commit(); err != nil {
		return false, err
	}

	s.invalidateCaches(ctx, fromUserID, toUserID)
	return true, nil
}

// IsFollowing 判断 fromUserID 是否关注了 toUserID。
func (s *RelationService) IsFollowing(fromUserID, toUserID uint64) (bool, error) {
	cnt, err := s.repo.ExistsFollowing(fromUserID, toUserID)
	if err != nil {
		return false, err
	}
	return cnt > 0, nil
}

// ============================================================================
// 列表查询
// ============================================================================

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

// RelationStatus 返回两个用户之间的关系状态。
func (s *RelationService) RelationStatus(ctx context.Context, fromUserID, toUserID uint64) (string, error) {
	following, err := s.IsFollowing(fromUserID, toUserID)
	if err != nil {
		return "", err
	}
	followedBy, err := s.IsFollowing(toUserID, fromUserID)
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

// ============================================================================
// 内部缓存逻辑
// ============================================================================

func (s *RelationService) getListWithOffset(ctx context.Context, userID uint64, listType string, limit, offset int) ([]uint64, error) {
	// L1：freecache（仅 BigV 使用，先检查 L2 的 ZCard）
	if s.isBigV(ctx, userID) {
		l1Key := s.l1KeyStr(listType, userID)
		if data, err := s.l1.Get([]byte(l1Key)); err == nil {
			ids := s.toLongList(string(data))
			if offset < len(ids) {
				end := offset + limit
				if end > len(ids) {
					end = len(ids)
				}
				return ids[offset:end], nil
			}
		}
	}

	// L2：Redis ZSet
	zsetKey := s.zsetKey(listType, userID)
	exists, _ := s.redis.Exists(ctx, zsetKey).Result()
	if exists > 0 {
		members, err := s.redis.ZRevRange(ctx, zsetKey, int64(offset), int64(offset+limit-1)).Result()
		if err == nil {
			return s.toIDList(members), nil
		}
	}

	// L3：回源数据库
	rows, err := s.readFromDB(listType, userID, limit+offset, 0)
	if err != nil {
		return nil, err
	}
	ids := make([]uint64, 0, len(rows))
	for _, entry := range rows {
		ids = append(ids, entry.UserID)
	}

	// 回填 ZSet
	s.fillZSet(ctx, listType, userID)

	// 如果是 BigV，则回填 L1
	if s.isBigV(ctx, userID) {
		s.fillL1(ctx, listType, userID)
	}

	if offset >= len(ids) {
		return []uint64{}, nil
	}
	end := offset + limit
	if end > len(ids) {
		end = len(ids)
	}
	return ids[offset:end], nil
}

func (s *RelationService) getListWithCursor(ctx context.Context, userID uint64, listType string, limit int, cursor int64) ([]uint64, int64, error) {
	zsetKey := s.zsetKey(listType, userID)
	exists, _ := s.redis.Exists(ctx, zsetKey).Result()
	if exists == 0 {
		if err := s.fillZSet(ctx, listType, userID); err != nil {
			return nil, 0, err
		}
	}

	var maxVal string
	if cursor > 0 {
		maxVal = fmt.Sprintf("(%d", cursor)
	} else {
		maxVal = "+inf"
	}

	members, err := s.redis.ZRevRangeByScore(ctx, zsetKey, &redis.ZRangeBy{
		Min:    "-inf",
		Max:    maxVal,
		Offset: 0,
		Count:  int64(limit),
	}).Result()
	if err != nil {
		return nil, 0, err
	}

	result := s.toIDList(members)
	var nextCursor int64
	if len(result) > 0 {
		lastID := fmt.Sprintf("%d", result[len(result)-1])
		score, _ := s.redis.ZScore(ctx, zsetKey, lastID).Result()
		nextCursor = int64(score)
	}

	return result, nextCursor, nil
}

func (s *RelationService) fillZSet(ctx context.Context, listType string, userID uint64) error {
	zsetKey := s.zsetKey(listType, userID)
	entries, err := s.readFromDB(listType, userID, 2000, 0)
	if err != nil || len(entries) == 0 {
		return err
	}

	members := make([]redis.Z, len(entries))
	for i, entry := range entries {
		members[i] = redis.Z{
			Score:  float64(entry.CreatedAt.UnixMilli()),
			Member: strconv.FormatUint(entry.UserID, 10),
		}
	}

	if err := s.redis.ZAdd(ctx, zsetKey, members...).Err(); err != nil {
		return err
	}
	s.redis.Expire(ctx, zsetKey, 2*time.Hour)
	return nil
}

func (s *RelationService) fillL1(ctx context.Context, listType string, userID uint64) {
	key := s.l1KeyStr(listType, userID)
	entries, err := s.readFromDB(listType, userID, 500, 0)
	if err != nil || len(entries) == 0 {
		return
	}
	idStrs := make([]string, len(entries))
	for i, e := range entries {
		idStrs[i] = strconv.FormatUint(e.UserID, 10)
	}
	s.l1.Set([]byte(key), []byte(strings.Join(idStrs, ",")), 600) // 10 min TTL
}

// ============================================================================
// 辅助函数
// ============================================================================

type listEntry struct {
	UserID    uint64
	CreatedAt time.Time
}

func (s *RelationService) readFromDB(listType string, userID uint64, limit, offset int) ([]listEntry, error) {
	if listType == "following" {
		rows, err := s.repo.ListFollowingRows(userID, limit, offset)
		if err != nil {
			return nil, err
		}
		entries := make([]listEntry, len(rows))
		for i, r := range rows {
			entries[i] = listEntry{UserID: r.ToUserID, CreatedAt: r.CreatedAt}
		}
		return entries, nil
	}
	rows, err := s.repo.ListFollowerRows(userID, limit, offset)
	if err != nil {
		return nil, err
	}
	if len(rows) == 0 {
		// 向后兼容：旧版本写入只填充了正向索引。
		rows, err = s.repo.ListFollowerRowsFromFollowing(userID, limit, offset)
		if err != nil {
			return nil, err
		}
	}
	entries := make([]listEntry, len(rows))
	for i, r := range rows {
		entries[i] = listEntry{UserID: r.FromUserID, CreatedAt: r.CreatedAt}
	}
	return entries, nil
}

func (s *RelationService) isBigV(ctx context.Context, userID uint64) bool {
	key := s.zsetKey("followers", userID)
	size, err := s.redis.ZCard(ctx, key).Result()
	if err != nil {
		return false
	}
	return size >= bigVThreshold
}

func (s *RelationService) zsetKey(listType string, userID uint64) string {
	return fmt.Sprintf("z:%s:%d", listType, userID)
}

func (s *RelationService) l1KeyStr(listType string, userID uint64) string {
	return fmt.Sprintf("l1:%s:%d", listType, userID)
}

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

func (s *RelationService) toIDList(members []string) []uint64 {
	result := make([]uint64, 0, len(members))
	for _, m := range members {
		if v, err := strconv.ParseUint(m, 10, 64); err == nil {
			result = append(result, v)
		}
	}
	return result
}

func (s *RelationService) invalidateCaches(ctx context.Context, fromUserID, toUserID uint64) {
	// 失效 fromUserID 的关注列表缓存
	s.redis.Del(ctx, s.zsetKey("following", fromUserID))
	s.l1.Del([]byte(s.l1KeyStr("following", fromUserID)))
	// 失效 toUserID 的粉丝列表缓存
	s.redis.Del(ctx, s.zsetKey("followers", toUserID))
	s.l1.Del([]byte(s.l1KeyStr("followers", toUserID)))
}
