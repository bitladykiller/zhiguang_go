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
// 缓存架构：
//
//	L1（freecache）：BigV 用户的前 500 条关注/粉丝列表，约 50ns 响应
//	L2（Redis ZSet）：按关注/创建时间排序（created_at 的毫秒时间戳作为 score），约 1ms 响应
//	L3（MySQL）：真实数据源，通过 offset 分页
//
// 设计模式：
//   - Transactional Outbox：关注/取关操作与 outbox 事件写入在同一个数据库事务内完成，
//     确保不会出现「关注已建立但事件未发出」的不一致情况。
//   - Token Bucket（令牌桶限流）：通过 Lua 脚本实现用户级别的关注频率控制，
//     防止单用户短时间内大量关注导致全局限流或数据写入压力。
//   - Read-Through Cache：缓存未命中时回源 DB 查询并回填 L2 ZSet 和 L1 freecache。
type RelationService struct {
	db    *sqlx.DB
	redis *redis.Client
	repo  *RelationRepository
	l1    *freecache.Cache
}

// NewRelationService 创建一个带多级缓存的关系服务实例。
//
// 功能：初始化 RelationService 的必要依赖。
//
// 参数：
//   - db: *sqlx.DB，数据库连接。
//   - rdb: *redis.Client，Redis 客户端，用于 L2 缓存和限流。
//   - cacheSize: int，freecache（L1）的内存分配大小（字节）。例如 10*1024*1024 = 10MB。
//
// 返回值：*RelationService，创建好的服务实例。
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

// Follow 创建一条关注关系。
//
// 功能：执行关注操作。步骤如下：
//  1. 限流检查：通过 Redis Lua 脚本执行令牌桶限流（TOKEN_BUCKET_LUA）。
//     如果限流拒绝，返回 (false, nil) 表示操作未执行但非错误。
//  2. 在同一个数据库事务中写入正向索引（Following）、反向索引（Follower）
//     和 outbox 事件（Transactional Outbox Pattern）。
//  3. 提交事务后失效关联缓存。
//
// TOKEN_BUCKET_LUA 说明（自定义 Lua 令牌桶算法）：
//   - KEYS[1] = 限流键（如 "rl:follow:{userID}"）
//   - ARGV[1] = capacity（桶容量，此处为 10）
//   - ARGV[2] = rate（每秒补充令牌数，此处为 1）
//   - 原理：使用 Redis HASH 存储上个时间点的剩余令牌数和时间戳。
//     每次调用时计算时间差并补充令牌。令牌不足返回 0，否则扣减并返回 1。
//   - TTL：1 分钟（PEXPIRE 60000），避免空键一直占用内存。
//
// freecache 说明：
//   - coocood/freecache 是一个进程内缓存库，约 50ns 响应时间。
//   - 使用 ring buffer 存储数据，不会产生 GC 压力。
//   - 当缓存满时自动淘汰最旧的数据。
//   - 此处用于缓存 BigV 用户的前 500 个关注/粉丝 ID。
//
// ZAdd/ZRevRange 说明：
//   - Redis ZAdd：向有序集合（Sorted Set）添加成员。
//     记分（score）是关注时间（created_at）的毫秒时间戳。
//   - ZRevRange：按 score 降序返回成员列表，即最新的关注/粉丝优先。
//
// 参数：
//   - ctx: context.Context。
//   - fromUserID: uint64，发起关注的操作者用户 ID。
//   - toUserID: uint64，被关注的用户 ID。
//
// 返回值：
//   - bool: true 表示关注成功；false 表示被限流。
//   - error: 限流、数据库等系统错误。
//
// 边界情况：
//   - 已关注再次调用：UpsertFollowing 使用 ON DUPLICATE KEY UPDATE，
//     不会报错，但 CancelFollowing 后再次 Follow 会重新激活关系。
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

	if err := txRepo.UpsertFollowing(ctx, id, fromUserID, toUserID, 1); err != nil {
		_ = tx.Rollback()
		return false, err
	}
	if err := txRepo.UpsertFollower(ctx, reverseID, toUserID, fromUserID, 1); err != nil {
		_ = tx.Rollback()
		return false, err
	}

	event := RelationEvent{EventType: "FollowCreated", FromUserID: fromUserID, ToUserID: toUserID, RelationID: &id}
	payload, _ := json.Marshal(event)
	if err := txRepo.InsertOutbox(ctx, NextID(), "following", &id, "FollowCreated", string(payload)); err != nil {
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

// Unfollow 取消关注关系，并在同一事务中写入 outbox 事件。
//
// 功能：将 following 表与 follower 表中的对应关系标记为取消（rel_status = 0）。
//
// 历史兼容处理：
//
//	如果 follower 表中没有对应的反向记录（旧版本数据只写了 following 表），
//	则忽略该错误，只以 following 表为准。这解决了早期版本写操作不完整的兼容问题。
//
// 参数：
//   - ctx: context.Context。
//   - fromUserID: uint64，发起取关的用户 ID。
//   - toUserID: uint64，被取关的用户 ID。
//
// 返回值：
//   - bool: true 表示取关成功（affected > 0）；false 表示未关注或已取关。
//   - error: 数据库错误。
func (s *RelationService) Unfollow(ctx context.Context, fromUserID, toUserID uint64) (bool, error) {
	tx, err := s.db.BeginTxx(ctx, nil)
	if err != nil {
		return false, err
	}
	txRepo := s.repo.WithDB(tx)

	affected, err := txRepo.CancelFollowing(ctx, fromUserID, toUserID)
	if err != nil || affected == 0 {
		_ = tx.Rollback()
		return false, err
	}
	reverseAffected, err := txRepo.CancelFollower(ctx, toUserID, fromUserID)
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
	if err := txRepo.InsertOutbox(ctx, NextID(), "following", nil, "FollowCanceled", string(payload)); err != nil {
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
//
// 功能：通过查询 following 表中是否存在有效（rel_status = 1）的记录来判断。
// 直接查库，不做缓存——因为单个关系判断的查询复杂度是 O(1)，
// 缓存带来的收益和复杂度不成正比。
//
// 参数：
//   - fromUserID: uint64，可能未关注的人。
//   - toUserID: uint64，被关注的目标。
//
// 返回值：
//   - bool: true 表示已关注，false 表示未关注。
//   - error: 数据库查询错误。
func (s *RelationService) IsFollowing(fromUserID, toUserID uint64) (bool, error) {
	ctx := context.Background()
	cnt, err := s.repo.ExistsFollowing(ctx, fromUserID, toUserID)
	if err != nil {
		return false, err
	}
	return cnt > 0, nil
}

// ============================================================================
// 列表查询
// ============================================================================

// Following 返回 userID 关注的人列表，使用 offset 分页。
//
// 功能：调用 getListWithOffset 读取关注列表，利用三级缓存加速。
//
// 参数：
//   - ctx: context.Context。
//   - userID: uint64，目标用户 ID。
//   - limit: int，每页条数。
//   - offset: int，偏移量。
//
// 返回值：
//   - []uint64: 关注用户的 ID 列表。
//   - error: 查询错误。
func (s *RelationService) Following(ctx context.Context, userID uint64, limit, offset int) ([]uint64, error) {
	return s.getListWithOffset(ctx, userID, "following", limit, offset)
}

// Followers 返回粉丝列表，使用 offset 分页。
//
// 功能：调用 getListWithOffset 读取粉丝列表，利用三级缓存加速。
//
// 参数：
//   - ctx: context.Context。
//   - userID: uint64，目标用户 ID。
//   - limit: int，每页条数。
//   - offset: int，偏移量。
//
// 返回值：
//   - []uint64: 粉丝用户的 ID 列表。
//   - error: 查询错误。
func (s *RelationService) Followers(ctx context.Context, userID uint64, limit, offset int) ([]uint64, error) {
	return s.getListWithOffset(ctx, userID, "followers", limit, offset)
}

// FollowingCursor 返回基于游标分页的关注列表。
//
// 功能：使用 Redis ZSet 的 score 作为游标，通过 ZRevRangeByScore 实现游标分页。
// 游标值是最后一条记录的关注时间（毫秒时间戳）。
// 使用 MySQL + ZSet 回填方案（Read-Through Cache）。
//
// 游标分页 vs Offset 分页：
//   - 游标分页更稳定：当数据在翻页过程中发生变化时，不会出现"跳页"或"重复"问题。
//   - 但要求数据有顺序且唯一（此处使用 created_at 毫秒时间戳）。
//
// 参数：
//   - ctx: context.Context。
//   - userID: uint64，目标用户 ID。
//   - limit: int，每页条数。
//   - cursor: int64，上次返回的游标值（毫秒时间戳）。0 表示从最新开始。
//
// 返回值：
//   - []uint64: 用户 ID 列表。
//   - int64: 下一页的游标值（0 表示没有更多数据）。
//   - error: 查询错误。
func (s *RelationService) FollowingCursor(ctx context.Context, userID uint64, limit int, cursor int64) ([]uint64, int64, error) {
	return s.getListWithCursor(ctx, userID, "following", limit, cursor)
}

// FollowersCursor 返回基于游标分页的粉丝列表。
//
// 功能：与 FollowingCursor 实现相同，只是读取的是粉丝 ZSet。
//
// 参数：
//   - ctx: context.Context。
//   - userID: uint64，目标用户 ID。
//   - limit: int，每页条数。
//   - cursor: int64，上次返回的游标值。0 表示从最新开始。
//
// 返回值：
//   - []uint64: 粉丝 ID 列表。
//   - int64: 下一页游标。
//   - error: 查询错误。
func (s *RelationService) FollowersCursor(ctx context.Context, userID uint64, limit int, cursor int64) ([]uint64, int64, error) {
	return s.getListWithCursor(ctx, userID, "followers", limit, cursor)
}

// RelationStatus 返回两个用户之间的关系状态。
//
// 功能：通过双向查询关注关系，确定两者间的关系类型。
//
// 可能的状态值：
//   - "mutual": 互相关注（双向关注）
//   - "following": 我已关注对方（单向）
//   - "followed": 对方关注了我（单向）
//   - "none": 没有关注关系（互不关注）
//
// 参数：
//   - ctx: context.Context。
//   - fromUserID: uint64，当前用户 ID。
//   - toUserID: uint64，目标用户 ID。
//
// 返回值：
//   - string: 关系状态。
//   - error: 查询错误。
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

// getListWithOffset 以 Offset 分页方式读取关注/粉丝列表（含三级缓存）。
//
// 功能：这是三级缓存读取关注/粉丝列表的核心方法。
//
// 读取路径如下：
//  1. L1（freecache）：
//     仅对 BigV（粉丝数 >= 500）用户启用。L1 缓存中保存了前 500 个 ID，
//     以逗号分隔的字符串存储。如果 offset < len(ids) 则可以直接切片返回。
//  2. L2（Redis ZSet）：
//     使用 ZRevRange 从 ZSet 中按 score 降序读取指定范围的成员。
//     ZRevRange(key, start, stop) 返回 [start, stop] 范围内的元素，
//     复杂度 O(log(N) + M)，其中 N 是 ZSet 大小，M 是返回的元素数。
//  3. L3（MySQL 回源）：
//     从数据库查询完整的关注/粉丝列表（limit + offset 条），
//     然后回填 ZSet（fillZSet）和大 V 的 L1（fillL1）。
//
// 参数：
//   - ctx: context.Context。
//   - userID: uint64，目标用户 ID。
//   - listType: string，"following" 或 "followers"。
//   - limit: int，每页条数。
//   - offset: int，偏移量。
//
// 返回值：
//   - []uint64: 用户 ID 列表。如果 offset 超出列表长度，返回空切片。
//   - error: 缓存或数据库错误。
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
	rows, err := s.readFromDB(ctx, listType, userID, limit+offset, 0)
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

// getListWithCursor 以游标分页方式读取关注/粉丝列表。
//
// 功能：使用 Redis ZSet 的 ZRevRangeByScore 实现游标分页。
//
// ZRevRangeByScore 说明：
//
//	按 score 从高到低遍历 ZSet 中指定范围内的元素。
//	参数 ZRangeBy{Min: "-inf", Max: "(cursor", Offset: 0, Count: limit}：
//	- Min = "-inf"：从最小 score 开始。
//	- Max = "(cursor"：使用 "(" 表示独占边界，即 score < cursor 的元素。
//	  当 cursor > 0 时使用此值，否则使用 "+inf" 表示最新。
//	- Count: limit：最多返回的元素数量。
//
// 下一个游标的计算方法：
//
//	取最后一位用户的 score（关注时间毫秒时间戳）作为下一个游标。
//	客户端在下次请求时把这个值传入 cursor 参数即可。
//
// 参数：
//   - ctx: context.Context。
//   - userID: uint64，目标用户 ID。
//   - listType: string，"following" 或 "followers"。
//   - limit: int，每页条数。
//   - cursor: int64，当前游标值（毫秒时间戳）。0 表示从最新开始。
//
// 返回值：
//   - []uint64: 用户 ID 列表。
//   - int64: 下一页的游标值（0 表示没有更多数据）。
//   - error: 查询错误。
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

// fillZSet 从数据库读取关注/粉丝列表并回填到 Redis ZSet。
//
// 功能：在缓存未命中、回源 DB 后调用，把数据库查询结果写入 Redis ZSet。
// 以关注时间的毫秒时间戳作为 score，用户 ID 作为 member。
//
// ZAdd 说明：
//   - ZAdd(ctx, key, members...) 向 ZSet 中添加一个或多个成员。
//   - 每个成员是一个 redis.Z 结构体 {Score: float64, Member: string}。
//   - 如果 member 已存在，ZAdd 会更新其 score（UPSERT 语义）。
//   - 复杂度 O(M * log(N))，M 是新增成员数，N 是 ZSet 大小。
//
// 参数：
//   - ctx: context.Context。
//   - listType: string，"following" 或 "followers"。
//   - userID: uint64，目标用户 ID。
//
// 返回值：
//   - error: 数据库或 Redis 错误。
//
// 边界情况：
//   - 数据库查询结果为空：不写入 ZSet，返回 nil。
//   - 最多读取 2000 条（限制 DB 查询量，防止内存溢出）。
func (s *RelationService) fillZSet(ctx context.Context, listType string, userID uint64) error {
	zsetKey := s.zsetKey(listType, userID)
	entries, err := s.readFromDB(ctx, listType, userID, 2000, 0)
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

// fillL1 将 BigV 用户的前 500 个关注/粉丝 ID 写入 freecache（L1）。
//
// 功能：只对 BigV 用户调用。从数据库读取前 500 条记录，
// 以逗号分隔的字符串形式存入 freecache。TTL 为 10 分钟。
//
// WHY 只缓存前 500 条：
//   - 大多数用户的翻页行为集中在前几页，前 500 条足够覆盖绝大部分场景。
//   - freecache 的内存有限，不能无限存储所有用户的完整列表。
//
// 参数：
//   - ctx: context.Context。
//   - listType: string，"following" 或 "followers"。
//   - userID: uint64，目标用户 ID。
func (s *RelationService) fillL1(ctx context.Context, listType string, userID uint64) {
	key := s.l1KeyStr(listType, userID)
	entries, err := s.readFromDB(ctx, listType, userID, 500, 0)
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

// readFromDB 从数据库读取用户的关注/粉丝列表。
//
// 功能：统一的数据读取方法，根据 listType 分别查询 following 表或 follower 表。
// 对于粉丝列表的读取，如果 follower 表为空（向后的兼容性处理），
// 会尝试从 following 表查询反向关系（ListFollowerRowsFromFollowing）。
//
// 参数：
//   - listType: string，"following" 或 "followers"。
//   - userID: uint64，目标用户 ID。
//   - limit: int，查询限制数。
//   - offset: int，偏移量。
//
// 返回值：
//   - []listEntry: 用户 ID 和关注时间的列表。
//   - error: 数据库查询错误。
func (s *RelationService) readFromDB(ctx context.Context, listType string, userID uint64, limit, offset int) ([]listEntry, error) {
	if listType == "following" {
		rows, err := s.repo.ListFollowingRows(ctx, userID, limit, offset)
		if err != nil {
			return nil, err
		}
		entries := make([]listEntry, len(rows))
		for i, r := range rows {
			entries[i] = listEntry{UserID: r.ToUserID, CreatedAt: r.CreatedAt}
		}
		return entries, nil
	}
	rows, err := s.repo.ListFollowerRows(ctx, userID, limit, offset)
	if err != nil {
		return nil, err
	}
	if len(rows) == 0 {
		// 向后兼容：旧版本写入只填充了正向索引。
		rows, err = s.repo.ListFollowerRowsFromFollowing(ctx, userID, limit, offset)
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

// isBigV 判断某个用户是否是 BigV（粉丝数 >= 500）。
//
// 功能：通过 ZCard 查询 Redis 中该用户粉丝 ZSet 的大小。
// ZCard(key) 返回 ZSet 中元素的数量，复杂度 O(1)。
// BigV 用户会获得 L1 缓存，优化其关注/粉丝列表的查询性能。
//
// 参数：
//   - ctx: context.Context。
//   - userID: uint64，用户 ID。
//
// 返回值：
//   - bool: true 表示是 BigV（粉丝数 >= bigVThreshold）。
//
// 边界情况：
//   - Redis 查询失败（如连接超时）：返回 false，降级为非 BigV 处理，
//     不会阻断列表查询。
func (s *RelationService) isBigV(ctx context.Context, userID uint64) bool {
	key := s.zsetKey("followers", userID)
	size, err := s.redis.ZCard(ctx, key).Result()
	if err != nil {
		return false
	}
	return size >= bigVThreshold
}

// zsetKey 生成 Redis ZSet 的缓存键。
//
// 功能：按统一格式生成关注/粉丝列表的 ZSet 键名。
// 格式：`z:{listType}:{userID}`，如 "z:following:1001" 或 "z:followers:1001"。
//
// 参数：
//   - listType: string，"following" 或 "followers"。
//   - userID: uint64，目标用户 ID。
//
// 返回值：string，ZSet 键名。
func (s *RelationService) zsetKey(listType string, userID uint64) string {
	return fmt.Sprintf("z:%s:%d", listType, userID)
}

// l1KeyStr 生成 freecache（L1）的缓存键。
//
// 功能：按统一格式生成 L1 缓存键名。
// 格式：`l1:{listType}:{userID}`，如 "l1:following:1001"。
//
// 参数：
//   - listType: string，"following" 或 "followers"。
//   - userID: uint64，目标用户 ID。
//
// 返回值：string，freecache 键名。
func (s *RelationService) l1KeyStr(listType string, userID uint64) string {
	return fmt.Sprintf("l1:%s:%d", listType, userID)
}

// toLongList 将 freecache 中的逗号分隔 ID 字符串解析为 uint64 切片。
//
// 功能：与 fillL1 反向操作，将存储为 "1001,1002,1003" 的字符串还原为 ID 列表。
//
// 参数：
//   - data: string，来自 freecache 的逗号分隔 ID 字符串。
//
// 返回值：[]uint64，解析后的 ID 列表。解析失败的 ID 被忽略。
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
//
// 功能：ZSet 的成员以字符串形式存储（如 "1001"、"1002"），
// 此函数将其解析为 uint64 列表。
//
// 参数：
//   - members: []string，Redis ZRevRange 返回的成员字符串列表。
//
// 返回值：[]uint64，转换后的 ID 列表。解析失败的成员被忽略。
func (s *RelationService) toIDList(members []string) []uint64 {
	result := make([]uint64, 0, len(members))
	for _, m := range members {
		if v, err := strconv.ParseUint(m, 10, 64); err == nil {
			result = append(result, v)
		}
	}
	return result
}

// invalidateCaches 在关注/取关操作后，失效涉及用户的 L1（freecache）和 L2（Redis ZSet）缓存。
//
// 功能：失效发起人（fromUserID）的关注列表缓存和被关注者（toUserID）的粉丝列表缓存。
// 这样可以确保下次查询时不会读到过期的关注/粉丝数据。
//
// WHY 只失效两个列表缓存而非四个：
//   - 关注 fromUserID 的粉丝列表不受影响（fromUserID 的粉丝没变化）。
//   - toUserID 的关注列表不受影响（toUserID 的关注人没变化）。
//
// 参数：
//   - ctx: context.Context。
//   - fromUserID: uint64，关注/取关的发起人。
//   - toUserID: uint64，关注/取关的目标。
func (s *RelationService) invalidateCaches(ctx context.Context, fromUserID, toUserID uint64) {
	// 失效 fromUserID 的关注列表缓存
	s.redis.Del(ctx, s.zsetKey("following", fromUserID))
	s.l1.Del([]byte(s.l1KeyStr("following", fromUserID)))
	// 失效 toUserID 的粉丝列表缓存
	s.redis.Del(ctx, s.zsetKey("followers", toUserID))
	s.l1.Del([]byte(s.l1KeyStr("followers", toUserID)))
}
