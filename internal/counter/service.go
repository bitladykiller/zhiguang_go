package counter

import (
	"context"
	"encoding/binary"
	"fmt"
	"strconv"
	"time"

	"github.com/redis/go-redis/v9"
)

// TOGGLE_LUA 以原子方式切换位图中的单个位，并返回状态是否发生变化。
//
// 功能：如果操作是 "add"，当前位为 0 则设为 1 并返回 1；已为 1 则返回 0。
// 如果操作是 "remove"，当前位为 1 则设为 0 并返回 1；已为 0 则返回 0。
// 无效的操作（非 add/remove）返回 -1。
//
// KEYS[1]：位图键（bm:{metric}:{entityType}:{entityID}:{chunk}）
// ARGV[1]：位偏移（用户 ID 在分片内的位置）
// ARGV[2]：操作类型（"add" 或 "remove"）
//
// 返回值：1=状态发生变化，0=无变化，-1=未知操作
const TOGGLE_LUA = `
local bmKey = KEYS[1]
local offset = tonumber(ARGV[1])
local op = ARGV[2]
local prev = redis.call('GETBIT', bmKey, offset)
if op == 'add' then
  if prev == 1 then return 0 end
  redis.call('SETBIT', bmKey, offset, 1)
  return 1
elseif op == 'remove' then
  if prev == 0 then return 0 end
  redis.call('SETBIT', bmKey, offset, 0)
  return 1
end
return -1
`

// INCR_SDS_FIELD_LUA 原子递增指定 SDS 槽位。
const INCR_SDS_FIELD_LUA = `
local cntKey = KEYS[1]
local schemaLen = tonumber(ARGV[1])
local fieldSize = tonumber(ARGV[2])
local idx = tonumber(ARGV[3])
local delta = tonumber(ARGV[4])
local function read32be(s, off)
  local b = {string.byte(s, off+1, off+4)}
  local n = 0
  for i=1,4 do n = n * 256 + b[i] end
  return n
end
local function write32be(n)
  local t = {}
  for i=4,1,-1 do t[i] = n % 256; n = math.floor(n/256) end
  return string.char(unpack(t))
end
local cnt = redis.call('GET', cntKey)
if not cnt then cnt = string.rep(string.char(0), schemaLen * fieldSize) end
local off = (idx - 1) * fieldSize
local v = read32be(cnt, off) + delta
if v < 0 then v = 0 end
local seg = write32be(v)
cnt = string.sub(cnt, 1, off) .. seg .. string.sub(cnt, off+fieldSize+1)
redis.call('SET', cntKey, cnt)
return 1
`

// CounterService 提供原子化的计数开关操作。
//
// 设计模式：
//   - Strategy（策略模式）：Like/Unlike/Fav/Unfav 都是 toggle(add/remove) 的不同变体，
//     共用同一套位图操作逻辑，只是传入的 metric 和 op 参数不同。
//   - Circuit Breaker（断路器模式）：SDS 重建失败后采用指数退避，
//     退避时间呈指数增长（500ms → 1s → 2s → ... → 30s cap），
//     避免持续失败的请求压迫数据库。
//   - Distributed Lock（分布式锁模式）：通过 Redis SETNX 防止多个服务实例
//     同时对同一个 SDS 执行重建操作。
//
// 数据流：
//   toggle (Lua) → 修改位图 → 失效 SDS（触发按需重建） → 发送 Kafka 事件（异步）
type CounterService struct {
	redis    *redis.Client
	producer *CounterEventProducer
}

func NewCounterService(rdb *redis.Client, producer *CounterEventProducer) *CounterService {
	return &CounterService{redis: rdb, producer: producer}
}

// ============================================================================
// 开关操作
// ============================================================================

// Like 打开一次点赞状态。
func (s *CounterService) Like(ctx context.Context, userID uint64, entityType, entityID string) (bool, error) {
	return s.toggle(ctx, userID, entityType, entityID, "like", "add")
}

// Unlike 取消一次点赞状态。
func (s *CounterService) Unlike(ctx context.Context, userID uint64, entityType, entityID string) (bool, error) {
	return s.toggle(ctx, userID, entityType, entityID, "like", "remove")
}

// Fav 打开一次收藏状态。
func (s *CounterService) Fav(ctx context.Context, userID uint64, entityType, entityID string) (bool, error) {
	return s.toggle(ctx, userID, entityType, entityID, "fav", "add")
}

// Unfav 取消一次收藏状态。
func (s *CounterService) Unfav(ctx context.Context, userID uint64, entityType, entityID string) (bool, error) {
	return s.toggle(ctx, userID, entityType, entityID, "fav", "remove")
}

// IncrementFollowings 增量更新用户维度的关注数。
func (s *CounterService) IncrementFollowings(ctx context.Context, userID uint64, delta int) error {
	return s.incrementUserMetric(ctx, userID, "following", delta)
}

// IncrementFollowers 增量更新用户维度的粉丝数。
func (s *CounterService) IncrementFollowers(ctx context.Context, userID uint64, delta int) error {
	return s.incrementUserMetric(ctx, userID, "follower", delta)
}

// toggle 执行原子 Lua 脚本；如果状态发生变化，则向 Kafka 发布 CounterEvent。
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
		// Bitmap 是权威数据源，因此要立即让 SDS 失效。
		// 这样即使 Kafka 异步聚合延迟或失败，后续读取也会从最新位图重建。
		s.invalidateDerivedCounts(ctx, entityType, entityID)

		event := &CounterEvent{
			EntityType: entityType,
			EntityID:   entityID,
			Metric:     metric,
			Index:      int(chunk),
			UserID:     userID,
			Delta:      delta,
		}
		// 这里采用 fire-and-forget，Kafka 发布只做尽力而为。
		if s.producer != nil {
			go func() { _ = s.producer.Publish(event) }()
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

// ============================================================================
// 读操作
// ============================================================================

// GetCounts 读取 SDS 计数值；如果缺失或损坏，则触发重建。
func (s *CounterService) GetCounts(ctx context.Context, entityType, entityID string, metrics []string) (map[string]int32, error) {
	sdsKey := SdsKey(entityType, entityID)

	raw, err := s.redis.Get(ctx, sdsKey).Bytes()
	if err == redis.Nil || len(raw) != SchemaLen*FieldSize {
		// 尝试重建（带退避和分布式锁）
		raw, err = s.rebuildSds(ctx, entityType, entityID)
		if err != nil {
			return s.emptyCounts(metrics), nil
		}
		if len(raw) != SchemaLen*FieldSize {
			return s.emptyCounts(metrics), nil
		}
	} else if err != nil {
		return nil, fmt.Errorf("redis get: %w", err)
	}

	result := make(map[string]int32, len(metrics))
	for _, m := range metrics {
		idx, ok := NameToIdx[m]
		if !ok {
			continue
		}
		result[m] = readInt32BE(raw, idx*FieldSize)
	}
	return result, nil
}

// IsLiked 通过 GETBIT 判断用户是否点赞过该实体。
func (s *CounterService) IsLiked(ctx context.Context, userID uint64, entityType, entityID string) (bool, error) {
	chunk := ChunkOf(userID)
	offset := BitOf(userID)
	bmKey := BitmapKey("like", entityType, entityID, chunk)
	val, err := s.redis.GetBit(ctx, bmKey, int64(offset)).Result()
	if err != nil {
		return false, err
	}
	return val == 1, nil
}

// IsFaved 通过 GETBIT 判断用户是否收藏过该实体。
func (s *CounterService) IsFaved(ctx context.Context, userID uint64, entityType, entityID string) (bool, error) {
	chunk := ChunkOf(userID)
	offset := BitOf(userID)
	bmKey := BitmapKey("fav", entityType, entityID, chunk)
	val, err := s.redis.GetBit(ctx, bmKey, int64(offset)).Result()
	if err != nil {
		return false, err
	}
	return val == 1, nil
}

// GetCountsBatch 使用 pipeline 批量获取多个实体的 SDS 计数。
func (s *CounterService) GetCountsBatch(ctx context.Context, entityType string, entityIDs, metrics []string) (map[string]map[string]int32, error) {
	if len(entityIDs) == 0 {
		return nil, nil
	}

	keys := make([]string, len(entityIDs))
	keyToEID := make(map[string]string, len(entityIDs))
	for i, eid := range entityIDs {
		k := SdsKey(entityType, eid)
		keys[i] = k
		keyToEID[k] = eid
	}

	// 使用 Pipeline 批量 GET
	pipe := s.redis.Pipeline()
	cmds := make([]*redis.StringCmd, len(keys))
	for i, k := range keys {
		cmds[i] = pipe.Get(ctx, k)
	}
	pipe.Exec(ctx)

	result := make(map[string]map[string]int32, len(entityIDs))
	for i, cmd := range cmds {
		raw, err := cmd.Bytes()
		if err != nil || len(raw) != SchemaLen*FieldSize {
			continue
		}
		counts := make(map[string]int32, len(metrics))
		for _, m := range metrics {
			idx, ok := NameToIdx[m]
			if !ok {
				continue
			}
			counts[m] = readInt32BE(raw, idx*FieldSize)
		}
		result[keyToEID[keys[i]]] = counts
	}
	return result, nil
}

// ============================================================================
// SDS 重建
// ============================================================================

func (s *CounterService) rebuildSds(ctx context.Context, entityType, entityID string) ([]byte, error) {
	sdsKey := SdsKey(entityType, entityID)

	// 检查是否处于退避期
	if s.inBackoff(ctx, entityType, entityID) {
		return nil, fmt.Errorf("in backoff")
	}

	// 限流
	if !s.allowedByRateLimiter(ctx, entityType, entityID) {
		s.escalateBackoff(ctx, entityType, entityID)
		return nil, fmt.Errorf("rate limited")
	}

	// 分布式锁
	lockKey := fmt.Sprintf("lock:sds-rebuild:%s:%s", entityType, entityID)
	locked, err := s.redis.SetNX(ctx, lockKey, "1", 10*time.Second).Result()
	if err != nil || !locked {
		if err == nil {
			s.escalateBackoff(ctx, entityType, entityID)
		}
		return nil, fmt.Errorf("locked by another rebuild")
	}
	defer s.redis.Del(ctx, lockKey)

	// 统计所有位图片段
	metrics := []string{"like", "fav", "follower", "following", "posts"}
	raw := make([]byte, SchemaLen*FieldSize)
	for i, metric := range metrics {
		total, err := s.bitCountShards(ctx, metric, entityType, entityID)
		if err != nil {
			s.escalateBackoff(ctx, entityType, entityID)
			return nil, err
		}
		writeInt32BE(raw, i*FieldSize, int32(total))
	}

	// 回写 SDS
	if err := s.redis.Set(ctx, sdsKey, raw, 0).Err(); err != nil {
		s.escalateBackoff(ctx, entityType, entityID)
		return nil, err
	}

	// 清理聚合桶
	s.redis.Del(ctx, AggKey(entityType, entityID))

	// 重置退避状态
	s.resetBackoff(ctx, entityType, entityID)

	return raw, nil
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
	pipe.Exec(ctx)

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

// ============================================================================
// 退避与限流
// ============================================================================

func (s *CounterService) backoffKey(entityType, entityID string) string {
	return fmt.Sprintf("backoff:sds-rebuild:until:%s:%s", entityType, entityID)
}

func (s *CounterService) backoffExpKey(entityType, entityID string) string {
	return fmt.Sprintf("backoff:sds-rebuild:exp:%s:%s", entityType, entityID)
}

func (s *CounterService) rateLimiterKey(entityType, entityID string) string {
	return fmt.Sprintf("rl:sds-rebuild:%s:%s", entityType, entityID)
}

func (s *CounterService) inBackoff(ctx context.Context, entityType, entityID string) bool {
	until, err := s.redis.Get(ctx, s.backoffKey(entityType, entityID)).Int64()
	if err != nil {
		return false
	}
	return time.Now().UnixMilli() < until
}

func (s *CounterService) escalateBackoff(ctx context.Context, entityType, entityID string) {
	expKey := s.backoffExpKey(entityType, entityID)
	exp, _ := s.redis.Get(ctx, expKey).Int()

	ms := int64(500) << exp
	if ms > 30000 {
		ms = 30000
	}
	until := time.Now().UnixMilli() + ms

	s.redis.Set(ctx, s.backoffKey(entityType, entityID), until, 0)
	s.redis.Set(ctx, expKey, exp+1, 0)
	s.redis.Del(ctx, s.rateLimiterKey(entityType, entityID))
}

func (s *CounterService) resetBackoff(ctx context.Context, entityType, entityID string) {
	s.redis.Del(ctx, s.backoffKey(entityType, entityID))
	s.redis.Del(ctx, s.backoffExpKey(entityType, entityID))
	s.redis.Del(ctx, s.rateLimiterKey(entityType, entityID))
}

func (s *CounterService) allowedByRateLimiter(ctx context.Context, entityType, entityID string) bool {
	key := s.rateLimiterKey(entityType, entityID)
	val, _ := s.redis.Incr(ctx, key).Result()
	if val == 1 {
		s.redis.Expire(ctx, key, 60*time.Second)
	}
	return val <= 5
}

func (s *CounterService) emptyCounts(metrics []string) map[string]int32 {
	m := make(map[string]int32, len(metrics))
	for _, k := range metrics {
		m[k] = 0
	}
	return m
}

// ============================================================================
// 二进制辅助函数
// ============================================================================

func readInt32BE(b []byte, offset int) int32 {
	return int32(binary.BigEndian.Uint32(b[offset:]))
}

func writeInt32BE(b []byte, offset int, val int32) {
	binary.BigEndian.PutUint32(b[offset:], uint32(val))
}

func (s *CounterService) invalidateDerivedCounts(ctx context.Context, entityType, entityID string) {
	s.redis.Del(ctx, SdsKey(entityType, entityID))
	s.redis.Del(ctx, AggKey(entityType, entityID))
}
