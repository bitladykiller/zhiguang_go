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
//
// 功能：
//  1. 读取 Redis 中 SDS 键的二进制值。
//  2. 如果键不存在，初始化为全 0（长度为 schemaLen × fieldSize）。
//  3. 在指定槽位（idx）上增加 delta（可为正数或负数）。
//  4. 如果结果 < 0 则截断为 0（不允许负数计数）。
//  5. 写回 Redis。
//
// KEYS[1]：SDS 键（cnt:{entityType}:{entityID}）
// ARGV[1]：schemaLen（5，指标个数）
// ARGV[2]：fieldSize（4，每个指标占的字节数）
// ARGV[3]：idx（Lua 中槽位索引，从 1 开始，对应 Go 的 idx+1）
// ARGV[4]：delta（增量，+1 或 -1）
//
// Lua 辅助函数说明：
//   - read32be(s, off): 从字符串 s 的 off 偏移处读取 4 字节大端 uint32
//     通过 string.byte 逐字节取出，手动拼装为整数
//   - write32be(n): 将整数 n 编码为 4 字节大端字符串
//     通过取模运算逐字节分解，用 string.char 组装
//   - string.sub(s, 1, off): 取字符串从开头到 off 的子串
//   - string.sub(s, off+fieldSize+1): 取字符串从 off+fieldSize+1 到末尾的子串
//     两者拼接起来替换掉中间的 fieldSize 字节，实现定点写入
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
//
//	toggle (Lua) → 修改位图 → 失效 SDS（触发按需重建） → 发送 Kafka 事件（异步）
type CounterService struct {
	redis    *redis.Client
	producer *CounterEventProducer
}

// NewCounterService 创建计数器服务实例。
//
// 参数：
//   - rdb: Redis 客户端，用于执行 Lua 脚本和 SDS/Bitmap 操作
//   - producer: Kafka 事件生产者，用于异步发布计数变更事件
func NewCounterService(rdb *redis.Client, producer *CounterEventProducer) *CounterService {
	return &CounterService{redis: rdb, producer: producer}
}

// ============================================================================
// 开关操作
// ============================================================================

// Like 为指定用户对指定实体打开点赞状态。
//
// 参数：
//   - userID: 操作用户 ID
//   - entityType: 实体类型（如 "knowpost"）
//   - entityID: 实体 ID 的字符串表示
//
// 返回值：
//   - bool: true 表示状态发生变化（从未点赞变为已点赞），false 表示已经是点赞状态
//   - error: Redis 操作失败时返回
func (s *CounterService) Like(ctx context.Context, userID uint64, entityType, entityID string) (bool, error) {
	return s.toggle(ctx, userID, entityType, entityID, "like", "add")
}

// Unlike 为指定用户取消对指定实体的点赞状态。
//
// 参数同 Like，但操作方向相反。
func (s *CounterService) Unlike(ctx context.Context, userID uint64, entityType, entityID string) (bool, error) {
	return s.toggle(ctx, userID, entityType, entityID, "like", "remove")
}

// Fav 为指定用户对指定实体打开收藏状态。
func (s *CounterService) Fav(ctx context.Context, userID uint64, entityType, entityID string) (bool, error) {
	return s.toggle(ctx, userID, entityType, entityID, "fav", "add")
}

// Unfav 为指定用户取消对指定实体的收藏状态。
func (s *CounterService) Unfav(ctx context.Context, userID uint64, entityType, entityID string) (bool, error) {
	return s.toggle(ctx, userID, entityType, entityID, "fav", "remove")
}

// IncrementFollowings 增量更新用户维度的关注数。
//
// delta = +1 表示新增一个关注，-1 表示减少一个关注。
// 通过 INCR_SDS_FIELD_LUA 脚本原子更新 SDS 中的 following 槽位。
func (s *CounterService) IncrementFollowings(ctx context.Context, userID uint64, delta int) error {
	return s.incrementUserMetric(ctx, userID, "following", delta)
}

// IncrementFollowers 增量更新用户维度的粉丝数。
//
// delta = +1 表示新增一个粉丝，-1 表示减少一个粉丝。
func (s *CounterService) IncrementFollowers(ctx context.Context, userID uint64, delta int) error {
	return s.incrementUserMetric(ctx, userID, "follower", delta)
}

// toggle 执行原子 Lua 脚本；如果状态发生变化，则发布 CounterEvent 到 Kafka。
//
// 参数：
//   - userID: 操作用户 ID
//   - entityType: 实体类型
//   - entityID: 实体 ID
//   - metric: 指标名称（"like" / "fav"）
//   - op: 操作类型（"add" / "remove"）
//
// 执行流程：
//  1. 计算用户 ID 对应的 bitmap 分片和位偏移（ChunkOf / BitOf）。
//  2. 构造完整的 bitmap 键：bm:{metric}:{entityType}:{entityID}:{chunk}。
//  3. 使用 Redis Eval 执行 TOGGLE_LUA 脚本，原子完成 GETBIT + SETBIT。
//  4. 如果 val == 1（状态发生变化）：
//     a. 立即让 SDS 失效（invalidateDerivedCounts），这样下一次读取会从位图重建。
//     b. 异步发送 CounterEvent 到 Kafka（fire-and-forget）。
//
// 函数调用说明：
//   - redis.Eval(ctx, script, keys, args...):
//     go-redis 的 Eval 方法用于执行 Lua 脚本。
//     第一个参数是 Lua 脚本文本。
//     keys 是 KEYS 数组（Lua 中的 KEYS[]）。
//     args 是 ARGV 数组（Lua 中的 ARGV[]）。
//     .Int() 将 Lua 返回值转为 Go int。
//
// 设计决策：
//   - Bitmap 是权威数据源，因此 toggle 后立即失效 SDS。
//     这样即使 Kafka 异步聚合延迟或失败，后续读取也会从最新位图重建 SDS。
//   - Kafka 发布采用 fire-and-forget（goroutine 异步执行），
//     不阻塞主请求路径。计数事件可以容忍偶尔丢失。
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
			Index:      NameToIdx[metric],
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

// incrementUserMetric 增量更新用户维度的计数指标。
//
// 参数：
//   - userID: 用户 ID
//   - metric: 指标名称（"following" / "follower"）
//   - delta: 增量（+1 或 -1）
//
// 函数调用说明：
//   - s.redis.Eval(ctx, INCR_SDS_FIELD_LUA, []string{key}, SchemaLen, FieldSize, idx+1, delta):
//     执行 INCR_SDS_FIELD_LUA 脚本。注意 idx+1 是因为 Lua 数组索引从 1 开始。
//     .Err() 只检查返回值的错误，不关心脚本本身的返回值。
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

// GetCounts 读取指定实体的 SDS（序列数据结构）计数值。
//
// 功能：
//  1. 通过 Redis GET 命令读取 SDS 二进制数据。
//  2. 如果键不存在（redis.Nil）或数据长度不合法（不等于 SchemaLen*FieldSize），
//     则触发按需重建（rebuildSds）。
//  3. 从重建结果中按请求的 metrics 列表提取对应槽位的计数值。
//  4. 重建失败时返回全零的空计数（不阻塞请求）。
//
// 参数：
//   - entityType: 实体类型（如 "knowpost"、"user"）
//   - entityID:   实体的字符串 ID
//   - metrics:    需要读取的指标名称列表（如 ["like", "fav"]）
//
// 返回值：
//   - map[string]int32: 指标名称到计数值的映射。如果某个指标不在 schema 中则被跳过。
//   - error: Redis 发生非预期错误时返回（如连接断开），此时整个请求应失败。
//
// 函数调用说明：
//   - s.redis.Get(ctx, key).Bytes():
//     go-redis 的 Get 命令返回 Redis 字符串值。.Bytes() 将返回值解析为 []byte。
//     如果键不存在，.Bytes() 会返回 redis.Nil 错误。
//   - redis.Nil: go-redis 中表示键不存在的哨兵错误值。
//     这是预期的正常情况，不应视为错误。
//
// 边界情况：
//   - SDS 缺失 → 尝试重建，重建失败则返回全零计数
//   - SDS 损坏（长度不对）→ 同上处理，相当于重建
//   - 请求的 metrics 中有未知指标名 → 自动跳过（查 NameToIdx 映射）
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

// IsLiked 判断指定用户是否已给该实体点赞。
//
// 功能：
//
//	通过 Redis GETBIT 命令检查点赞位图中用户 ID 对应位置是否为 1。
//	位图键格式：bm:like:{entityType}:{entityID}:{chunk}
//
// 参数：
//   - userID:     要查询的用户 ID
//   - entityType: 实体类型
//   - entityID:   实体 ID
//
// 返回值：
//   - bool:  true=已点赞，false=未点赞
//   - error: Redis 操作失败时返回
//
// 函数调用说明：
//   - s.redis.GetBit(ctx, key, offset).Result():
//     go-redis 的 GETBIT 命令返回指定偏移量上的位值（0 或 1）。
//     offset 使用 int64 类型。
//
// 设计决策：
//
//	这里直接读取位图而非 SDS，因为 SDS 是延迟重建的（仅在读取计数时触发）。
//	IsLiked 只关心当前用户的单一状态，直接读位图更快且保证实时性。
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

// IsFaved 判断指定用户是否已收藏该实体。
//
// 功能：
//
//	通过 Redis GETBIT 命令检查收藏位图中用户 ID 对应位置是否为 1。
//	位图键格式：bm:fav:{entityType}:{entityID}:{chunk}
//
// 参数：
//   - userID:     要查询的用户 ID
//   - entityType: 实体类型
//   - entityID:   实体 ID
//
// 返回值：
//   - bool:  true=已收藏，false=未收藏
//   - error: Redis 操作失败时返回
//
// 设计考虑与 IsLiked 相同，直接读位图以保证实时性。
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

// GetCountsBatch 使用 Redis Pipeline 批量获取多个实体的 SDS 计数。
//
// 功能：
//  1. 为每个 entityID 构造对应的 SDS 键。
//  2. 将所有 GET 命令放入一个 Redis Pipeline 中批量发送，减少网络往返。
//  3. 遍历返回结果，解析 SDS 二进制数据提取请求的指标计数值。
//  4. 跳过缺失或数据长度不合法的实体（返回结果中不包含该实体）。
//
// 参数：
//   - entityType: 实体类型（所有 entityID 必须属于同一类型）
//   - entityIDs:  需要查询的实体 ID 列表
//   - metrics:    需要读取的指标名称列表
//
// 返回值：
//   - map[string]map[string]int32: 实体 ID => (指标名 => 计数值) 的嵌套映射。
//     数据缺失的实体不会出现在返回结果中。
//   - error: Pipeline 执行失败时返回
//
// 函数调用说明：
//   - s.redis.Pipeline():
//     go-redis 的 Pipeline 机制将多个命令打包在一次 TCP 请求中发送，
//     减少客户端与 Redis 之间的网络往返次数（RTT），适合批量查询场景。
//   - pipe.Get(ctx, k):
//     将 GET 命令加入 Pipeline 队列，返回 *redis.StringCmd 句柄。
//     Pipeline 执行（pipe.Exec）后，通过 cmd.Bytes() 获取结果。
//   - cmd.Bytes():
//     从 *redis.StringCmd 中获取原始字节值。
//     如果键不存在，会返回 redis.Nil 错误。
//
// 边界情况：
//   - entityIDs 为空时直接返回 nil（无操作）
//   - 某个实体的 SDS 不存在 → 该实体不在返回结果中（不报错）
//   - 某个实体的 SDS 长度不合法 → 跳过该实体
//   - 请求的指标名称不在 schema 中 → 自动跳过
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

// rebuildSds 从位图重建 SDS（序列数据结构）计数。
//
// 功能：
//
//	SDS（Serial Data Structure）是通过 BITCOUNT 聚合位图数据生成的缓存计数。
//	当 SDS 缺失、损坏或过期时调用此函数重建。
//
// 重建流程（带退避 + 限流 + 分布式锁保护）：
//
//	Step 1: 检查是否处于退避期（inBackoff），如果是则拒绝重建。
//	Step 2: 检查是否超过限流水位（allowedByRateLimiter），否则提升退避等级并拒绝。
//	Step 3: 通过 SETNX 获取分布式锁（10s TTL），防止多实例同时重建同一 SDS。
//	Step 4: 遍历所有指标（like/fav/follower/following/posts），
//	        对每个指标调用 bitCountShards 汇总所有位图片段的 BITCOUNT 值。
//	Step 5: 将汇总结果以固定长度（4 字节/字段）写入 SDS 字节数组。
//	Step 6: 将 SDS 写回 Redis（SET 命令，不过期）。
//	Step 7: 清理聚合桶键（AggKey），释放锁并重置退避状态。
//
// 参数：
//   - entityType: 实体类型
//   - entityID:   实体 ID
//
// 返回值：
//   - []byte: 重建后的 SDS 二进制数据（SchemaLen * FieldSize 字节）
//   - error: 退避中、被限流、被锁定或 Redis 操作失败时返回错误
//
// 函数调用说明：
//   - s.redis.SetNX(ctx, key, value, expiration).Result():
//     SETNX = "SET if Not eXists"，原子操作。
//     如果 key 不存在则设置并返回 true，已存在则返回 false。
//     作为分布式锁的基础：持有锁的实例设置它，其他实例看到已存在就不会重复执行。
//   - s.redis.Del(ctx, key):
//     释放分布式锁或删除缓存键。
//
// 设计决策：
//   - 位图是权威数据源，SDS 是通过聚合位图得到的缓存数据。
//     这样设计的优势是 toggle 操作只需修改位图（O(1)），
//     而 SDS 在需要时才重建（lazy rebuild），避免每次 toggle 都执行 BITCOUNT。
//   - 退避策略防止频繁失败的重建请求过度消耗 Redis 资源。
//   - SETNX 分布式锁防止多实例并发重建同一个 SDS，造成"惊群效应"。
//   - 聚合桶清理：AGG 键是存储增量事件的桶，重建后桶数据已经过期，故清理。
//   - 重建成功后 SDS 不过期（TTL=0），由外部失效机制控制其生命周期。
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

// bitCountShards 统计指定指标的所有位图片段的 SETBIT 总数量。
//
// 功能：
//  1. 使用 Redis KEYS 命令匹配模式 `bm:{metric}:{entityType}:{entityID}:*`
//     找出所有相关位图片段。
//  2. 对每个匹配的位图键执行 BITCOUNT，通过 Pipeline 批量发送。
//  3. 汇总所有分片的 BITCOUNT 结果。
//
// 参数：
//   - metric:     指标名称（"like"/"fav"/"follower"/"following"/"posts"）
//   - entityType: 实体类型
//   - entityID:   实体 ID
//
// 返回值：
//   - int64: 所有位图片段中值为 1 的位总数（即该指标的总计数）
//   - error: KEYS 或 Pipeline 执行失败时返回
//
// 函数调用说明：
//   - s.redis.Keys(ctx, pattern).Result():
//     Redis KEYS 命令，返回匹配模式的所有键名。
//     注意：生产环境应避免使用 KEYS（会阻塞 Redis 单线程），
//     但此处仅在重建 SDS 时调用，频率不高，可接受。
//   - pipe.BitCount(ctx, key, nil):
//     Redis BITCOUNT 命令，统计字符串中值为 1 的位数。
//     nil 表示统计整个字符串的全部字节。
//
// 注意：
//   - 使用 KEYS 而非 SCAN 是因为 SDS 重建是低频操作，
//     简化实现比性能优化更重要。
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

// inBackoff 检查当前实体是否处于退避期。
//
// 功能：
//
//	读取 Redis 中的退避截止时间戳（毫秒级 Unix 时间戳），
//	如果当前时间小于截止时间，返回 true，表示应跳过重建。
//
// 退避机制用于防止频繁失败的重建请求压垮 Redis。
// 当重建因限流、锁抢占或其他错误失败时，会设置退避期。
//
// 参数：
//   - entityType: 实体类型
//   - entityID:   实体 ID
//
// 返回值：
//   - bool: true=处于退避期，应跳过重建；false=不在退避期，可以重建
//
// 注意：
//
//	如果 Redis 中不存在退避键（GET 返回 redis.Nil），
//	.Int64() 会返回 0 和错误，此时视为不在退避期，返回 false。
func (s *CounterService) inBackoff(ctx context.Context, entityType, entityID string) bool {
	until, err := s.redis.Get(ctx, s.backoffKey(entityType, entityID)).Int64()
	if err != nil {
		return false
	}
	return time.Now().UnixMilli() < until
}

// escalateBackoff 提升指定实体的退避等级（指数退避）。
//
// 功能：
//  1. 读取当前的退避指数（exp），初始为 0。
//  2. 计算退避时长：baseMs << exp（指数增长），上限 30 秒。
//  3. 设置退避截止时间戳（当前时间 + 退避时长）。
//  4. 退避指数 +1（下次退避时间翻倍）。
//  5. 删除限流器键，让新的退避期从零开始累加。
//
// 指数退避时间序列：500ms → 1s → 2s → 4s → 8s → 16s → 30s（cap）
//
// 参数：
//   - entityType: 实体类型
//   - entityID:   实体 ID
//
// 函数调用说明：
//   - s.redis.Get(ctx, key).Int():
//     .Int() 将 Redis 返回的字符串值解析为 int 类型。
//     如果键不存在，返回 0（零值）。
//   - s.redis.Set(ctx, key, value, expiration):
//     Set 的过期时间为 0 表示永不过期，由 resetBackoff 或下次 escalate 时覆盖。
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

// resetBackoff 重置指定实体的退避状态。
//
// 功能：
//
//	删除 Redis 中的退避截止键、退避指数键和限流器键，
//	使该实体可以从零开始接受下一次重建。
//
// 调用时机：
//
//	当 SDS 成功重建后调用，清除之前的失败状态。
//
// 参数：
//   - entityType: 实体类型
//   - entityID:   实体 ID
func (s *CounterService) resetBackoff(ctx context.Context, entityType, entityID string) {
	s.redis.Del(ctx, s.backoffKey(entityType, entityID))
	s.redis.Del(ctx, s.backoffExpKey(entityType, entityID))
	s.redis.Del(ctx, s.rateLimiterKey(entityType, entityID))
}

// allowedByRateLimiter 检查当前是否允许触发 SDS 重建（基于 Redis 的简单限流器）。
//
// 功能：
//  1. 使用 Redis INCR 递增限流器计数。
//  2. 如果这是该时间窗口内的第一次递增（val==1），设置 60 秒过期时间。
//  3. 如果当前计数 <= 5（允许的每分钟最大重建次数），返回 true。
//
// 参数：
//   - entityType: 实体类型
//   - entityID:   实体 ID
//
// 返回值：
//   - bool: true=允许重建，false=已超过限流阈值
//
// 函数调用说明：
//   - s.redis.Incr(ctx, key).Result():
//     Redis INCR 命令将键的值原子递增 1，并返回递增后的值。
//     如果键不存在，会自动创建并从 0 开始。
//   - s.redis.Expire(ctx, key, duration):
//     为键设置过期时间。结合 INCR 实现滑动窗口限流器：
//     第一次 INCR 后设置过期，后续 INCR 会继续累加，
//     过期后自动归零，从而实现"每分钟最多 N 次"的限流语义。
//
// 边界情况：
//   - val == 1 时设置过期时间，后续请求不需要重复设置
//   - INCR 或 Expire 失败时静默忽略错误（val 为 0，视为允许）
func (s *CounterService) allowedByRateLimiter(ctx context.Context, entityType, entityID string) bool {
	key := s.rateLimiterKey(entityType, entityID)
	val, _ := s.redis.Incr(ctx, key).Result()
	if val == 1 {
		s.redis.Expire(ctx, key, 60*time.Second)
	}
	return val <= 5
}

// emptyCounts 为请求的指标列表生成全零的计数值映射。
//
// 功能：
//
//	创建一个 map，将每个指标名映射为 0。
//
// 参数：
//   - metrics: 指标名称列表
//
// 返回值：
//   - map[string]int32: 各指标均为 0 的映射
//
// 使用场景：
//
//	当 SDS 重建失败时，用全零结果替代，使调用方不至于崩溃。
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

// readInt32BE 从字节数组中按大端序读取 int32 值。
//
// 功能：
//
//	从偏移量 offset 开始读取 4 字节，使用大端字节序解码为 int32。
//	SDS 中每个计数值以 4 字节大端整数存储。
//
// 参数：
//   - b:      源字节数组
//   - offset: 起始偏移量（字节）
//
// 返回值：
//   - int32: 解码后的 32 位有符号整数
//
// 函数调用说明：
//   - binary.BigEndian.Uint32(b[offset:]):
//     encoding/binary 包提供的大端序解码函数。
//     BigEndian 是 ByteOrder 类型实例。
//     Uint32(b) 从 b 的前 4 字节解码 uint32。
//     int32(...) 将无符号转为有符号（适用于计数场景）。
//
// 设计决策：
//
//	使用大端序（网络字节序）是为了与 Java 实现保持兼容。
//	Java 中的 DataOutputStream.writeInt() 默认使用大端序。
func readInt32BE(b []byte, offset int) int32 {
	return int32(binary.BigEndian.Uint32(b[offset:]))
}

// writeInt32BE 将 int32 值按大端序写入字节数组的指定偏移位置。
//
// 功能：
//
//	将 val 编码为 4 字节大端序，写入 b[offset:offset+4] 位置。
//	用于在 SDS 字节数组中写入单个计数值。
//
// 参数：
//   - b:      目标字节数组
//   - offset: 起始偏移量（字节）
//   - val:    要写入的 32 位有符号整数
//
// 函数调用说明：
//   - binary.BigEndian.PutUint32(b[offset:], uint32(val)):
//     大端序编码函数。将 uint32 值编码为 4 字节写入切片。
//     需要将 int32 强转为 uint32。
//
// 注意：
//
//	调用方需保证 b[offset:offset+4] 在数组范围内，否则 panic。
//	writeInt32BE 始终写入刚好 4 字节。
func writeInt32BE(b []byte, offset int, val int32) {
	binary.BigEndian.PutUint32(b[offset:], uint32(val))
}

// invalidateDerivedCounts 清除指定实体的衍生计数缓存，触发下次读取时重建。
//
// 功能：
//
//	删除 SDS 缓存键和聚合桶键。
//	这样下次读取 GetCounts 时会发现 SDS 缺失，进而触发 rebuildSds 重建。
//
// 参数：
//   - entityType: 实体类型
//   - entityID:   实体 ID
//
// 调用时机：
//
//	在 toggle 操作（Like/Unlike/Fav/Unfav）改变位图后立即调用。
//	这样即使 Kafka 事件异步消费有延迟，后续读取计数时仍会从最新位图重建。
//
// 设计决策：
//
//	位图（Bitmap）是权威数据源，SDS 和聚合桶都是可丢弃的衍生数据。
//	当位图发生变化时，直接清除衍生数据，让下一次读取自然从位图重建，
//	保证最终一致性。
func (s *CounterService) invalidateDerivedCounts(ctx context.Context, entityType, entityID string) {
	s.redis.Del(ctx, SdsKey(entityType, entityID))
	s.redis.Del(ctx, AggKey(entityType, entityID))
}
