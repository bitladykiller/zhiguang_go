// Package cache 的存在性过滤器：基于 Redis 的共享 Cuckoo Filter（可删除）。
//
// 设计目标：
//   - 与详情「空值缓存」叠加：过滤器拦截「一定不存在」的扫号请求；
//     误判为可能存在时，仍走 L1/L2/读穿锁，并由 NULL 缓存兜底。
//   - 多实例共享：表数据存在 Redis，Lua 保证 Add/Delete/Lookup 原子性。
//   - 无第三方依赖 / 不依赖 RedisBloom 模块：标准 Redis GETRANGE/SETRANGE + EVAL。
//   - 支持可靠删除：软删后 Delete 清除 fingerprint，减少对 NULL/DB 的依赖。
//
// 语义（Cuckoo Filter）：
//   - MightContain=false → 一定不存在（可直接 404）
//   - MightContain=true  → 可能存在（必须继续查缓存/DB）
//   - Delete 成功清除本成员 fingerprint（若因指纹碰撞误删他人槽位，概率极低）
//
// Redis 布局：
//   - {key}      : 连续字节表，每个 bucket 含 bucketSize 个 1-byte fingerprint（0=空）
//   - {key}:n    : 近似元素计数，>0 表示已预热（IsWarm）
package cache

import (
	"context"
	"hash/fnv"
	"math"
	"strconv"

	"github.com/redis/go-redis/v9"
	"go.uber.org/zap"
)

const (
	cuckooBucketSize = 4
	cuckooLoadFactor = 0.95
	cuckooMaxKicks   = 500
	// fingerprint 固定 8 bit（非 0）；与 bucketSize=4 时 FPR 约在百分位量级，足够拦扫号。
	cuckooFPBits = 8
	// 与 Lua 内 alt 计算保持一致：alt = i XOR ((fp * golden) % m)
	cuckooAltMul = 2654435761
)

// BloomConfig 控制 Redis Cuckoo 的容量与误判目标。
// 字段名保留 Bloom* 以兼容既有配置与调用方。
type BloomConfig struct {
	// Enabled 为 false 时所有操作 no-op，保持与旧行为兼容。
	Enabled bool
	// ExpectedItems 预估元素量（用于计算 bucket 数 m）。
	ExpectedItems uint64
	// FalsePositiveRate 目标误判率（当前实现固定 8-bit fingerprint，该字段用于文档/兼容；过小会放大 m）。
	FalsePositiveRate float64
	// Key Redis 表键；空则使用默认 "cuckoo:knowpost:ids"。
	Key string
	// HashCount 保留字段：经典 Bloom 的 k；Cuckoo 忽略。
	HashCount int
	// BucketSize 每桶槽位数；0 表示默认 4。
	BucketSize int
	// MaxKicks 插入时最大踢出次数；0 表示默认 500。
	MaxKicks int
}

// DefaultBloomConfig 返回知文详情场景的合理默认值。
func DefaultBloomConfig() BloomConfig {
	return BloomConfig{
		Enabled:           true,
		ExpectedItems:     1_000_000,
		FalsePositiveRate: 0.01,
		Key:               "cuckoo:knowpost:ids",
		HashCount:         0,
		BucketSize:        cuckooBucketSize,
		MaxKicks:          cuckooMaxKicks,
	}
}

// RedisBloom 使用 Redis 字节表实现的共享 Cuckoo 过滤器。
// 类型名保留 RedisBloom，避免业务层大面积改名；语义已升级为可删除 Cuckoo。
type RedisBloom struct {
	rdb        *redis.Client
	key        string
	countKey   string
	m          uint64 // bucket 数（2 的幂）
	bucketSize int
	maxKicks   int
	tableBytes int
	logger     *zap.Logger
}

// NewRedisBloom 根据配置创建过滤器。cfg.Enabled=false 或 rdb=nil 时返回 nil。
func NewRedisBloom(rdb *redis.Client, cfg BloomConfig, logger *zap.Logger) *RedisBloom {
	if rdb == nil || !cfg.Enabled {
		return nil
	}
	if logger == nil {
		logger = zap.NewNop()
	}
	if cfg.ExpectedItems == 0 {
		cfg.ExpectedItems = 1_000_000
	}
	if cfg.FalsePositiveRate <= 0 || cfg.FalsePositiveRate >= 1 {
		cfg.FalsePositiveRate = 0.01
	}
	if cfg.Key == "" {
		cfg.Key = "cuckoo:knowpost:ids"
	}
	bs := cfg.BucketSize
	if bs <= 0 {
		bs = cuckooBucketSize
	}
	if bs > 16 {
		bs = 16
	}
	maxKicks := cfg.MaxKicks
	if maxKicks <= 0 {
		maxKicks = cuckooMaxKicks
	}

	// m = next_pow2(ceil(n / (load * bucketSize)))
	raw := uint64(math.Ceil(float64(cfg.ExpectedItems) / (cuckooLoadFactor * float64(bs))))
	m := nextPowerOfTwo(raw)
	if m < 64 {
		m = 64
	}
	// 误判目标更严时适当放大表（空间换更低碰撞）
	if cfg.FalsePositiveRate < 0.01 {
		m = nextPowerOfTwo(m * 2)
	}

	return &RedisBloom{
		rdb:        rdb,
		key:        cfg.Key,
		countKey:   cfg.Key + ":n",
		m:          m,
		bucketSize: bs,
		maxKicks:   maxKicks,
		tableBytes: int(m) * bs,
		logger:     logger,
	}
}

// Add 将成员加入过滤器。失败只记日志，不影响主写路径。
func (b *RedisBloom) Add(ctx context.Context, member string) {
	if b == nil || member == "" {
		return
	}
	i1, i2, fp := b.indices(member)
	res, err := cuckooAddScript.Run(ctx, b.rdb,
		[]string{b.key, b.countKey},
		b.m, b.bucketSize, i1, i2, fp, b.maxKicks, b.tableBytes,
	).Int()
	if err != nil {
		b.logger.Warn("cuckoo add failed", zap.String("member", member), zap.Error(err))
		return
	}
	if res == 0 {
		b.logger.Warn("cuckoo add kick exhausted", zap.String("member", member))
	}
}

// AddUint64 便捷方法：按十进制字符串加入。
func (b *RedisBloom) AddUint64(ctx context.Context, id uint64) {
	if b == nil {
		return
	}
	b.Add(ctx, strconv.FormatUint(id, 10))
}

// Delete 从过滤器删除成员（Cuckoo 支持可靠删除）。找不到则 no-op。
// 失败只记日志，不阻断软删主路径；NULL 缓存仍作兜底。
func (b *RedisBloom) Delete(ctx context.Context, member string) {
	if b == nil || member == "" {
		return
	}
	i1, i2, fp := b.indices(member)
	if _, err := cuckooDeleteScript.Run(ctx, b.rdb,
		[]string{b.key, b.countKey},
		b.m, b.bucketSize, i1, i2, fp,
	).Result(); err != nil {
		b.logger.Warn("cuckoo delete failed", zap.String("member", member), zap.Error(err))
	}
}

// DeleteUint64 便捷方法。
func (b *RedisBloom) DeleteUint64(ctx context.Context, id uint64) {
	if b == nil {
		return
	}
	b.Delete(ctx, strconv.FormatUint(id, 10))
}

// IsWarm 判断过滤器表是否已初始化。
//
// WHY 看数据键而非 count：全部 Delete 后 count 可为 0，但表仍有效，
// 此时应返回 MightContain=false，而不是 fail-open 放行所有 ID。
// 仅当表键不存在（冷启动未 Add/未预热）时 fail-open。
func (b *RedisBloom) IsWarm(ctx context.Context) bool {
	if b == nil {
		return false
	}
	n, err := b.rdb.Exists(ctx, b.key).Result()
	if err != nil {
		return false
	}
	return n > 0
}

// MightContain 判断成员是否可能存在。
//
// 返回：
//   - false, nil：一定不存在（仅当过滤器已预热）
//   - true, nil：可能存在（含误判），或过滤器未预热 / 故障 fail-open
//   - true, err：Redis 故障时 fail-open，避免误拦真实内容
func (b *RedisBloom) MightContain(ctx context.Context, member string) (bool, error) {
	if b == nil || member == "" {
		return true, nil
	}
	if !b.IsWarm(ctx) {
		return true, nil
	}
	i1, i2, fp := b.indices(member)
	res, err := cuckooLookupScript.Run(ctx, b.rdb,
		[]string{b.key},
		b.m, b.bucketSize, i1, i2, fp,
	).Int()
	if err != nil {
		b.logger.Warn("cuckoo might_contain failed, fail-open", zap.String("member", member), zap.Error(err))
		return true, err
	}
	// 1=found, 0=absent, 2=table missing (fail-open)
	if res == 2 {
		return true, nil
	}
	return res == 1, nil
}

// MightContainUint64 便捷方法。
func (b *RedisBloom) MightContainUint64(ctx context.Context, id uint64) (bool, error) {
	if b == nil {
		return true, nil
	}
	return b.MightContain(ctx, strconv.FormatUint(id, 10))
}

// Stats 返回表参数，便于运维与单测断言。
// 返回：key, bucket 数 m, fingerprint 位数（兼容旧签名第三项 k）。
func (b *RedisBloom) Stats() (key string, m uint64, k int) {
	if b == nil {
		return "", 0, 0
	}
	return b.key, b.m, cuckooFPBits
}

// indices 计算 Cuckoo 双桶下标与 fingerprint。
func (b *RedisBloom) indices(member string) (i1, i2 uint64, fp int) {
	h1, h2 := bloomHashes(member)
	fpByte := byte(h2 & 0xFF)
	if fpByte == 0 {
		fpByte = 1
	}
	i1 = h1 % b.m
	alt := (uint64(fpByte) * cuckooAltMul) % b.m
	i2 = (i1 ^ alt) % b.m
	return i1, i2, int(fpByte)
}

func bloomHashes(member string) (uint64, uint64) {
	a := fnv.New64a()
	_, _ = a.Write([]byte(member))
	h1 := a.Sum64()

	bh := fnv.New64()
	_, _ = bh.Write([]byte(member))
	// 混入长度避免与 h1 完全相关
	_, _ = bh.Write([]byte{byte(len(member)), byte(len(member) >> 8)})
	h2 := bh.Sum64()
	return h1, h2
}

func nextPowerOfTwo(n uint64) uint64 {
	if n <= 1 {
		return 1
	}
	n--
	n |= n >> 1
	n |= n >> 2
	n |= n >> 4
	n |= n >> 8
	n |= n >> 16
	n |= n >> 32
	return n + 1
}

// --- Lua：纯标准 Redis 命令，兼容 miniredis ---
// Redis Lua 5.1 无原生位运算，用纯 Lua 实现 bxor。

// language=Lua
const cuckooLuaBXOR = `
local function bxor(a, b)
  a = math.floor(tonumber(a) or 0)
  b = math.floor(tonumber(b) or 0)
  local r = 0
  local bitv = 1
  for _ = 1, 32 do
    local aa = a % 2
    local bb = b % 2
    if aa ~= bb then
      r = r + bitv
    end
    a = math.floor(a / 2)
    b = math.floor(b / 2)
    bitv = bitv * 2
  end
  return r
end
local function alt_index(i, fp, m)
  local h = (fp * 2654435761) % m
  return bxor(i, h) % m
end
`

// language=Lua
var cuckooAddScript = redis.NewScript(cuckooLuaBXOR + `
local key = KEYS[1]
local cntkey = KEYS[2]
local m = tonumber(ARGV[1])
local b = tonumber(ARGV[2])
local i1 = tonumber(ARGV[3])
local i2 = tonumber(ARGV[4])
local fp = tonumber(ARGV[5])
local maxk = tonumber(ARGV[6])
local tsize = tonumber(ARGV[7])

local function ensure()
  local len = redis.call('STRLEN', key)
  if len ~= tsize then
    redis.call('SET', key, string.rep('\0', tsize))
    redis.call('SET', cntkey, 0)
  end
end

local function get_bucket(idx)
  local start = idx * b
  return redis.call('GETRANGE', key, start, start + b - 1)
end

local function set_slot(idx, slot, val)
  -- slot 1-based
  local start = idx * b + (slot - 1)
  redis.call('SETRANGE', key, start, string.char(val))
end

local function try_place(idx)
  local data = get_bucket(idx)
  if not data or #data < b then
    data = string.rep('\0', b)
  end
  for i = 1, b do
    local c = string.byte(data, i) or 0
    if c == fp then
      return 2
    end
    if c == 0 then
      set_slot(idx, i, fp)
      return 1
    end
  end
  return 0
end

ensure()
local r1 = try_place(i1)
if r1 == 2 then
  return 1
end
if r1 == 1 then
  redis.call('INCR', cntkey)
  return 1
end
local r2 = try_place(i2)
if r2 == 2 then
  return 1
end
if r2 == 1 then
  redis.call('INCR', cntkey)
  return 1
end

-- cuckoo kick
local idx = i1
local cur = fp
math.randomseed(tonumber(string.sub(tostring(fp + i1 + i2), -6)) or 1)
for _ = 1, maxk do
  local data = get_bucket(idx)
  if not data or #data < b then
    data = string.rep('\0', b)
  end
  local slot = math.random(1, b)
  local victim = string.byte(data, slot) or 0
  set_slot(idx, slot, cur)
  if victim == 0 then
    redis.call('INCR', cntkey)
    return 1
  end
  cur = victim
  idx = alt_index(idx, victim, m)
end
return 0
`)

// language=Lua
var cuckooDeleteScript = redis.NewScript(`
local key = KEYS[1]
local cntkey = KEYS[2]
local m = tonumber(ARGV[1])
local b = tonumber(ARGV[2])
local i1 = tonumber(ARGV[3])
local i2 = tonumber(ARGV[4])
local fp = tonumber(ARGV[5])

if redis.call('EXISTS', key) == 0 then
  return 0
end

local function clear_in(idx)
  local start = idx * b
  local data = redis.call('GETRANGE', key, start, start + b - 1)
  if not data or #data < b then
    return false
  end
  for i = 1, b do
    if (string.byte(data, i) or 0) == fp then
      local off = start + (i - 1)
      redis.call('SETRANGE', key, off, string.char(0))
      local n = tonumber(redis.call('GET', cntkey) or '0') or 0
      if n > 0 then
        redis.call('DECR', cntkey)
      end
      return true
    end
  end
  return false
end

if clear_in(i1) or clear_in(i2) then
  return 1
end
return 0
`)

// language=Lua
var cuckooLookupScript = redis.NewScript(`
local key = KEYS[1]
local m = tonumber(ARGV[1])
local b = tonumber(ARGV[2])
local i1 = tonumber(ARGV[3])
local i2 = tonumber(ARGV[4])
local fp = tonumber(ARGV[5])

if redis.call('EXISTS', key) == 0 then
  return 2
end

local function has_fp(idx)
  local start = idx * b
  local data = redis.call('GETRANGE', key, start, start + b - 1)
  if not data then
    return false
  end
  for i = 1, #data do
    if (string.byte(data, i) or 0) == fp then
      return true
    end
  end
  return false
end

if has_fp(i1) or has_fp(i2) then
  return 1
end
return 0
`)
