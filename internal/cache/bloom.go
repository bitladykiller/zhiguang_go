// Package cache 的 Bloom 过滤器：基于 Redis 位图的共享存在性前置判断。
//
// 设计目标：
//   - 与详情「空值缓存」叠加：Bloom 拦截「一定不存在」的扫号请求；
//     误判为可能存在时，仍走 L1/L2/读穿锁，并由 NULL 缓存兜底。
//   - 多实例共享：位数组存在 Redis，避免进程内 Bloom 不一致。
//   - 无第三方依赖：用双重哈希 + SETBIT/GETBIT，兼容标准 Redis。
//
// 语义（经典 Bloom）：
//   - MightContain=false → 一定不存在（可直接 404）
//   - MightContain=true  → 可能存在（必须继续查缓存/DB）
//   - 不支持可靠删除：软删后位仍置 1，多打一次缓存/DB，由 NULL 缓存吸收。
package cache

import (
	"context"
	"fmt"
	"hash/fnv"
	"math"

	"github.com/redis/go-redis/v9"
	"go.uber.org/zap"
)

// BloomConfig 控制 Redis Bloom 的容量与误判率。
type BloomConfig struct {
	// Enabled 为 false 时所有操作 no-op，保持与旧行为兼容。
	Enabled bool
	// ExpectedItems 预估元素量（用于计算位图长度 m）。
	ExpectedItems uint64
	// FalsePositiveRate 目标误判率，例如 0.01 表示 1%。
	FalsePositiveRate float64
	// Key Redis 位图键；空则使用默认 "bloom:knowpost:ids"。
	Key string
	// HashCount 哈希函数个数 k；0 表示按公式自动计算。
	HashCount int
}

// DefaultBloomConfig 返回知文详情场景的合理默认值。
func DefaultBloomConfig() BloomConfig {
	return BloomConfig{
		Enabled:           true,
		ExpectedItems:     1_000_000,
		FalsePositiveRate: 0.01,
		Key:               "bloom:knowpost:ids",
		HashCount:         0,
	}
}

// RedisBloom 使用 Redis 字符串位图实现的共享 Bloom 过滤器。
type RedisBloom struct {
	rdb    *redis.Client
	key    string
	m      uint64 // 位图长度（bit 数）
	k      int    // 哈希次数
	logger *zap.Logger
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
		cfg.Key = "bloom:knowpost:ids"
	}

	// m = -n*ln(p) / (ln2)^2
	m := uint64(math.Ceil(-float64(cfg.ExpectedItems) * math.Log(cfg.FalsePositiveRate) / (math.Ln2 * math.Ln2)))
	if m < 64 {
		m = 64
	}
	// k = (m/n)*ln2
	k := cfg.HashCount
	if k <= 0 {
		k = int(math.Round(float64(m) / float64(cfg.ExpectedItems) * math.Ln2))
	}
	if k < 1 {
		k = 1
	}
	if k > 30 {
		k = 30
	}

	return &RedisBloom{
		rdb:    rdb,
		key:    cfg.Key,
		m:      m,
		k:      k,
		logger: logger,
	}
}

// Add 将成员加入过滤器。失败只记日志，不影响主写路径。
func (b *RedisBloom) Add(ctx context.Context, member string) {
	if b == nil || member == "" {
		return
	}
	positions := b.positions(member)
	pipe := b.rdb.Pipeline()
	for _, pos := range positions {
		pipe.SetBit(ctx, b.key, int64(pos), 1)
	}
	if _, err := pipe.Exec(ctx); err != nil {
		b.logger.Warn("bloom add failed", zap.String("member", member), zap.Error(err))
	}
}

// AddUint64 便捷方法：按十进制字符串加入。
func (b *RedisBloom) AddUint64(ctx context.Context, id uint64) {
	if b == nil {
		return
	}
	b.Add(ctx, fmt.Sprintf("%d", id))
}

// IsWarm 判断过滤器是否已有数据。空过滤器必须 fail-open，否则会拦下所有合法 ID。
func (b *RedisBloom) IsWarm(ctx context.Context) bool {
	if b == nil {
		return false
	}
	n, err := b.rdb.BitCount(ctx, b.key, &redis.BitCount{Start: 0, End: -1}).Result()
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
	// 空位图：全部返回「可能存在」，避免冷启动把全站详情打成 404。
	if !b.IsWarm(ctx) {
		return true, nil
	}
	positions := b.positions(member)
	pipe := b.rdb.Pipeline()
	cmds := make([]*redis.IntCmd, len(positions))
	for i, pos := range positions {
		cmds[i] = pipe.GetBit(ctx, b.key, int64(pos))
	}
	if _, err := pipe.Exec(ctx); err != nil && err != redis.Nil {
		// fail-open：Bloom 不可用时退回原路径（L1/L2/NULL/DB）
		b.logger.Warn("bloom might_contain failed, fail-open", zap.String("member", member), zap.Error(err))
		return true, err
	}
	for _, cmd := range cmds {
		bit, err := cmd.Result()
		if err != nil {
			return true, err
		}
		if bit == 0 {
			return false, nil
		}
	}
	return true, nil
}

// MightContainUint64 便捷方法。
func (b *RedisBloom) MightContainUint64(ctx context.Context, id uint64) (bool, error) {
	if b == nil {
		return true, nil
	}
	return b.MightContain(ctx, fmt.Sprintf("%d", id))
}

// Stats 返回位图参数，便于运维与单测断言。
func (b *RedisBloom) Stats() (key string, m uint64, k int) {
	if b == nil {
		return "", 0, 0
	}
	return b.key, b.m, b.k
}

// positions 双重哈希：pos_i = (h1 + i*h2) % m
func (b *RedisBloom) positions(member string) []uint64 {
	h1, h2 := bloomHashes(member)
	if h2%b.m == 0 {
		h2 = 1
	}
	out := make([]uint64, b.k)
	for i := 0; i < b.k; i++ {
		out[i] = (h1 + uint64(i)*h2) % b.m
	}
	return out
}

func bloomHashes(member string) (uint64, uint64) {
	a := fnv.New64a()
	_, _ = a.Write([]byte(member))
	h1 := a.Sum64()

	b := fnv.New64()
	_, _ = b.Write([]byte(member))
	// 混入长度避免与 h1 完全相关
	_, _ = b.Write([]byte{byte(len(member)), byte(len(member) >> 8)})
	h2 := b.Sum64()
	return h1, h2
}
