// Package cache 的存在性过滤器：基于 RedisBloom 模块的共享 Cuckoo Filter（CF.*）。
//
// 设计目标：
//   - 与详情「空值缓存」叠加：过滤器拦截「一定不存在」的扫号请求；
//     误判为可能存在时，仍走 L1/L2/读穿锁，并由 NULL 缓存兜底。
//   - 多实例共享：数据在 Redis 模块侧，官方 CF 命令原子、功能完整。
//   - 依赖 RedisBloom 模块（docker 使用 redis-stack-server），非进程内库。
//   - 支持可靠删除：软删后 CF.DEL，减少对 NULL/DB 的依赖。
//
// 语义（Cuckoo Filter / CF.*）：
//   - MightContain=false → 一定不存在（可直接 404）
//   - MightContain=true  → 可能存在（必须继续查缓存/DB）
//   - Delete → CF.DEL
//
// 主要命令：
//   CF.RESERVE / CF.ADD / CF.DEL / CF.EXISTS / CF.INFO
//   （模块还提供 CF.INSERT、CF.COUNT、CF.SCANDUMP/LOADCHUNK 等，按需扩展）
package cache

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync"

	"github.com/redis/go-redis/v9"
	"go.uber.org/zap"
)

// BloomConfig 控制 Redis Cuckoo（RedisBloom CF）的容量与行为。
// 字段名保留 Bloom* 以兼容既有配置与调用方。
type BloomConfig struct {
	// Enabled 为 false 时所有操作 no-op，保持与旧行为兼容。
	Enabled bool
	// ExpectedItems 预估元素量，映射为 CF.RESERVE capacity。
	ExpectedItems uint64
	// FalsePositiveRate 目标误判率；用于选择 bucket size（模块侧近似控制）。
	FalsePositiveRate float64
	// Key Redis CF 键；空则使用默认 "cf:knowpost:ids"。
	Key string
	// HashCount 保留字段（经典 Bloom）；CF 忽略。
	HashCount int
	// BucketSize 映射 CF.RESERVE BUCKETSIZE；0 表示按 FPR 自动选择（默认 2）。
	BucketSize int
	// MaxKicks 映射 CF.RESERVE MAXITERATIONS；0 表示默认 20。
	MaxKicks int
	// Expansion 映射 CF.RESERVE EXPANSION；0 表示默认 1。
	Expansion int
}

// DefaultBloomConfig 返回知文详情场景的合理默认值。
func DefaultBloomConfig() BloomConfig {
	return BloomConfig{
		Enabled:           true,
		ExpectedItems:     1_000_000,
		FalsePositiveRate: 0.01,
		Key:               "cf:knowpost:ids",
		BucketSize:        2,
		MaxKicks:          20,
		Expansion:         1,
	}
}

// RedisBloom 基于 RedisBloom 模块 CF.* 的共享 Cuckoo 过滤器。
// 类型名保留 RedisBloom，避免业务层大面积改名。
type RedisBloom struct {
	rdb        *redis.Client
	key        string
	capacity   int64
	bucketSize int
	maxIter    int
	expansion  int
	logger     *zap.Logger

	ensureMu   sync.Mutex
	ensured    bool
	moduleOK   bool // 探测到 CF 命令可用
	moduleFail bool // 明确不可用（unknown command），后续 fail-open
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
		cfg.Key = "cf:knowpost:ids"
	}
	bs := cfg.BucketSize
	if bs <= 0 {
		// 更严 FPR 用更大 bucket（RedisBloom 默认 2）
		if cfg.FalsePositiveRate <= 0.001 {
			bs = 4
		} else {
			bs = 2
		}
	}
	if bs > 8 {
		bs = 8
	}
	maxIter := cfg.MaxKicks
	if maxIter <= 0 {
		maxIter = 20
	}
	exp := cfg.Expansion
	if exp <= 0 {
		exp = 1
	}

	return &RedisBloom{
		rdb:        rdb,
		key:        cfg.Key,
		capacity:   int64(cfg.ExpectedItems),
		bucketSize: bs,
		maxIter:    maxIter,
		expansion:  exp,
		logger:     logger,
	}
}

// Add 将成员加入过滤器（CF.ADD）。失败只记日志，不影响主写路径。
func (b *RedisBloom) Add(ctx context.Context, member string) {
	if b == nil || member == "" {
		return
	}
	if !b.ensureReady(ctx) {
		return
	}
	if err := b.rdb.Do(ctx, "CF.ADD", b.key, member).Err(); err != nil {
		b.logger.Warn("cf add failed", zap.String("member", member), zap.Error(err))
		b.markModuleError(err)
	}
}

// AddUint64 便捷方法：按十进制字符串加入。
func (b *RedisBloom) AddUint64(ctx context.Context, id uint64) {
	if b == nil {
		return
	}
	b.Add(ctx, strconv.FormatUint(id, 10))
}

// Delete 从过滤器删除成员（CF.DEL）。找不到则 no-op。
// 失败只记日志，不阻断软删主路径；NULL 缓存仍作兜底。
func (b *RedisBloom) Delete(ctx context.Context, member string) {
	if b == nil || member == "" {
		return
	}
	if !b.ensureReady(ctx) {
		return
	}
	// CF.DEL 返回 0/1；键不存在时可能报错，视为 no-op
	if err := b.rdb.Do(ctx, "CF.DEL", b.key, member).Err(); err != nil {
		if isNotExistErr(err) {
			return
		}
		b.logger.Warn("cf delete failed", zap.String("member", member), zap.Error(err))
		b.markModuleError(err)
	}
}

// DeleteUint64 便捷方法。
func (b *RedisBloom) DeleteUint64(ctx context.Context, id uint64) {
	if b == nil {
		return
	}
	b.Delete(ctx, strconv.FormatUint(id, 10))
}

// IsWarm 判断 CF 键是否已存在（已 RESERVE 或至少 Add 过）。
// 冷启动键不存在时 fail-open，避免误拦全站。
func (b *RedisBloom) IsWarm(ctx context.Context) bool {
	if b == nil {
		return false
	}
	if b.moduleFail {
		return false
	}
	n, err := b.rdb.Exists(ctx, b.key).Result()
	if err != nil {
		return false
	}
	return n > 0
}

// MightContain 判断成员是否可能存在（CF.EXISTS）。
//
// 返回：
//   - false, nil：一定不存在（仅当过滤器已预热）
//   - true, nil：可能存在（含误判），或过滤器未预热 / 故障 fail-open
//   - true, err：Redis/模块故障时 fail-open
func (b *RedisBloom) MightContain(ctx context.Context, member string) (bool, error) {
	if b == nil || member == "" {
		return true, nil
	}
	if b.moduleFail {
		return true, nil
	}
	if !b.IsWarm(ctx) {
		return true, nil
	}
	// CF.EXISTS 在 RESP2 返回 0/1，RESP3 可能返回 bool
	raw, err := b.rdb.Do(ctx, "CF.EXISTS", b.key, member).Result()
	if err != nil {
		b.logger.Warn("cf exists failed, fail-open", zap.String("member", member), zap.Error(err))
		b.markModuleError(err)
		return true, err
	}
	return redisTruthy(raw), nil
}

// MightContainUint64 便捷方法。
func (b *RedisBloom) MightContainUint64(ctx context.Context, id uint64) (bool, error) {
	if b == nil {
		return true, nil
	}
	return b.MightContain(ctx, strconv.FormatUint(id, 10))
}

// Stats 返回 key、capacity、bucketSize（兼容旧签名第三项）。
func (b *RedisBloom) Stats() (key string, m uint64, k int) {
	if b == nil {
		return "", 0, 0
	}
	return b.key, uint64(b.capacity), b.bucketSize
}

// Info 调用 CF.INFO，返回解析后的字段（运维/测试用）。模块不可用时返回 error。
func (b *RedisBloom) Info(ctx context.Context) (map[string]int64, error) {
	if b == nil {
		return nil, errors.New("nil filter")
	}
	if !b.ensureReady(ctx) {
		return nil, errors.New("redisbloom module unavailable")
	}
	val, err := b.rdb.Do(ctx, "CF.INFO", b.key).Result()
	if err != nil {
		return nil, err
	}
	return parseCFInfo(val), nil
}

// ensureReady 保证模块可用并完成 CF.RESERVE（幂等）。
func (b *RedisBloom) ensureReady(ctx context.Context) bool {
	if b == nil || b.moduleFail {
		return false
	}
	b.ensureMu.Lock()
	defer b.ensureMu.Unlock()
	if b.ensured && b.moduleOK {
		return true
	}
	if b.moduleFail {
		return false
	}

	// 已有键则无需 RESERVE
	n, err := b.rdb.Exists(ctx, b.key).Result()
	if err != nil {
		b.logger.Warn("cf exists check failed", zap.Error(err))
		b.markModuleError(err)
		return false
	}
	if n > 0 {
		b.ensured = true
		b.moduleOK = true
		return true
	}

	// CF.RESERVE key capacity [BUCKETSIZE bs] [MAXITERATIONS mi] [EXPANSION exp]
	err = b.rdb.Do(ctx, "CF.RESERVE", b.key, b.capacity,
		"BUCKETSIZE", b.bucketSize,
		"MAXITERATIONS", b.maxIter,
		"EXPANSION", b.expansion,
	).Err()
	if err != nil {
		// 并发 RESERVE：另一实例已创建
		if isAlreadyExistsErr(err) {
			b.ensured = true
			b.moduleOK = true
			return true
		}
		b.logger.Warn("cf reserve failed", zap.Error(err))
		b.markModuleError(err)
		return false
	}
	b.ensured = true
	b.moduleOK = true
	return true
}

func (b *RedisBloom) markModuleError(err error) {
	if err == nil {
		return
	}
	if isUnknownCommandErr(err) {
		b.moduleFail = true
		b.logger.Error("RedisBloom module missing: CF.* unavailable; filter fail-open. Use redis-stack-server image.")
	}
}

func isUnknownCommandErr(err error) bool {
	if err == nil {
		return false
	}
	s := strings.ToLower(err.Error())
	return strings.Contains(s, "unknown command") ||
		strings.Contains(s, "err unknown command")
}

func isAlreadyExistsErr(err error) bool {
	if err == nil {
		return false
	}
	s := strings.ToLower(err.Error())
	// CF.RESERVE on existing key: "ERR item exists" / "Filter already exists" etc.
	return strings.Contains(s, "item exists") ||
		strings.Contains(s, "already exists") ||
		strings.Contains(s, "busykey")
}

func isNotExistErr(err error) bool {
	if err == nil {
		return false
	}
	s := strings.ToLower(err.Error())
	return strings.Contains(s, "not found") ||
		strings.Contains(s, "does not exist") ||
		errors.Is(err, redis.Nil)
}

// redisTruthy 将 CF.EXISTS / CF.ADD 等返回值规范为 bool。
func redisTruthy(v interface{}) bool {
	switch x := v.(type) {
	case bool:
		return x
	case int64:
		return x != 0
	case int:
		return x != 0
	case string:
		return x == "1" || strings.EqualFold(x, "true")
	case []byte:
		s := string(x)
		return s == "1" || strings.EqualFold(s, "true")
	default:
		return toInt64(v) != 0
	}
}

// parseCFInfo 将 CF.INFO 返回的 [k1 v1 k2 v2 ...] 或 map 解析为 map[string]int64。
func parseCFInfo(val interface{}) map[string]int64 {
	out := make(map[string]int64)
	switch arr := val.(type) {
	case []interface{}:
		for i := 0; i+1 < len(arr); i += 2 {
			k := fmt.Sprint(arr[i])
			out[k] = toInt64(arr[i+1])
		}
	case map[interface{}]interface{}:
		for k, v := range arr {
			out[fmt.Sprint(k)] = toInt64(v)
		}
	case map[string]interface{}:
		for k, v := range arr {
			out[k] = toInt64(v)
		}
	}
	return out
}

func toInt64(v interface{}) int64 {
	switch x := v.(type) {
	case int64:
		return x
	case int:
		return int64(x)
	case string:
		n, _ := strconv.ParseInt(x, 10, 64)
		return n
	case []byte:
		n, _ := strconv.ParseInt(string(x), 10, 64)
		return n
	default:
		n, _ := strconv.ParseInt(fmt.Sprint(x), 10, 64)
		return n
	}
}
