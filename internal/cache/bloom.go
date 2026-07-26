// 本文件：知文详情「存在性过滤器」客户端。
//
// # 第三方实现（不自研过滤器算法）
//
// 过滤算法与数据结构由 Redis 官方模块 **RedisBloom** 提供（Cuckoo Filter，命令前缀 CF.*），
// 随 redis-stack-server 镜像部署。本文件只做业务侧薄封装：
//
//   - 映射配置 → CF.RESERVE 参数
//   - 调用 CF.ADD / CF.DEL / CF.EXISTS / CF.INFO
//   - 冷启动 / 模块缺失 / Redis 故障时 **fail-open**（宁可多打缓存，不可误 404）
//
// 业务层类型名仍叫 RedisBloom / bloom_* 配置键，仅为兼容历史命名；
// 面试与文档应表述为：「存在性过滤用第三方 RedisBloom（CF.*），业务只写适配层」。
//
// # 为何用第三方模块而不是进程内自研 / 纯 Go 库
//
//   - 多实例共享：状态在 Redis，各 API 实例一致
//   - 可删除：软删走 CF.DEL（经典 Bloom 不支持可靠删除）
//   - 命令原子、运维成熟；本仓库不维护哈希/踢出/扩容算法
//
// # 与空值缓存叠加
//
//   - CF：拦「一定从未出现过的扫号 ID」（MightContain=false → 404）
//   - NULL：兜「查过确认不存在 / CF 假阳性 / 未预热 / 模块故障」
//
// 语义：
//   - MightContain=false → 一定不存在（仅过滤器已预热时）
//   - MightContain=true  → 可能存在，必须继续 L1/L2/DB
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

// RedisBloom 模块 CF.* 命令名（第三方模块协议，非本仓库实现）。
const (
	cfCmdReserve = "CF.RESERVE"
	cfCmdAdd     = "CF.ADD"
	cfCmdDel     = "CF.DEL"
	cfCmdExists  = "CF.EXISTS"
	cfCmdInfo    = "CF.INFO"
)

// BloomConfig 控制第三方 RedisBloom Cuckoo（CF）的容量与行为。
// 字段名保留 Bloom* 以兼容既有 YAML 与调用方。
type BloomConfig struct {
	// Enabled 为 false 时不创建客户端，详情读路径跳过存在性过滤。
	Enabled bool
	// ExpectedItems 预估元素量 → CF.RESERVE capacity。
	ExpectedItems uint64
	// FalsePositiveRate 目标误判率；仅用于选择 BUCKETSIZE 启发式（模块侧近似）。
	FalsePositiveRate float64
	// Key Redis 上的 CF 键；空则默认 "cf:knowpost:ids"。
	Key string
	// HashCount 历史字段（经典 Bloom）；第三方 CF 忽略。
	HashCount int
	// BucketSize → CF.RESERVE BUCKETSIZE；0 表示按 FPR 自动选择。
	BucketSize int
	// MaxKicks → CF.RESERVE MAXITERATIONS；0 表示默认 20。
	MaxKicks int
	// Expansion → CF.RESERVE EXPANSION；0 表示默认 1。
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

// RedisBloom 是第三方 RedisBloom 模块（CF.*）的业务侧客户端。
//
// 不实现 Cuckoo/Bloom 算法本身；只封装 go-redis 对 CF 命令的调用与降级策略。
// 类型名保留 RedisBloom，避免 knowpost 等调用方大面积改名。
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
	moduleOK   bool // 已确认 CF 命令可用且键已就绪
	moduleFail bool // 明确无模块（unknown command），后续 fail-open
}

// NewRedisBloom 创建第三方 CF 客户端。cfg.Enabled=false 或 rdb=nil 时返回 nil。
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
		// RedisBloom 默认 bucket size=2；更严 FPR 时用更大 bucket
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

// Add 调用第三方 CF.ADD。失败只记日志，不阻断业务写路径。
func (b *RedisBloom) Add(ctx context.Context, member string) {
	if b == nil || member == "" {
		return
	}
	if !b.ensureReady(ctx) {
		return
	}
	if err := b.cfDo(ctx, cfCmdAdd, b.key, member).Err(); err != nil {
		b.logger.Warn("redisbloom CF.ADD failed", zap.String("member", member), zap.Error(err))
		b.markModuleError(err)
	}
}

// AddUint64 将 uint64 ID 以十进制字符串加入过滤器。
func (b *RedisBloom) AddUint64(ctx context.Context, id uint64) {
	if b == nil {
		return
	}
	b.Add(ctx, strconv.FormatUint(id, 10))
}

// Delete 调用第三方 CF.DEL。找不到则 no-op；失败不阻断软删主路径。
func (b *RedisBloom) Delete(ctx context.Context, member string) {
	if b == nil || member == "" {
		return
	}
	if !b.ensureReady(ctx) {
		return
	}
	if err := b.cfDo(ctx, cfCmdDel, b.key, member).Err(); err != nil {
		if isNotExistErr(err) {
			return
		}
		b.logger.Warn("redisbloom CF.DEL failed", zap.String("member", member), zap.Error(err))
		b.markModuleError(err)
	}
}

// DeleteUint64 删除 uint64 ID。
func (b *RedisBloom) DeleteUint64(ctx context.Context, id uint64) {
	if b == nil {
		return
	}
	b.Delete(ctx, strconv.FormatUint(id, 10))
}

// IsWarm 判断第三方 CF 键是否已存在（已 RESERVE 或至少写入过）。
// 冷启动键不存在时读路径 fail-open，避免误拦全站。
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

// MightContain 调用第三方 CF.EXISTS。
//
// 返回：
//   - false, nil：一定不存在（仅过滤器已预热）
//   - true, nil：可能存在，或未预热 / 模块故障 fail-open
//   - true, err：Redis 调用失败时 fail-open 并带上错误供观测
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
	raw, err := b.cfDo(ctx, cfCmdExists, b.key, member).Result()
	if err != nil {
		b.logger.Warn("redisbloom CF.EXISTS failed, fail-open", zap.String("member", member), zap.Error(err))
		b.markModuleError(err)
		return true, err
	}
	return redisTruthy(raw), nil
}

// MightContainUint64 查询 uint64 ID 是否可能存在。
func (b *RedisBloom) MightContainUint64(ctx context.Context, id uint64) (bool, error) {
	if b == nil {
		return true, nil
	}
	return b.MightContain(ctx, strconv.FormatUint(id, 10))
}

// Stats 返回 key、capacity、bucketSize（兼容旧签名）。
func (b *RedisBloom) Stats() (key string, m uint64, k int) {
	if b == nil {
		return "", 0, 0
	}
	return b.key, uint64(b.capacity), b.bucketSize
}

// Info 调用第三方 CF.INFO（运维/测试）。模块不可用时返回 error。
func (b *RedisBloom) Info(ctx context.Context) (map[string]int64, error) {
	if b == nil {
		return nil, errors.New("nil redisbloom client")
	}
	if !b.ensureReady(ctx) {
		return nil, errors.New("redisbloom module unavailable")
	}
	val, err := b.cfDo(ctx, cfCmdInfo, b.key).Result()
	if err != nil {
		return nil, err
	}
	return parseCFInfo(val), nil
}

// ensureReady 保证第三方模块可用，并幂等 CF.RESERVE。
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

	n, err := b.rdb.Exists(ctx, b.key).Result()
	if err != nil {
		b.logger.Warn("redisbloom key exists check failed", zap.Error(err))
		b.markModuleError(err)
		return false
	}
	if n > 0 {
		b.ensured = true
		b.moduleOK = true
		return true
	}

	// CF.RESERVE key capacity [BUCKETSIZE bs] [MAXITERATIONS mi] [EXPANSION exp]
	err = b.cfDo(ctx, cfCmdReserve, b.key, b.capacity,
		"BUCKETSIZE", b.bucketSize,
		"MAXITERATIONS", b.maxIter,
		"EXPANSION", b.expansion,
	).Err()
	if err != nil {
		if isAlreadyExistsErr(err) {
			b.ensured = true
			b.moduleOK = true
			return true
		}
		b.logger.Warn("redisbloom CF.RESERVE failed", zap.Error(err))
		b.markModuleError(err)
		return false
	}
	b.ensured = true
	b.moduleOK = true
	return true
}

// cfDo 统一走 go-redis 自定义命令，调用第三方 RedisBloom 模块。
func (b *RedisBloom) cfDo(ctx context.Context, args ...interface{}) *redis.Cmd {
	return b.rdb.Do(ctx, args...)
}

func (b *RedisBloom) markModuleError(err error) {
	if err == nil {
		return
	}
	if isUnknownCommandErr(err) {
		b.moduleFail = true
		b.logger.Error("RedisBloom third-party module missing: CF.* unavailable; filter fail-open. Use redis/redis-stack-server.")
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

// redisTruthy 规范化 CF.EXISTS 等返回值（RESP2 0/1 或 RESP3 bool）。
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

// parseCFInfo 解析 CF.INFO 返回结构（运维用，非算法实现）。
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
