package cache

import (
	"context"
	"errors"
	"time"

	"github.com/redis/go-redis/v9"
	"go.uber.org/zap"

	"github.com/zhiguang/app/pkg/cacheutil"
	"github.com/zhiguang/app/pkg/redislock"
)

// ErrNullCached 表示命中了空值缓存：此前已确认该键对应的数据不存在。
// 调用方通常将其映射为业务上的「未找到」错误。
var ErrNullCached = errors.New("cache: null sentinel cached")

// HitLevel 标识一次 Tiered.Get 的数据来源。
type HitLevel int

const (
	// HitL1 命中进程内缓存（无任何网络往返）。
	HitL1 HitLevel = iota
	// HitL2 命中 Redis（含锁内 double-check 命中）。
	HitL2
	// HitLoad 两级均未命中，由 Loader 回源加载。
	HitLoad
)

// ByteCache 是 L1 进程内缓存所需的最小能力，由 PrefixCache 实现。
type ByteCache interface {
	Get(key []byte) ([]byte, error)
	Set(key, value []byte, expireSeconds int) error
}

// Loader 回源加载权威数据。
//
// 返回值约定：
//   - found=false 且 err=nil：数据确认不存在（触发空值缓存，若已配置 NullSentinel）。
//   - err!=nil：加载失败，原样透传给调用方，不写任何缓存。
type Loader[T any] func(ctx context.Context) (value T, found bool, err error)

// Tiered 是「L1 进程内 → L2 Redis → 分布式锁回源」的通用三级读穿缓存。
//
// # WHY 需要这个抽象
//
// 在引入本组件前，同一套「查 L1 → 查 L2 → 抢锁 double-check → 回源 → 回填」的舞步
// 在知文详情、我的 Feed 等处各手写一遍，细节各有微差——
// 历史上的公共 Feed 跨用户串号事故，根因正是其中一份在叠加用户态**之后**才写缓存，
// 而其他份在之前。复制粘贴的模式没有结构可言，正确性只能靠逐处人工核对。
//
// # 结构性防串号
//
// 本组件的关键不变量：**缓存中只会出现 Loader 的原始产物或 L2 的原始字节**。
// Get 在返回之前就已完成全部缓存写入，用户维度的叠加（liked/faved 等）
// 发生在调用方拿到返回值之后——即便调用方随手修改返回值，也污染不了缓存。
// 「共享视图进缓存、用户态读后叠加」由此从纪律变成结构。
//
// # 空值缓存
//
// 配置 NullSentinel 后：Loader 报告 found=false 时向 L2 写入哨兵串（NullTTL 生存期），
// 后续命中哨兵直接返回 ErrNullCached，不再打扰 Loader（穿透防护的一层）。
//
// # 各字段均为纯配置，实例可按请求临时构造（无自有状态，构造成本为零分配级）。
type Tiered[T any] struct {
	L1     ByteCache     // 进程内缓存；nil 表示无 L1
	Redis  *redis.Client // L2；nil 表示无 L2（直接走锁回源）
	Logger *zap.Logger   // 可为 nil

	Encode func(T) ([]byte, error)
	Decode func([]byte) (T, error)

	L1TTLSeconds int
	L2TTL        func() time.Duration // 每次写 L2 时调用（便于带 jitter）

	// NullSentinel 非空则启用空值缓存；NullTTL 为哨兵的生存期。
	NullSentinel string
	NullTTL      func() time.Duration

	// PreLoad 在两级缓存均未命中后、抢锁之前调用（如 Bloom 存在性预判）。
	// 返回非 nil 错误则中止流程并原样返回该错误。
	PreLoad func(ctx context.Context) error

	// 分布式锁参数；LockKey 为空则不加锁直接回源（低竞争场景）。
	LockKey     string
	LockOptions redislock.Options
	LockRetry   time.Duration
}

// Get 按 L1 → L2 → (PreLoad) → 锁内 double-check → Loader 的顺序取值。
//
// 返回的 HitLevel 供调用方做命中级差异化处理（如热点记录）；
// err 为 ErrNullCached 时表示空值命中。
func (t *Tiered[T]) Get(ctx context.Context, key string, load Loader[T]) (T, HitLevel, error) {
	var zero T

	// L1：解码成功才算命中；解码失败视为 miss（缓存内容可能来自旧布局）。
	if t.L1 != nil {
		if raw, err := t.L1.Get([]byte(key)); err == nil {
			if v, decErr := t.Decode(raw); decErr == nil {
				return v, HitL1, nil
			}
		}
	}

	// L2：空值哨兵优先判定；命中后先解码再回填 L1（不回填解不开的字节）。
	if v, ok, err := t.getL2(ctx, key); err != nil {
		return zero, HitL2, err
	} else if ok {
		return v, HitL2, nil
	}

	if t.PreLoad != nil {
		if err := t.PreLoad(ctx); err != nil {
			return zero, HitLoad, err
		}
	}

	if t.LockKey == "" {
		v, err := t.loadAndFill(ctx, key, load)
		return v, HitLoad, err
	}

	// 锁内先 double-check L2：拿到锁的可能是等待队列中的后来者，
	// 前一个持锁者大概率已回填缓存，无需再次回源。
	type hitResult struct {
		v   T
		lvl HitLevel
	}
	res, err := cacheutil.CacheReadThrough(ctx, t.Redis, t.LockKey, t.LockOptions, t.LockRetry,
		func(ctx context.Context) (hitResult, bool, error) {
			v, ok, err := t.getL2(ctx, key)
			if err != nil {
				return hitResult{}, false, err
			}
			return hitResult{v: v, lvl: HitL2}, ok, nil
		},
		func(ctx context.Context) (hitResult, error) {
			v, err := t.loadAndFill(ctx, key, load)
			return hitResult{v: v, lvl: HitLoad}, err
		},
	)
	if err != nil {
		return zero, res.lvl, err
	}
	return res.v, res.lvl, nil
}

// getL2 查询 L2；命中时回填 L1。ok=false 表示未命中。
// 空值哨兵命中返回 ErrNullCached。
func (t *Tiered[T]) getL2(ctx context.Context, key string) (T, bool, error) {
	var zero T
	if t.Redis == nil {
		return zero, false, nil
	}
	cached, err := t.Redis.Get(ctx, key).Result()
	if err != nil || cached == "" {
		return zero, false, nil // Redis 故障按 miss 处理：缓存层不阻断读路径
	}
	if t.NullSentinel != "" && cached == t.NullSentinel {
		return zero, false, ErrNullCached
	}
	v, decErr := t.Decode([]byte(cached))
	if decErr != nil {
		return zero, false, nil // 旧布局残留，按 miss 回源重建
	}
	t.fillL1(key, []byte(cached))
	return v, true, nil
}

// loadAndFill 执行回源，并在返回前完成全部缓存写入。
func (t *Tiered[T]) loadAndFill(ctx context.Context, key string, load Loader[T]) (T, error) {
	var zero T
	v, found, err := load(ctx)
	if err != nil {
		return zero, err
	}
	if !found {
		if t.NullSentinel != "" && t.Redis != nil && t.NullTTL != nil {
			if setErr := t.Redis.Set(ctx, key, t.NullSentinel, t.NullTTL()).Err(); setErr != nil {
				t.logWarn("tiered: set null sentinel failed", key, setErr)
			}
		}
		return zero, ErrNullCached
	}

	raw, encErr := t.Encode(v)
	if encErr != nil {
		// 编码失败只影响缓存回填，不影响本次结果。
		t.logWarn("tiered: encode for cache failed", key, encErr)
		return v, nil
	}
	if t.Redis != nil && t.L2TTL != nil {
		if setErr := t.Redis.Set(ctx, key, raw, t.L2TTL()).Err(); setErr != nil {
			t.logWarn("tiered: set L2 failed", key, setErr)
		}
	}
	t.fillL1(key, raw)
	return v, nil
}

func (t *Tiered[T]) fillL1(key string, raw []byte) {
	if t.L1 == nil {
		return
	}
	if err := t.L1.Set([]byte(key), raw, t.L1TTLSeconds); err != nil {
		// freecache 对超过容量 1/1024 的单条会拒写；静默会让该键永远不走 L1，须可见。
		t.logWarn("tiered: set L1 failed", key, err)
	}
}

func (t *Tiered[T]) logWarn(msg, key string, err error) {
	if t.Logger != nil {
		t.Logger.Warn(msg, zap.String("key", key), zap.Error(err))
	}
}
