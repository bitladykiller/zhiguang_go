package cache

import (
	"context"
	"strconv"

	"github.com/coocood/freecache"
	"github.com/redis/go-redis/v9"
)

// Versions 统一「版本号编进缓存键」失效模式的**读取端**。
//
// # 模式回顾
//
// 本项目的多级缓存用版本号做失效：缓存键形如 "{业务键}:ver{N}"，
// 写操作递增 N，旧版本键在所有实例上自然失效（键不匹配即不命中）。
// 该模式此前有三个变体各自手搓读取逻辑：
//
//	知文详情：knowpost:ver:{id}       （per-post，带进程内短缓存）
//	公共 Feed：feed:public:version    （全局，每次读 Redis）
//	我的 Feed：feed:mine:version:{id} （per-user，每次读 Redis）
//
// 三份代码语义相同、细节漂移——例如只有详情做了进程内短缓存，
// 于是公共 Feed 的 L1 命中仍要为版本号付一次 Redis 往返（与详情当年同款缺陷）。
// 收敛为一个组件后，语义一处定义，三处复用。
//
// # 本地短缓存的取舍
//
// 版本号只在写操作时变化，用极短 TTL（秒级）做进程内缓存是安全的：
// 代价是「其他实例写入后，本实例最多延迟 TTL 秒才切到新键」，
// 而这个窗口远小于 L1 载荷缓存自身的 TTL——版本号的即时性本来就不构成端到端强一致。
// 本实例自己的写操作通过 Drop 立即失效本地缓存，保证自读自写一致。
//
// 并发安全：freecache 与 go-redis 客户端本身并发安全，本类型无自有可变状态。
type Versions struct {
	// Redis 是版本号的权威存储。nil 时 Get 恒返回 Default（供零依赖单测）。
	Redis *redis.Client
	// Local 是进程内短缓存的载体；nil 表示关闭本地缓存。
	Local *freecache.Cache
	// LocalTTLSeconds 是本地缓存秒数；<=0 表示关闭本地缓存。
	LocalTTLSeconds int
	// LocalPrefix 隔离本地缓存的键空间（Local 可能是全局共享的 freecache 实例）。
	LocalPrefix string
	// Default 是 Redis 键缺失或值非法时返回的版本号。
	Default int64
}

// Get 返回 redisKey 对应的当前版本号。
func (v *Versions) Get(ctx context.Context, redisKey string) int64 {
	if v == nil {
		return 0
	}
	useLocal := v.Local != nil && v.LocalTTLSeconds > 0

	if useLocal {
		if raw, err := v.Local.Get(v.localKey(redisKey)); err == nil {
			if n, parseErr := strconv.ParseInt(string(raw), 10, 64); parseErr == nil {
				return n
			}
		}
	}

	version := v.Default
	if v.Redis != nil {
		if n, err := v.Redis.Get(ctx, redisKey).Int64(); err == nil && n > 0 {
			version = n
		}
	}

	if useLocal {
		// 写入失败无需处理：下次读取回落 Redis，语义不变。
		_ = v.Local.Set(v.localKey(redisKey), []byte(strconv.FormatInt(version, 10)), v.LocalTTLSeconds)
	}
	return version
}

// Drop 作废 redisKey 的本地缓存。写侧递增版本号后必须调用，保证本实例自读自写一致。
func (v *Versions) Drop(redisKey string) {
	if v == nil || v.Local == nil {
		return
	}
	v.Local.Del(v.localKey(redisKey))
}

func (v *Versions) localKey(redisKey string) []byte {
	return []byte(v.LocalPrefix + redisKey)
}
