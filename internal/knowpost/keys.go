package knowpost

import (
	"fmt"
	"strconv"

	"github.com/redis/go-redis/v9"

	"github.com/zhiguang/app/internal/cache"
	"github.com/zhiguang/app/pkg/config"
)

// 本文件集中 knowpost 模块的全部缓存键 schema。
//
// WHY 需要键注册表：
//
//	键名是模块的**公共契约**——写入方、读取方、失效方、Lua 脚本必须对同一格式达成一致。
//	此前这些键以 fmt.Sprintf 字面量散落在多个文件里（feed:item: 曾出现在 3 处），
//	改一处漏一处只能靠全文搜索保证。集中定义后，格式只有一个出处，
//	读者也能在一页之内看清本模块占用了哪些 Redis 键空间。
//
// 键空间总览（值类型 → 用途）：
//
//	knowpost:ver:{id}                    String  详情缓存版本号（写操作 INCR）
//	knowpost:detail:{id}:v{L}:ver{N}     String  详情页 JSON / "NULL" 空值哨兵
//	feed:public:version                  String  公共 Feed 全局版本号
//	feed:mine:version:{userID}           String  「我的已发布」版本号
//	feed:public:ids:{ver}:{size}:{page}  List    公共 Feed 某页的有序帖子 ID
//	feed:item:{id}                       String  Feed 条目碎片 JSON
//	lock:{cacheKey}                      String  回源分布式锁
//
// L1（freecache，进程内）前缀由 bootstrap 分配：d:（详情）、fp:（公共页）、fm:（我的页）；
// 版本号本地短缓存另用 dv: / fv: 前缀，与载荷键空间隔离。
const (
	detailVersionKeyPrefix = "knowpost:ver:"
	detailPageKeyFmt       = "knowpost:detail:%d:v%d:ver%d"

	publicFeedVersionKey  = "feed:public:version"
	mineFeedVersionKeyFmt = "feed:mine:version:%d"

	feedItemKeyPrefix = "feed:item:"

	lockKeyPrefix = "lock:"

	// 版本号本地短缓存的前缀（隔离共享 freecache 的键空间）。
	detailVersionLocalPrefix = "dv:"
	feedVersionLocalPrefix   = "fv:"
)

// detailVersionKey 返回某篇知文详情版本号的 Redis 键。
func detailVersionKey(id uint64) string {
	return detailVersionKeyPrefix + strconv.FormatUint(id, 10)
}

// detailPageKey 返回详情页缓存键；版本号编进键名实现「递增即失效」。
func detailPageKey(id uint64, version int64) string {
	return fmt.Sprintf(detailPageKeyFmt, id, detailLayoutVer, version)
}

// mineFeedVersionKey 返回某用户「我的已发布」版本号的 Redis 键。
func mineFeedVersionKey(userID uint64) string {
	return fmt.Sprintf(mineFeedVersionKeyFmt, userID)
}

// feedItemKey 返回单条 Feed 碎片的缓存键。
func feedItemKey(id string) string { return feedItemKeyPrefix + id }

// feedItemKeyU 是 feedItemKey 的 uint64 便捷形式。
func feedItemKeyU(id uint64) string { return feedItemKeyPrefix + strconv.FormatUint(id, 10) }

// publicFeedIDsKey 返回公共 Feed 某页 ID 列表的键（版本号维度实现整体失效）。
func publicFeedIDsKey(version int64, size, page int) string {
	return fmt.Sprintf("feed:public:ids:%d:%d:%d", version, size, page)
}

// publicFeedPageLocalKey 返回公共 Feed 整页在 L1 的键。
func publicFeedPageLocalKey(size, page int, version int64) string {
	return fmt.Sprintf("feed:public:%d:%d:v%d:%d", size, page, feedLayoutVer, version)
}

// mineFeedPageKey 返回「我的已发布」整页缓存键（L1 与 L2 共用）。
func mineFeedPageKey(userID uint64, size, page int, version int64) string {
	return fmt.Sprintf("feed:mine:%d:%d:%d:%d", userID, size, page, version)
}

// lockKeyFor 返回某缓存键对应的回源锁键。
func lockKeyFor(cacheKey string) string { return lockKeyPrefix + cacheKey }

// hotKeyID 返回知文在热点探测器中的统计键。
func hotKeyID(id uint64) string { return "knowpost:" + strconv.FormatUint(id, 10) }

// defaultVersionLocalTTLSeconds 是版本号进程内短缓存的默认存活秒数。
const defaultVersionLocalTTLSeconds = 2

// versionLocalTTL 解析版本号本地缓存 TTL 配置：0 用默认值，负数关闭。
func versionLocalTTL(cfg *config.KnowPostConfig) int {
	if cfg == nil {
		return defaultVersionLocalTTLSeconds
	}
	if v := cfg.DetailCache.VersionCacheTTLSeconds; v != 0 {
		if v < 0 {
			return 0
		}
		return v
	}
	return defaultVersionLocalTTLSeconds
}

// newDetailVersions 构造详情版本号读取器。
// l1 可为 nil（零依赖单测）：此时无本地缓存，每次直读 Redis。
func newDetailVersions(rdb *redis.Client, l1 *PrefixCache, cfg *config.KnowPostConfig) *cache.Versions {
	v := &cache.Versions{
		Redis:           rdb,
		LocalPrefix:     detailVersionLocalPrefix,
		Default:         detailLayoutVer,
		LocalTTLSeconds: versionLocalTTL(cfg),
	}
	if l1 != nil {
		v.Local = l1.Cache
	}
	return v
}

// newFeedVersions 构造 Feed 版本号读取器（公共与「我的」共用一套本地短缓存策略）。
//
// 公共 Feed 的 L1 整页键同样编入版本号——没有本地缓存时，
// 每次 L1 命中都要为版本号付一次 Redis 往返，与详情当年同款缺陷。
func newFeedVersions(rdb *redis.Client, l1 *PrefixCache, cfg *config.KnowPostConfig) *cache.Versions {
	v := &cache.Versions{
		Redis:           rdb,
		LocalPrefix:     feedVersionLocalPrefix,
		Default:         1,
		LocalTTLSeconds: versionLocalTTL(cfg),
	}
	if l1 != nil {
		v.Local = l1.Cache
	}
	return v
}

// detailCacheConfig 返回详情缓存配置；cfg 为 nil（零依赖单测）时，
// 走与全局装配**同一条代码路径**取默认值——零值节 + 节级 ApplyDefaults。
// 默认值因此只有 pkg/config 一处出处，不存在“服务内回退表”这个第二数字源。
func detailCacheConfig(cfg *config.KnowPostConfig) config.KnowPostDetailCacheConfig {
	if cfg != nil {
		return cfg.DetailCache
	}
	var d config.KnowPostDetailCacheConfig
	d.ApplyDefaults()
	return d
}

// feedCacheConfig 同 detailCacheConfig，作用于 Feed 缓存节。
func feedCacheConfig(cfg *config.KnowPostFeedCacheConfig) config.KnowPostFeedCacheConfig {
	if cfg != nil {
		return *cfg
	}
	var f config.KnowPostFeedCacheConfig
	f.ApplyDefaults()
	return f
}
