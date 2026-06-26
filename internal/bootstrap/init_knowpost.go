package bootstrap

import (
	"github.com/coocood/freecache"
	"github.com/jmoiron/sqlx"
	"github.com/redis/go-redis/v9"
	"go.uber.org/zap"

	"github.com/zhiguang/app/internal/cache"
	"github.com/zhiguang/app/internal/knowpost"
	"github.com/zhiguang/app/pkg/config"
	"github.com/zhiguang/app/pkg/idgen"
)

// initKnowPost 创建知文模块的完整服务栈。
//
// 创建顺序：
//   1. KnowPostFeedService（先创建，注入 counter）
//   2. KnowPostService（写操作 + 详情读取 + 缓存管理，注入 counter 和 feedSvc）
//   3. KnowPostHandler（HTTP 请求适配）
//
// 参数：
//   - l1Cache: 统一的 freecache 实例，通过 key 前缀区分不同用途
//   - counter: 计数器客户端（已由 initCounter 创建），nil 表示不使用计数器
//
// 返回：
//   - *knowpost.KnowPostHandler: HTTP handler
//   - *knowpost.KnowPostService: 写/读服务
//   - *knowpost.KnowPostFeedService: Feed 服务
func initKnowPost(
	db *sqlx.DB,
	redisClient *redis.Client,
	l1Cache *freecache.Cache,
	hotKeyDetector *cache.HotKeyDetector,
	cfg *config.Config,
	idGen *idgen.SnowflakeGenerator,
	counter knowpost.CounterClient,
	logger *zap.Logger,
) (*knowpost.KnowPostHandler, *knowpost.KnowPostService, *knowpost.KnowPostFeedService) {
	detailCache := &knowpost.PrefixCache{Cache: l1Cache, Prefix: "d:"}
	feedPublicCache := &knowpost.PrefixCache{Cache: l1Cache, Prefix: "fp:"}
	feedMineCache := &knowpost.PrefixCache{Cache: l1Cache, Prefix: "fm:"}

	feedSvc := knowpost.NewKnowPostFeedService(knowpost.NewKnowPostRepository(db), redisClient, feedPublicCache, feedMineCache, hotKeyDetector, counter, logger)
	kpSvc := knowpost.NewKnowPostService(db, idGen, redisClient, detailCache, hotKeyDetector, &cfg.OSS, counter, feedSvc, logger)
	kpHandler := knowpost.NewKnowPostHandler(kpSvc, kpSvc, feedSvc)

	return kpHandler, kpSvc, feedSvc
}
