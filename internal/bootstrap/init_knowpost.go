package bootstrap

import (
	"context"
	"time"

	"github.com/coocood/freecache"
	"github.com/jmoiron/sqlx"
	"github.com/redis/go-redis/v9"
	"go.uber.org/zap"

	"github.com/zhiguang/app/internal/cache"
	"github.com/zhiguang/app/internal/counter"
	"github.com/zhiguang/app/internal/knowpost"
	"github.com/zhiguang/app/pkg/config"
	"github.com/zhiguang/app/pkg/idgen"
)

// initKnowPost 创建知文模块的完整服务栈。
//
// 知文按读写路径拆为三个服务，依赖面互不重叠：
//
//	KnowPostFeedService    Feed 列表读（公共 / 我的已发布 / 关注流）
//	KnowPostDetailService  详情读（Tiered 三级缓存 + Bloom + 热点）
//	KnowPostService        写路径（草稿 / 发布 / 编辑 / 删除 + 写后失效）
//
// Bloom 过滤器由本函数创建一次，同时注入写服务（发布 ADD / 删除 DEL）
// 与详情读服务（EXISTS 预判）——两侧共享同一份过滤器状态。
//
// counterSvc 以具体类型传入，各读服务在自己的**消费侧窄接口**上接收它
// （detailEngagement / feedEngagement），依赖面即真实使用面。
func initKnowPost(
	db *sqlx.DB,
	redisClient *redis.Client,
	l1Cache *freecache.Cache,
	hotKeyDetector *cache.HotKeyDetector,
	cfg *config.Config,
	idGen *idgen.SnowflakeGenerator,
	counterSvc *counter.CounterService,
	logger *zap.Logger,
) (*knowpost.KnowPostHandler, *knowpost.KnowPostService, *knowpost.KnowPostFeedService) {
	detailCache := &knowpost.PrefixCache{Cache: l1Cache, Prefix: "d:"}
	feedPublicCache := &knowpost.PrefixCache{Cache: l1Cache, Prefix: "fp:"}
	feedMineCache := &knowpost.PrefixCache{Cache: l1Cache, Prefix: "fm:"}

	repo := knowpost.NewKnowPostRepository(db)
	bloom := knowpost.NewDetailBloom(redisClient, &cfg.KnowPost, logger)

	feedSvc := knowpost.NewKnowPostFeedService(repo, redisClient, feedPublicCache, feedMineCache, hotKeyDetector, counterSvc, logger, &cfg.KnowPost.FeedCache)
	detailSvc := knowpost.NewKnowPostDetailService(repo, redisClient, detailCache, hotKeyDetector, bloom, counterSvc, logger, &cfg.KnowPost)
	kpSvc := knowpost.NewKnowPostService(db, idGen, redisClient, detailCache, bloom, &cfg.OSS, feedSvc, logger, nil, &cfg.KnowPost)
	kpHandler := knowpost.NewKnowPostHandler(kpSvc, detailSvc, feedSvc)

	// 异步预热详情 Bloom：与空值缓存叠加，冷启动期间 fail-open 不误拦。
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
		defer cancel()
		if err := detailSvc.WarmDetailBloom(ctx); err != nil {
			logger.Warn("warm detail bloom failed", zap.Error(err))
		}
	}()

	return kpHandler, kpSvc, feedSvc
}
