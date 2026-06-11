package bootstrap

import (
	"github.com/zhiguang/app/internal/knowpost"
	"github.com/zhiguang/app/internal/server"
)

// BuildKnowPostHandler 构建知文领域，并返回其路由注册器。
//
// 尽管内部会组装两个具体 service，但对外只暴露一个 RouteRegistrar，
// 避免 bootstrap 之外继续依赖 knowpost 的具体 handler / service 类型。
func BuildKnowPostHandler(infra *InfraDeps, counterClient knowpost.CounterClient) server.RouteRegistrar {
	feedSvc := knowpost.NewKnowPostFeedService(knowpost.KnowPostFeedServiceDeps{
		Repo:     knowpost.NewKnowPostRepository(infra.DB),
		Redis:    infra.Redis,
		L1Public: infra.FeedPublicCache,
		L1Mine:   infra.FeedMineCache,
		HotKey:   infra.HotKeyDetector,
		Counter:  counterClient,
	})
	kpSvc := knowpost.NewKnowPostService(knowpost.KnowPostServiceDeps{
		DB:        infra.DB,
		IDGen:     infra.IDGen,
		Redis:     infra.Redis,
		L1Cache:   infra.DetailCache,
		HotKey:    infra.HotKeyDetector,
		OSSConfig: &infra.Config.OSS,
		Counter:   counterClient,
		FeedCache: feedSvc,
	})

	return knowpost.NewKnowPostHandler(kpSvc, feedSvc)
}
