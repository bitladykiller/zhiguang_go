package bootstrap

import (
	"github.com/zhiguang/app/internal/profile"
	"github.com/zhiguang/app/internal/server"
)

// BuildProfileHandler 构建资料模块 HTTP 处理器。
//
// profile 目前边界较小，没有后台 runner，也不依赖可选组件，
// 因此这里只返回一个路由注册器。
func BuildProfileHandler(infra *InfraDeps) server.RouteRegistrar {
	profileRepo := profile.NewProfileRepository(infra.DB)
	profileSvc := profile.NewProfileService(profileRepo)
	return profile.NewProfileHandler(profileSvc)
}
