package knowpost

import "github.com/gin-gonic/gin"

// KnowPostHandler 暴露知文模块的 HTTP 接口，负责请求参数解析和响应组装。
//
// 为了避免写操作、读操作和 handler 级辅助逻辑继续堆在同一文件里，
// 当前按职责拆分为：
//   - handler.go: 结构体、构造函数、路由注册
//   - handler_write.go: 写路径 HTTP 处理函数
//   - handler_read.go: 读路径 HTTP 处理函数
//   - handler_helpers.go: handler 内部复用的参数与错误转换辅助函数
type KnowPostHandler struct {
	svc     KnowPostUseCase
	feedSvc KnowPostFeedUseCase
}

// NewKnowPostHandler 创建处理器实例。
func NewKnowPostHandler(svc KnowPostUseCase, feedSvc KnowPostFeedUseCase) *KnowPostHandler {
	return &KnowPostHandler{svc: svc, feedSvc: feedSvc}
}

// RegisterRoutes 注册知文模块的全部路由。
//
// 路由分类：
//   - 写操作（需要 JWT 登录）：
//     /draft（创建草稿）、/:id/content（确认内容）、/:id/publish（发布）等
//   - 读操作（可选登录，使用全局 OptionalAuthMiddleware）：
//     /:id（详情）、/feed/public（公共 feed）、/feed/mine（我的已发布）
//
// 写操作在处理器内通过 middleware.GetUserID 显式鉴权。
// 读操作中 /feed/mine 也必须登录（因为 "我的" 需要知道是谁）。
func (h *KnowPostHandler) RegisterRoutes(r *gin.RouterGroup) {
	kp := r.Group("/knowposts")
	{
		// 写操作（要求登录）
		kp.POST("/draft", h.CreateDraft)
		kp.PUT("/:id/content", h.ConfirmContent)
		kp.PUT("/:id/metadata", h.UpdateMetadata)
		kp.POST("/:id/publish", h.Publish)
		kp.PUT("/:id/top", h.UpdateTop)
		kp.PUT("/:id/visibility", h.UpdateVisibility)
		kp.DELETE("/:id", h.Delete)

		// 读操作（可选登录）
		kp.GET("/:id", h.GetDetail)
		kp.GET("/feed/public", h.GetPublicFeed)
		kp.GET("/feed/mine", h.GetMyPublished)
	}
}
