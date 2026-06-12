package relation

import "github.com/gin-gonic/gin"

// RelationHandler 暴露关注、取关和关系列表相关 HTTP 接口。
//
// 为了避免写操作、读操作和 handler 级辅助逻辑继续堆在同一文件里，
// 当前按职责拆分为：
//   - handler.go: 结构体、构造函数、路由注册
//   - handler_write.go: 关注/取关写接口
//   - handler_read.go: 关系状态和列表读取接口
//   - handler_helpers.go: handler 内部复用的登录态、请求绑定和查询参数解析
type RelationHandler struct {
	svc RelationUseCase
}

func NewRelationHandler(svc RelationUseCase) *RelationHandler {
	return &RelationHandler{svc: svc}
}

func (h *RelationHandler) RegisterRoutes(r *gin.RouterGroup) {
	rel := r.Group("/relations")
	{
		rel.POST("/follow", h.Follow)
		rel.POST("/unfollow", h.Unfollow)
		rel.GET("/status", h.Status)
		rel.GET("/following", h.Following)
		rel.GET("/followers", h.Followers)
		rel.GET("/following/cursor", h.FollowingCursor)
		rel.GET("/followers/cursor", h.FollowersCursor)
	}
}
