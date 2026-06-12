package counter

import "github.com/gin-gonic/gin"

// CounterHandler 暴露计数器模块的 HTTP 接口。
//
// 为了避免写操作、读操作和 handler 级辅助逻辑继续堆在同一文件里，
// 当前按职责拆分为：
//   - handler.go: 结构体、构造函数、路由注册
//   - handler_write.go: 点赞/收藏写路径
//   - handler_read.go: 计数与状态查询
//   - handler_helpers.go: handler 内部复用的请求解析与错误处理
type CounterHandler struct {
	svc CounterUseCase
}

func NewCounterHandler(svc CounterUseCase) *CounterHandler {
	return &CounterHandler{svc: svc}
}

// RegisterRoutes 注册计数器模块路由，所有接口都要求登录。
func (h *CounterHandler) RegisterRoutes(r *gin.RouterGroup) {
	ctr := r.Group("/counter")
	{
		ctr.POST("/like", h.Like)
		ctr.POST("/unlike", h.Unlike)
		ctr.POST("/fav", h.Fav)
		ctr.POST("/unfav", h.Unfav)
		ctr.GET("/counts", h.GetCounts)
		ctr.GET("/status", h.Status)
	}
}
