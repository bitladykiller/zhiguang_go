package search

import (
	"github.com/gin-gonic/gin"
	"github.com/zhiguang/app/pkg/errcode"
	"github.com/zhiguang/app/pkg/httputil"
	"github.com/zhiguang/app/pkg/middleware"
	"github.com/zhiguang/app/pkg/response"
)

// SearchHandler 暴露内容搜索相关的 HTTP 接口。
type SearchHandler struct {
	svc SearchServiceInterface
}

// NewSearchHandler 创建搜索 HTTP 处理器。
//
// 参数:
//   - svc: 搜索服务实例（可能为 nil，对应 ES 配置不完整时的降级场景）
//
// 返回值:
//   - *SearchHandler: 处理器实例
//
// 说明:
//   svc 可能为 nil（当 ES 配置不完整时），此时所有接口返回 503 Service Unavailable。
//   这样设计是为了避免启动时因 ES 不可用而拒绝服务，让其他模块正常工作。
func NewSearchHandler(svc SearchServiceInterface) *SearchHandler {
	return &SearchHandler{svc: svc}
}

// RegisterRoutes 在给定的路由组下注册搜索相关的 HTTP 接口。
//
// 参数:
//   - r: Gin 路由组（通常是 /api/v1 下的子路由组）
//
// 注册的端点:
//   - GET /search:  全文搜索 (Search)
//   - GET /search/suggest: 自动补全建议 (Suggest)
//
// 说明:
//   所有搜索接口均注册为 GET 方法，符合 RESTful 查询语义。
//   搜索参数（关键词、标签、游标）通过查询字符串传递。
func (h *SearchHandler) RegisterRoutes(r *gin.RouterGroup) {
	sr := r.Group("/search")
	{
		sr.GET("", h.Search)
		sr.GET("/suggest", h.Suggest)
	}
}

// Search 处理 GET /search 请求，执行全文搜索并返回结果。
//
// 请求参数（查询字符串）:
//   - q:     搜索关键词（必填）
//   - size:  每页结果数（可选，默认 20）
//   - tags:  标签筛选，逗号分隔（可选）
//   - after: 游标值，由上一页响应中的 next_after 提供（可选）
//
// 响应:
//   - 成功: HTTP 200 + SearchResponse JSON（包含 items 列表、next_after、has_more）
//   - 失败: HTTP 400（缺少 q 参数）、HTTP 500（搜索内部错误）、HTTP 503（服务不可用）
//
// 鉴权:
//   搜索接口不强制要求登录，但会尝试从上下文中获取用户信息。
//   如果用户已登录，搜索结果中会包含每个结果的点赞/收藏状态。
//
// 边界情况:
//   - svc 为 nil 时返回 503（ES 配置缺失或连接失败）
//   - keyword 为空时返回 400
func (h *SearchHandler) Search(c *gin.Context) {
	if h.svc == nil {
		response.Error(c, errcode.ErrServiceUnavailable.WithMsg("search service is unavailable"))
		return
	}

	keyword := c.Query("q")
	if keyword == "" {
		response.Error(c, errcode.ErrBadRequest.WithMsg("query parameter 'q' is required"))
		return
	}

	var currentUserID *uint64
	if userID, ok := middleware.GetUserID(c); ok {
		currentUserID = &userID
	}

	tags := c.Query("tags")
	after := c.Query("after")
	size := httputil.QueryInt(c, "size", 20)

	result, err := h.svc.Search(c.Request.Context(), keyword, size, tags, after, currentUserID)
	if err != nil {
		middleware.RecordError(c, err)
		response.Error(c, httputil.ToAppError(err))
		return
	}
	response.Success(c, result)
}

// Suggest 处理 GET /search/suggest 请求，返回前缀匹配的自动补全建议。
//
// 请求参数（查询字符串）:
//   - prefix: 用户输入的前缀（必填）
//   - size:   建议数量（可选，默认 10）
//
// 响应:
//   - 成功: HTTP 200 + JSON { items: ["建议1", "建议2", ...] }
//   - 失败: HTTP 500（搜索内部错误）、HTTP 503（服务不可用）
//
// 说明:
//   建议来源包括知文的标题和标签，
//   使用 ES completion suggester 在 FST 数据结构上执行前缀匹配。
//
// 边界情况:
//   - svc 为 nil 时返回 503
//   - prefix 为空时 ES 会报错，调用方需确保传入有效前缀
func (h *SearchHandler) Suggest(c *gin.Context) {
	if h.svc == nil {
		response.Error(c, errcode.ErrServiceUnavailable.WithMsg("search service is unavailable"))
		return
	}

	prefix := c.Query("prefix")
	size := httputil.QueryInt(c, "size", 10)

	suggestions, err := h.svc.Suggest(c.Request.Context(), prefix, size)
	if err != nil {
		middleware.RecordError(c, err)
		response.Error(c, httputil.ToAppError(err))
		return
	}
	response.Success(c, gin.H{"items": suggestions})
}
