package search

import (
	"context"

	"github.com/gin-gonic/gin"
	"github.com/zhiguang/app/pkg/errcode"
	"github.com/zhiguang/app/pkg/httputil"
	"github.com/zhiguang/app/pkg/middleware"
	"github.com/zhiguang/app/pkg/response"
)

// SearchServiceInterface 定义搜索模块对外暴露的业务方法。
//
// Handler 依赖此接口而非具体 *SearchService，使得 handler 可以独立于
// service 实现进行单元测试，同时支持在搜索服务不可用时注入 nil。
type SearchServiceInterface interface {
	Search(ctx context.Context, keyword string, size int, tagsCSV, after string, currentUserID *uint64) (*SearchResponse, error)
	Suggest(ctx context.Context, prefix string, size int) ([]string, error)
}

// 编译期断言：*SearchService 实现了 SearchServiceInterface。
var _ SearchServiceInterface = (*SearchService)(nil)

// SearchHandler 暴露内容搜索的 HTTP 端点。
type SearchHandler struct {
	svc SearchServiceInterface
}

// NewSearchHandler 创建一个搜索 HTTP handler。
//
// 参数：
//   - svc: 搜索服务实例（可为 nil，当 ES 配置不完整时进入降级模式）
//
// 返回：
//   - *SearchHandler: handler 实例
//
// 说明：
//   svc 可为 nil（当 ES 配置不完整时），此时所有端点返回 503 Service Unavailable。
//   这种设计防止 ES 不可用导致启动失败，使其他模块可以正常工作。
func NewSearchHandler(svc SearchServiceInterface) *SearchHandler {
	return &SearchHandler{svc: svc}
}

// RegisterRoutes 在给定的路由组下注册搜索相关的 HTTP 端点。
//
// 参数：
//   - r: Gin 路由组（通常是 /api/v1 的子组）
//
// 注册的端点：
//   - GET /search: 全文搜索（Search）
//   - GET /search/suggest: 自动补全建议（Suggest）
//
// 说明：
//   所有搜索端点使用 GET 方法，符合 RESTful 查询语义。
//   搜索参数（关键字、标签、游标）通过查询字符串传递。
func (h *SearchHandler) RegisterRoutes(r *gin.RouterGroup) {
	sr := r.Group("/search")
	{
		sr.GET("", h.Search)
		sr.GET("/suggest", h.Suggest)
	}
}

// Search 处理 GET /search，执行全文搜索并返回结果。
//
// 请求参数（查询字符串）：
//   - q:     搜索关键字（必填）
//   - size:  每页结果数（可选，默认 20）
//   - tags:  标签过滤，逗号分隔（可选）
//   - after: 游标值，由上一页响应的 next_after 提供（可选）
//
// 响应：
//   - 成功: HTTP 200 + SearchResponse JSON（包含 items 列表、next_after、has_more）
//   - 失败: HTTP 400（缺少 q 参数）、HTTP 500（内部搜索错误）、HTTP 503（服务不可用）
//
// 认证：
//   搜索端点不需要登录，但会尝试从上下文中获取用户信息。
//   如果用户已登录，搜索结果会包含每条结果的点赞/收藏状态。
//
// 边界情况：
//   - 当 svc 为 nil 时返回 503（ES 配置缺失或连接失败）
//   - 当关键字为空时返回 400
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

// Suggest 处理 GET /search/suggest，返回前缀匹配的自动补全建议。
//
// 请求参数（查询字符串）：
//   - prefix: 用户输入的前缀（必填）
//   - size:   建议数量（可选，默认 10）
//
// 响应：
//   - 成功: HTTP 200 + JSON { items: ["suggestion1", "suggestion2", ...] }
//   - 失败: HTTP 500（内部搜索错误）、HTTP 503（服务不可用）
//
// 说明：
//   建议来自知文标题和标签，
//   使用 ES completion suggester 基于 FST 数据结构进行前缀匹配。
//
// 边界情况：
//   - 当 svc 为 nil 时返回 503
//   - ES 在 prefix 为空时会返回错误；调用方必须确保传入有效的前缀
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
