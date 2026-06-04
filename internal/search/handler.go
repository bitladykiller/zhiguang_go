package search

import (
	"strconv"

	"github.com/gin-gonic/gin"
	"github.com/zhiguang/app/pkg/middleware"
	"github.com/zhiguang/app/pkg/response"
)

// SearchHandler 暴露内容搜索相关的 HTTP 接口。
type SearchHandler struct {
	svc *SearchService
}

func NewSearchHandler(svc *SearchService) *SearchHandler {
	return &SearchHandler{svc: svc}
}

func (h *SearchHandler) RegisterRoutes(r *gin.RouterGroup) {
	sr := r.Group("/search")
	{
		sr.GET("", h.Search)
		sr.GET("/suggest", h.Suggest)
	}
}

// Search 处理 `GET /search?q=xxx&size=20&tags=go,redis&after=xxx`。
func (h *SearchHandler) Search(c *gin.Context) {
	if h.svc == nil {
		response.Fail(c, 503, "search service is unavailable")
		return
	}

	keyword := c.Query("q")
	if keyword == "" {
		response.Fail(c, 400, "query parameter 'q' is required")
		return
	}

	var currentUserID *uint64
	if userID, ok := middleware.GetUserID(c); ok {
		currentUserID = &userID
	}

	tags := c.Query("tags")
	after := c.Query("after")
	size := queryInt(c, "size", 20)

	result, err := h.svc.Search(c.Request.Context(), keyword, size, tags, after, currentUserID)
	if err != nil {
		response.Fail(c, 500, err.Error())
		return
	}
	response.Success(c, result)
}

// Suggest 处理 `GET /search/suggest?prefix=xxx&size=10`。
func (h *SearchHandler) Suggest(c *gin.Context) {
	if h.svc == nil {
		response.Fail(c, 503, "search service is unavailable")
		return
	}

	prefix := c.Query("prefix")
	size := queryInt(c, "size", 10)

	suggestions, err := h.svc.Suggest(c.Request.Context(), prefix, size)
	if err != nil {
		response.Fail(c, 500, err.Error())
		return
	}
	response.Success(c, gin.H{"items": suggestions})
}

func queryInt(c *gin.Context, key string, def int) int {
	s := c.Query(key)
	if s == "" {
		return def
	}
	v, _ := strconv.Atoi(s)
	if v <= 0 {
		return def
	}
	return v
}
