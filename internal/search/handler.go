package search

import (
	"github.com/gin-gonic/gin"
	"github.com/zhiguang/app/pkg/errcode"
	"github.com/zhiguang/app/pkg/httputil"
	"github.com/zhiguang/app/pkg/middleware"
	"github.com/zhiguang/app/pkg/response"
)

// SearchHandler exposes HTTP endpoints for content search.
type SearchHandler struct {
	svc SearchServiceInterface
}

// NewSearchHandler creates a search HTTP handler.
//
// Parameters:
//   - svc: search service instance (can be nil for degraded mode when ES config is incomplete)
//
// Returns:
//   - *SearchHandler: handler instance
//
// Remarks:
//   svc can be nil (when ES config is incomplete), in which case all endpoints return 503 Service Unavailable.
//   This design prevents startup failure from ES unavailability, allowing other modules to work normally.
func NewSearchHandler(svc SearchServiceInterface) *SearchHandler {
	return &SearchHandler{svc: svc}
}

// RegisterRoutes registers search-related HTTP endpoints under the given router group.
//
// Parameters:
//   - r: Gin router group (typically a sub-group under /api/v1)
//
// Registered endpoints:
//   - GET /search: full-text search (Search)
//   - GET /search/suggest: auto-complete suggestions (Suggest)
//
// Remarks:
//   All search endpoints use GET method, conforming to RESTful query semantics.
//   Search parameters (keyword, tags, cursor) are passed via query string.
func (h *SearchHandler) RegisterRoutes(r *gin.RouterGroup) {
	sr := r.Group("/search")
	{
		sr.GET("", h.Search)
		sr.GET("/suggest", h.Suggest)
	}
}

// Search handles GET /search, performing full-text search and returning results.
//
// Request parameters (query string):
//   - q:     search keyword (required)
//   - size:  results per page (optional, default 20)
//   - tags:  tag filter, comma-separated (optional)
//   - after: cursor value, provided by next_after from previous page response (optional)
//
// Response:
//   - Success: HTTP 200 + SearchResponse JSON (contains items list, next_after, has_more)
//   - Failure: HTTP 400 (missing q parameter), HTTP 500 (internal search error), HTTP 503 (service unavailable)
//
// Authentication:
//   Search endpoints do not require login, but will attempt to retrieve user info from context.
//   If the user is logged in, search results include like/favorite status for each result.
//
// Edge cases:
//   - Returns 503 when svc is nil (ES config missing or connection failed)
//   - Returns 400 when keyword is empty
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

// Suggest handles GET /search/suggest, returning prefix-matched auto-complete suggestions.
//
// Request parameters (query string):
//   - prefix: user input prefix (required)
//   - size:   number of suggestions (optional, default 10)
//
// Response:
//   - Success: HTTP 200 + JSON { items: ["suggestion1", "suggestion2", ...] }
//   - Failure: HTTP 500 (internal search error), HTTP 503 (service unavailable)
//
// Remarks:
//   Suggestions come from knowpost titles and tags,
//   using ES completion suggester for prefix matching on FST data structure.
//
// Edge cases:
//   - Returns 503 when svc is nil
//   - ES will error if prefix is empty; caller must ensure a valid prefix is passed
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
