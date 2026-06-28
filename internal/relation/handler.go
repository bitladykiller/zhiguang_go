package relation

import (
	"strconv"

	"github.com/gin-gonic/gin"
	"github.com/zhiguang/app/pkg/errcode"
	"github.com/zhiguang/app/pkg/httputil"
	"github.com/zhiguang/app/pkg/middleware"
	"github.com/zhiguang/app/pkg/response"
)

// RelationHandler exposes HTTP endpoints for follow, unfollow, and relationship list operations.
type RelationHandler struct {
	svc RelationServiceInterface
}

// NewRelationHandler creates a RelationHandler instance.
//
// Parameters:
//   - svc: RelationServiceInterface implementation, handles relation business logic
//
// Returns:
//   - *RelationHandler: initialized Handler instance
func NewRelationHandler(svc RelationServiceInterface) *RelationHandler {
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

// Follow handles POST /relations/follow.
//
// Request: {"to_user_id": 12345}
// Response: 200 {"code": 0, "data": {"success": true}}
//
// Edge cases:
//   - Following yourself: returns 400 "cannot follow yourself".
//   - Rate limited (too fast): returns 429 "rate limited or already following".
//   - Not logged in: returns 401.
func (h *RelationHandler) Follow(c *gin.Context) {
	userID, ok := middleware.GetUserID(c)
	if !ok {
		response.Error(c, errcode.ErrUnauthorized)
		return
	}
	var req FollowRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		response.Error(c, errcode.ErrBadRequest.WithMsg("invalid request"))
		return
	}
	if req.ToUserID == userID {
		response.Error(c, errcode.ErrBadRequest.WithMsg("cannot follow yourself"))
		return
	}
	ok, err := h.svc.Follow(c.Request.Context(), userID, req.ToUserID)
	if err != nil {
		middleware.RecordError(c, err)
		response.Error(c, httputil.ToAppError(err))
		return
	}
	if !ok {
		response.Error(c, errcode.ErrTooManyRequests.WithMsg("rate limited or already following"))
		return
	}
	response.Success(c, gin.H{"success": true})
}

// Unfollow handles POST /relations/unfollow.
//
// Request: {"to_user_id": 12345}
// Response: 200 {"code": 0, "data": {"success": true, "changed": true}}
//   changed=true indicates unfollow succeeded; changed=false indicates already unfollowed.
func (h *RelationHandler) Unfollow(c *gin.Context) {
	userID, ok := middleware.GetUserID(c)
	if !ok {
		response.Error(c, errcode.ErrUnauthorized)
		return
	}
	var req FollowRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		response.Error(c, errcode.ErrBadRequest.WithMsg("invalid request"))
		return
	}
	ok, err := h.svc.Unfollow(c.Request.Context(), userID, req.ToUserID)
	if err != nil {
		middleware.RecordError(c, err)
		response.Error(c, httputil.ToAppError(err))
		return
	}
	response.Success(c, gin.H{"success": true, "changed": ok})
}

// Status handles GET /relations/status?other_id=12345.
//
// Function: returns the relationship status between the current logged-in user and the target user.
// Response: {"status": "mutual" | "following" | "followed" | "none"}
func (h *RelationHandler) Status(c *gin.Context) {
	userID, ok := middleware.GetUserID(c)
	if !ok {
		response.Error(c, errcode.ErrUnauthorized)
		return
	}
	otherID := httputil.QueryUint64(c, "other_id", 0)
	if otherID == 0 {
		response.Error(c, errcode.ErrBadRequest.WithMsg("invalid other_id"))
		return
	}
	status, err := h.svc.RelationStatus(c.Request.Context(), userID, otherID)
	if err != nil {
		middleware.RecordError(c, err)
		response.Error(c, httputil.ToAppError(err))
		return
	}
	response.Success(c, gin.H{"status": status})
}

// Following handles GET /relations/following?user_id=12345&limit=20&offset=0.
//
// Function: queries the list of users that a given user follows, using offset pagination.
// Note: this is a public endpoint and does not require authentication.
func (h *RelationHandler) Following(c *gin.Context) {
	userID := queryUint64(c, "user_id")
	limit := httputil.QueryInt(c, "limit", 20)
	offset := httputil.QueryInt(c, "offset", 0)

	data, err := h.svc.Following(c.Request.Context(), userID, limit, offset)
	if err != nil {
		middleware.RecordError(c, err)
		response.Error(c, httputil.ToAppError(err))
		return
	}
	response.Success(c, gin.H{"data": data})
}

// Followers handles GET /relations/followers?user_id=12345&limit=20&offset=0.
//
// Function: queries the list of followers for a given user, using offset pagination.
// Note: this is a public endpoint and does not require authentication.
func (h *RelationHandler) Followers(c *gin.Context) {
	userID := queryUint64(c, "user_id")
	limit := httputil.QueryInt(c, "limit", 20)
	offset := httputil.QueryInt(c, "offset", 0)

	data, err := h.svc.Followers(c.Request.Context(), userID, limit, offset)
	if err != nil {
		middleware.RecordError(c, err)
		response.Error(c, httputil.ToAppError(err))
		return
	}
	response.Success(c, gin.H{"data": data})
}

// FollowingCursor handles GET /relations/following/cursor?user_id=12345&limit=20&cursor=0.
//
// Function: queries the list of users that a given user follows, using cursor pagination.
// Note: this is a public endpoint and does not require authentication.
//
// The cursor is based on the millisecond timestamp of the follow time. cursor=0 means start from the beginning (newest follows).
// The response includes next_cursor which can be used for subsequent requests.
func (h *RelationHandler) FollowingCursor(c *gin.Context) {
	userID := queryUint64(c, "user_id")
	limit := httputil.QueryInt(c, "limit", 20)
	cursor := queryInt64(c, "cursor")

	data, nextCursor, err := h.svc.FollowingCursor(c.Request.Context(), userID, limit, cursor)
	if err != nil {
		middleware.RecordError(c, err)
		response.Error(c, httputil.ToAppError(err))
		return
	}
	response.Success(c, gin.H{"data": data, "cursor": nextCursor, "has_more": len(data) >= limit})
}

// FollowersCursor handles GET /relations/followers/cursor?user_id=12345&limit=20&cursor=0.
//
// Function: queries the list of followers for a given user, using cursor pagination.
// Note: this is a public endpoint and does not require authentication.
func (h *RelationHandler) FollowersCursor(c *gin.Context) {
	userID := queryUint64(c, "user_id")
	limit := httputil.QueryInt(c, "limit", 20)
	cursor := queryInt64(c, "cursor")

	data, nextCursor, err := h.svc.FollowersCursor(c.Request.Context(), userID, limit, cursor)
	if err != nil {
		middleware.RecordError(c, err)
		response.Error(c, httputil.ToAppError(err))
		return
	}
	response.Success(c, gin.H{"data": data, "cursor": nextCursor, "has_more": len(data) >= limit})
}

// queryInt64 parses an int64 value from query parameters, returning 0 if missing or invalid.
//
// Function: used to parse cursor values. Cursor is an int64 millisecond timestamp.
func queryInt64(c *gin.Context, key string) int64 {
	s := c.Query(key)
	if s == "" {
		return 0
	}
	v, err := strconv.ParseInt(s, 10, 64)
	if err != nil {
		return 0
	}
	return v
}

// queryUint64 parses a uint64 value from query parameters, returning 0 if missing or invalid.
//
// Function: used to parse user_id from query parameters.
// The difference from queryInt64 is that the return value is unsigned.
func queryUint64(c *gin.Context, key string) uint64 {
	s := c.Query(key)
	if s == "" {
		return 0
	}
	v, err := strconv.ParseUint(s, 10, 64)
	if err != nil {
		return 0
	}
	return v
}
