package relation

import (
	"strconv"

	"github.com/gin-gonic/gin"
	"github.com/zhiguang/app/pkg/errcode"
	"github.com/zhiguang/app/pkg/middleware"
	"github.com/zhiguang/app/pkg/response"
)

// RelationHandler 暴露关注、取关和关系列表相关 HTTP 接口。
type RelationHandler struct {
	svc *RelationService
}

func NewRelationHandler(svc *RelationService) *RelationHandler {
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

func (h *RelationHandler) Follow(c *gin.Context) {
	userID, ok := middleware.GetUserID(c)
	if !ok {
		response.Error(c, errcode.ErrUnauthorized)
		return
	}
	var req FollowRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		response.Fail(c, 400, "invalid request")
		return
	}
	if req.ToUserID == userID {
		response.Fail(c, 400, "cannot follow yourself")
		return
	}
	ok, err := h.svc.Follow(c.Request.Context(), userID, req.ToUserID)
	if err != nil {
		response.Fail(c, 500, err.Error())
		return
	}
	if !ok {
		response.Fail(c, 429, "rate limited or already following")
		return
	}
	response.Success(c, gin.H{"success": true})
}

func (h *RelationHandler) Unfollow(c *gin.Context) {
	userID, ok := middleware.GetUserID(c)
	if !ok {
		response.Error(c, errcode.ErrUnauthorized)
		return
	}
	var req FollowRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		response.Fail(c, 400, "invalid request")
		return
	}
	ok, err := h.svc.Unfollow(c.Request.Context(), userID, req.ToUserID)
	if err != nil {
		response.Fail(c, 500, err.Error())
		return
	}
	response.Success(c, gin.H{"success": true, "changed": ok})
}

func (h *RelationHandler) Status(c *gin.Context) {
	userID, ok := middleware.GetUserID(c)
	if !ok {
		response.Error(c, errcode.ErrUnauthorized)
		return
	}
	otherID, err := strconv.ParseUint(c.Query("other_id"), 10, 64)
	if err != nil {
		response.Fail(c, 400, "invalid other_id")
		return
	}
	status, err := h.svc.RelationStatus(c.Request.Context(), userID, otherID)
	if err != nil {
		response.Fail(c, 500, err.Error())
		return
	}
	response.Success(c, gin.H{"status": status})
}

func (h *RelationHandler) Following(c *gin.Context) {
	userID := queryUint64(c, "user_id")
	limit := queryInt(c, "limit", 20)
	offset := queryInt(c, "offset", 0)

	data, err := h.svc.Following(c.Request.Context(), userID, limit, offset)
	if err != nil {
		response.Fail(c, 500, err.Error())
		return
	}
	response.Success(c, gin.H{"data": data})
}

func (h *RelationHandler) Followers(c *gin.Context) {
	userID := queryUint64(c, "user_id")
	limit := queryInt(c, "limit", 20)
	offset := queryInt(c, "offset", 0)

	data, err := h.svc.Followers(c.Request.Context(), userID, limit, offset)
	if err != nil {
		response.Fail(c, 500, err.Error())
		return
	}
	response.Success(c, gin.H{"data": data})
}

func (h *RelationHandler) FollowingCursor(c *gin.Context) {
	userID := queryUint64(c, "user_id")
	limit := queryInt(c, "limit", 20)
	cursor := queryInt64(c, "cursor")

	data, nextCursor, err := h.svc.FollowingCursor(c.Request.Context(), userID, limit, cursor)
	if err != nil {
		response.Fail(c, 500, err.Error())
		return
	}
	response.Success(c, gin.H{"data": data, "cursor": nextCursor, "has_more": len(data) >= limit})
}

func (h *RelationHandler) FollowersCursor(c *gin.Context) {
	userID := queryUint64(c, "user_id")
	limit := queryInt(c, "limit", 20)
	cursor := queryInt64(c, "cursor")

	data, nextCursor, err := h.svc.FollowersCursor(c.Request.Context(), userID, limit, cursor)
	if err != nil {
		response.Fail(c, 500, err.Error())
		return
	}
	response.Success(c, gin.H{"data": data, "cursor": nextCursor, "has_more": len(data) >= limit})
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

func queryInt64(c *gin.Context, key string) int64 {
	s := c.Query(key)
	if s == "" {
		return 0
	}
	v, _ := strconv.ParseInt(s, 10, 64)
	return v
}

func queryUint64(c *gin.Context, key string) uint64 {
	s := c.Query(key)
	if s == "" {
		return 0
	}
	v, _ := strconv.ParseUint(s, 10, 64)
	return v
}
