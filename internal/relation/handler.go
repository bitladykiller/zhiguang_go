package relation

import (
	"strconv"

	"github.com/gin-gonic/gin"
	"github.com/zhiguang/app/pkg/errcode"
	"github.com/zhiguang/app/pkg/httputil"
	"github.com/zhiguang/app/pkg/middleware"
	"github.com/zhiguang/app/pkg/response"
)

// RelationHandler 暴露关注、取关和关系列表相关 HTTP 接口。
type RelationHandler struct {
	svc RelationServiceInterface
}

// NewRelationHandler 创建 RelationHandler 实例。
//
// 参数:
//   - svc: RelationServiceInterface 实现，负责关系业务逻辑
//
// 返回值:
//   - *RelationHandler: 已初始化的 Handler 实例
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

// Follow 处理 POST /relations/follow。
//
// 请求：{"to_user_id": 12345}
// 响应：200 {"code": 0, "data": {"success": true}}
//
// 边界情况：
//   - 自己关注自己：返回 400 "cannot follow yourself"。
//   - 被限流（操作太快）：返回 429 "rate limited or already following"。
//   - 未登录：返回 401。
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
		middleware.RecordError(c, err)
		response.Error(c, httputil.ToAppError(err))
		return
	}
	if !ok {
		response.Fail(c, 429, "rate limited or already following")
		return
	}
	response.Success(c, gin.H{"success": true})
}

// Unfollow 处理 POST /relations/unfollow。
//
// 请求：{"to_user_id": 12345}
// 响应：200 {"code": 0, "data": {"success": true, "changed": true}}
//   changed=true 表示取关成功；changed=false 表示之前就已取关。
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
		middleware.RecordError(c, err)
		response.Error(c, httputil.ToAppError(err))
		return
	}
	response.Success(c, gin.H{"success": true, "changed": ok})
}

// Status 处理 GET /relations/status?other_id=12345。
//
// 功能：返回当前登录用户与目标用户之间的关系状态。
// 响应：{"status": "mutual" | "following" | "followed" | "none"}
func (h *RelationHandler) Status(c *gin.Context) {
	userID, ok := middleware.GetUserID(c)
	if !ok {
		response.Error(c, errcode.ErrUnauthorized)
		return
	}
	otherID := httputil.QueryUint64(c, "other_id", 0)
	if otherID == 0 {
		response.Fail(c, 400, "invalid other_id")
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

// Following 处理 GET /relations/following?user_id=12345&limit=20&offset=0。
//
// 功能：使用 offset 分页查询某用户关注的人列表。
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

// Followers 处理 GET /relations/followers?user_id=12345&limit=20&offset=0。
//
// 功能：使用 offset 分页查询某用户的粉丝列表。
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

// FollowingCursor 处理 GET /relations/following/cursor?user_id=12345&limit=20&cursor=0。
//
// 功能：使用游标分页查询某用户关注的人列表。
//
// 游标基于关注时间的毫秒时间戳。cursor=0 表示从头开始（获取最新关注）。
// 响应中包含 next_cursor 可用于后续请求。
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

// FollowersCursor 处理 GET /relations/followers/cursor?user_id=12345&limit=20&cursor=0。
//
// 功能：使用游标分页查询某用户的粉丝列表。
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

// queryInt64 从查询参数中解析 int64 值，缺失或非法时返回 0。
//
// 功能：用于解析游标值。游标是 int64 类型的毫秒时间戳。
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

// queryUint64 从查询参数中解析 uint64 值，缺失或非法时返回 0。
//
// 功能：用于解析查询参数中的 user_id。
// 与 queryInt64 的区别在于返回值是无符号整型。
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
