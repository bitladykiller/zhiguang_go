package relation

import (
	"github.com/gin-gonic/gin"
	"github.com/zhiguang/app/pkg/response"
)

// Status 处理 GET /relations/status?other_id=12345。
//
// 功能：返回当前登录用户与目标用户之间的关系状态。
// 响应：{"status": "mutual" | "following" | "followed" | "none"}
func (h *RelationHandler) Status(c *gin.Context) {
	userID, ok := requireUserID(c)
	if !ok {
		return
	}
	otherID, ok := otherIDQuery(c)
	if !ok {
		return
	}

	status, err := h.svc.RelationStatus(c.Request.Context(), userID, otherID)
	if err != nil {
		response.Fail(c, 500, err.Error())
		return
	}
	response.Success(c, gin.H{"status": status})
}

// Following 处理 GET /relations/following?user_id=12345&limit=20&offset=0。
//
// 功能：使用 offset 分页查询某用户关注的人列表。
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

// Followers 处理 GET /relations/followers?user_id=12345&limit=20&offset=0。
//
// 功能：使用 offset 分页查询某用户的粉丝列表。
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

// FollowingCursor 处理 GET /relations/following/cursor?user_id=12345&limit=20&cursor=0。
//
// 功能：使用游标分页查询某用户关注的人列表。
//
// 游标基于关注时间的毫秒时间戳。cursor=0 表示从头开始（获取最新关注）。
// 响应中包含 next_cursor 可用于后续请求。
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

// FollowersCursor 处理 GET /relations/followers/cursor?user_id=12345&limit=20&cursor=0。
//
// 功能：使用游标分页查询某用户的粉丝列表。
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
