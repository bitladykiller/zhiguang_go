package relation

import (
	"github.com/gin-gonic/gin"
	"github.com/zhiguang/app/pkg/response"
)

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
	userID, ok := requireUserID(c)
	if !ok {
		return
	}

	req, ok := bindFollowRequest(c)
	if !ok {
		return
	}
	if req.ToUserID == userID {
		response.Fail(c, 400, "cannot follow yourself")
		return
	}

	changed, err := h.svc.Follow(c.Request.Context(), userID, req.ToUserID)
	if err != nil {
		response.Fail(c, 500, err.Error())
		return
	}
	if !changed {
		response.Fail(c, 429, "rate limited or already following")
		return
	}
	response.Success(c, gin.H{"success": true})
}

// Unfollow 处理 POST /relations/unfollow。
//
// 请求：{"to_user_id": 12345}
// 响应：200 {"code": 0, "data": {"success": true, "changed": true}}
//
//	changed=true 表示取关成功；changed=false 表示之前就已取关。
func (h *RelationHandler) Unfollow(c *gin.Context) {
	userID, ok := requireUserID(c)
	if !ok {
		return
	}

	req, ok := bindFollowRequest(c)
	if !ok {
		return
	}

	changed, err := h.svc.Unfollow(c.Request.Context(), userID, req.ToUserID)
	if err != nil {
		response.Fail(c, 500, err.Error())
		return
	}
	response.Success(c, gin.H{"success": true, "changed": changed})
}
