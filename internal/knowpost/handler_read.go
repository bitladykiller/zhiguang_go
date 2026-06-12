package knowpost

import (
	"github.com/gin-gonic/gin"
	"github.com/zhiguang/app/pkg/errcode"
	"github.com/zhiguang/app/pkg/response"
)

// GetDetail 处理 GET /knowposts/:id。
//
// 功能：返回知文详情。当前用户可登录也可不登录。
// 登录用户会额外获得点赞/收藏状态。
//
// 请求：GET /knowposts/:id
func (h *KnowPostHandler) GetDetail(c *gin.Context) {
	id, ok := parseKnowPostID(c)
	if !ok {
		return
	}

	resp, err := h.svc.GetDetail(id, optionalUserID(c))
	if err != nil {
		response.Error(c, toAppErr(err))
		return
	}
	response.Success(c, resp)
}

// GetPublicFeed 处理 GET /knowposts/feed/public。
//
// 功能：返回公共 Feed（已发布且公开的知文列表），支持分页，可选附带当前用户的点赞/收藏状态。
//
// 请求：GET /knowposts/feed/public?page=1&size=20
//
// 用户状态：
//   - 携带 JWT token：在 Feed 条目中附加 Liked/Faved 状态。
//   - 不携带 JWT token：Feed 条目中 Liked/Faved 为 nil。
func (h *KnowPostHandler) GetPublicFeed(c *gin.Context) {
	page := queryInt(c, "page", 1)
	size := queryInt(c, "size", 20)

	resp, err := h.feedSvc.GetPublicFeed(page, size, optionalUserID(c))
	if err != nil {
		response.Error(c, errcode.ErrInternal.WithMsg(err.Error()))
		return
	}
	response.Success(c, resp)
}

// GetMyPublished 处理 GET /knowposts/feed/mine。
//
// 功能：返回当前登录用户自己的已发布知文列表。
// 与 GetPublicFeed 不同，此接口必须要求用户已登录。
//
// 请求：GET /knowposts/feed/mine?page=1&size=20
//
// 边界情况：
//   - 未提供 JWT token（未登录）：返回 401 Unauthorized。
func (h *KnowPostHandler) GetMyPublished(c *gin.Context) {
	userID, ok := requireUserID(c)
	if !ok {
		return
	}

	page := queryInt(c, "page", 1)
	size := queryInt(c, "size", 20)
	resp, err := h.feedSvc.GetMyPublished(userID, page, size)
	if err != nil {
		response.Error(c, errcode.ErrInternal.WithMsg(err.Error()))
		return
	}
	response.Success(c, resp)
}
