package knowpost

import (
	"strconv"

	"github.com/gin-gonic/gin"
	"github.com/zhiguang/app/pkg/errcode"
	"github.com/zhiguang/app/pkg/response"
)

// CreateDraft 处理 POST /knowposts/draft。
//
// 功能：从 JWT token 中解析用户 ID，调用 CreateDraft 服务创建草稿，
// 然后返回 HTTP 201 Created 响应，body 中包含新的知文 ID。
//
// 请求：POST /knowposts/draft（无需请求体）
// 响应：HTTP 201，{ "code": 0, "message": "created", "data": { "id": "{雪花ID}" } }
//
// 边界情况：
//   - 未提供 JWT token：返回 401 Unauthorized。
//   - 创建失败：返回 500 Internal Server Error。
func (h *KnowPostHandler) CreateDraft(c *gin.Context) {
	userID, ok := requireUserID(c)
	if !ok {
		return
	}

	id, err := h.svc.CreateDraft(userID)
	if err != nil {
		response.Error(c, errcode.ErrInternal.WithMsg(err.Error()))
		return
	}
	response.Created(c, gin.H{"id": strconv.FormatUint(id, 10)})
}

// ConfirmContent 处理 PUT /knowposts/:id/content。
//
// 功能：接收客户端在 OSS 直传完成后返回的对象元数据，
// 更新知文的内容记录。
//
// 请求：PUT /knowposts/:id/content
// Body：{"object_key": "...", "etag": "...", "sha256": "...", "size": 12345}
//
// 参数来源：
//   - :id：URL 路径参数，知文 ID。
//   - Body：OSS 对象的元数据（对象键、ETag、SHA256、大小）。
func (h *KnowPostHandler) ConfirmContent(c *gin.Context) {
	userID, ok := requireUserID(c)
	if !ok {
		return
	}
	id, ok := parseKnowPostID(c)
	if !ok {
		return
	}

	var req KnowPostContentConfirmRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		response.Fail(c, 400, "invalid request: "+err.Error())
		return
	}
	if err := h.svc.ConfirmContent(userID, id, req.ObjectKey, req.Etag, req.Sha256, req.Size); err != nil {
		response.Error(c, toAppErr(err))
		return
	}
	response.Success(c, gin.H{"success": true})
}

// UpdateMetadata 处理 PUT /knowposts/:id/metadata。
//
// 功能：接收知文元数据的部分更新请求，传递给服务层。
// 使用 PATCH 语义（只更新请求中包含的字段）。
//
// 请求：PUT /knowposts/:id/metadata
// Body：KnowPostPatchRequest，含 Title、TagID、Tags、ImgUrls、Description、Visible 等。
func (h *KnowPostHandler) UpdateMetadata(c *gin.Context) {
	userID, ok := requireUserID(c)
	if !ok {
		return
	}
	id, ok := parseKnowPostID(c)
	if !ok {
		return
	}

	var req KnowPostPatchRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		response.Fail(c, 400, "invalid request: "+err.Error())
		return
	}
	if err := h.svc.UpdateMetadata(userID, id, &req); err != nil {
		response.Error(c, toAppErr(err))
		return
	}
	response.Success(c, gin.H{"success": true})
}

// Publish 处理 POST /knowposts/:id/publish。
//
// 功能：将指定知文从草稿状态发布为已发布状态。
//
// 请求：POST /knowposts/:id/publish（无需请求体）。
//
// 边界情况：
//   - 知文不存在、非草稿状态或无权操作：返回 404 给客户端（经由 toAppErr 转换）。
func (h *KnowPostHandler) Publish(c *gin.Context) {
	userID, ok := requireUserID(c)
	if !ok {
		return
	}
	id, ok := parseKnowPostID(c)
	if !ok {
		return
	}

	if err := h.svc.Publish(userID, id); err != nil {
		response.Error(c, toAppErr(err))
		return
	}
	response.Success(c, gin.H{"success": true})
}

// UpdateTop 处理 PUT /knowposts/:id/top。
//
// 功能：切换知文的置顶状态。
//
// 请求：PUT /knowposts/:id/top
// Body：{"isTop": true} 或 {"isTop": false}
func (h *KnowPostHandler) UpdateTop(c *gin.Context) {
	userID, ok := requireUserID(c)
	if !ok {
		return
	}
	id, ok := parseKnowPostID(c)
	if !ok {
		return
	}

	var req KnowPostTopPatchRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		response.Fail(c, 400, "invalid request: "+err.Error())
		return
	}
	if err := h.svc.UpdateTop(userID, id, req.IsTop); err != nil {
		response.Error(c, toAppErr(err))
		return
	}
	response.Success(c, gin.H{"success": true})
}

// UpdateVisibility 处理 PUT /knowposts/:id/visibility。
//
// 功能：更新知文的可见性设置。
//
// 请求：PUT /knowposts/:id/visibility
// Body：{"visible": "public"}，可见性值由 isValidVisible 校验。
func (h *KnowPostHandler) UpdateVisibility(c *gin.Context) {
	userID, ok := requireUserID(c)
	if !ok {
		return
	}
	id, ok := parseKnowPostID(c)
	if !ok {
		return
	}

	var req KnowPostVisibilityPatchRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		response.Fail(c, 400, "invalid request: "+err.Error())
		return
	}
	if err := h.svc.UpdateVisibility(userID, id, req.Visible); err != nil {
		response.Error(c, toAppErr(err))
		return
	}
	response.Success(c, gin.H{"success": true})
}

// Delete 处理 DELETE /knowposts/:id。
//
// 功能：对指定知文执行软删除。
//
// 请求：DELETE /knowposts/:id（无需请求体）。
//
// 边界情况：
//   - 知文已被删除或不存在：返回 404。
func (h *KnowPostHandler) Delete(c *gin.Context) {
	userID, ok := requireUserID(c)
	if !ok {
		return
	}
	id, ok := parseKnowPostID(c)
	if !ok {
		return
	}

	if err := h.svc.Delete(userID, id); err != nil {
		response.Error(c, toAppErr(err))
		return
	}
	response.Success(c, gin.H{"success": true})
}
