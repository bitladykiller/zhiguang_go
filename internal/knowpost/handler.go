package knowpost

import (
	"strconv"

	"github.com/gin-gonic/gin"
	"github.com/zhiguang/app/pkg/errcode"
	"github.com/zhiguang/app/pkg/httputil"
	"github.com/zhiguang/app/pkg/middleware"
	"github.com/zhiguang/app/pkg/response"
)

// KnowPostHandler 暴露知文模块的 HTTP 接口，负责请求参数解析和响应组装。
type KnowPostHandler struct {
	svc     KnowPostWriteService
	readSvc KnowPostReadService
	feedSvc KnowPostFeedServiceInterface
}

// NewKnowPostHandler 创建处理器实例。
func NewKnowPostHandler(svc KnowPostWriteService, readSvc KnowPostReadService, feedSvc KnowPostFeedServiceInterface) *KnowPostHandler {
	return &KnowPostHandler{svc: svc, readSvc: readSvc, feedSvc: feedSvc}
}

// RegisterRoutes 注册知文模块的全部路由。
//
// 路由分类：
//   - 写操作（需要 JWT 登录）：
//     /draft（创建草稿）、/:id/content（确认内容）、/:id/publish（发布）等
//   - 读操作（可选登录，使用全局 OptionalAuthMiddleware）：
//     /:id（详情）、/feed/public（公共 feed）、/feed/mine（我的已发布）
//
// 写操作在处理器内通过 middleware.GetUserID 显式鉴权。
// 读操作中 /feed/mine 也必须登录（因为 "我的" 需要知道是谁）。
func (h *KnowPostHandler) RegisterRoutes(r *gin.RouterGroup) {
	kp := r.Group("/knowposts")
	{
		// 写操作（要求登录）
		kp.POST("/draft", h.CreateDraft)
		kp.PUT("/:id/content", h.ConfirmContent)
		kp.PUT("/:id/metadata", h.UpdateMetadata)
		kp.POST("/:id/publish", h.Publish)
		kp.PUT("/:id/top", h.UpdateTop)
		kp.PUT("/:id/visibility", h.UpdateVisibility)
		kp.DELETE("/:id", h.Delete)

		// 读操作（可选登录）
		kp.GET("/:id", h.GetDetail)
		kp.GET("/feed/public", h.GetPublicFeed)
		kp.GET("/feed/mine", h.GetMyPublished)
	}
}

// --- [处理函数] ---

// CreateDraft 创建草稿（POST /knowposts/draft）。
func (h *KnowPostHandler) CreateDraft(c *gin.Context) {
	userID, ok := middleware.GetUserID(c)
	if !ok {
		response.Error(c, errcode.ErrUnauthorized)
		return
	}
	id, err := h.svc.CreateDraft(c.Request.Context(), userID)
	if err != nil {
		response.Error(c, errcode.ErrInternal.WithMsg("internal server error"))
		return
	}
	response.Created(c, gin.H{"id": strconv.FormatUint(id, 10)})
}

// ConfirmContent 确认内容上传完成（PUT /knowposts/:id/content）。
// 接收 OSS 直传后的对象元数据，更新知文内容记录。
func (h *KnowPostHandler) ConfirmContent(c *gin.Context) {
	userID, ok := middleware.GetUserID(c)
	if !ok {
		response.Error(c, errcode.ErrUnauthorized)
		return
	}
	id, err := strconv.ParseUint(c.Param("id"), 10, 64)
	if err != nil {
		response.Fail(c, 400, "invalid id")
		return
	}
	var req KnowPostContentConfirmRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		response.Fail(c, 400, "invalid request: "+err.Error())
		return
	}
	if err := h.svc.ConfirmContent(c.Request.Context(), userID, id, req.ObjectKey, req.Etag, req.Sha256, req.Size); err != nil {
		response.Error(c, toAppErr(err))
		return
	}
	response.Success(c, gin.H{"success": true})
}

// UpdateMetadata 更新元数据（PUT /knowposts/:id/metadata）。
// PATCH 语义：仅更新请求中包含的字段。
func (h *KnowPostHandler) UpdateMetadata(c *gin.Context) {
	userID, ok := middleware.GetUserID(c)
	if !ok {
		response.Error(c, errcode.ErrUnauthorized)
		return
	}
	id, err := strconv.ParseUint(c.Param("id"), 10, 64)
	if err != nil {
		response.Fail(c, 400, "invalid id")
		return
	}
	var req KnowPostPatchRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		response.Fail(c, 400, "invalid request: "+err.Error())
		return
	}
	if err := h.svc.UpdateMetadata(c.Request.Context(), userID, id, &req); err != nil {
		response.Error(c, toAppErr(err))
		return
	}
	response.Success(c, gin.H{"success": true})
}

// Publish 发布知文（POST /knowposts/:id/publish）。
func (h *KnowPostHandler) Publish(c *gin.Context) {
	userID, ok := middleware.GetUserID(c)
	if !ok {
		response.Error(c, errcode.ErrUnauthorized)
		return
	}
	id, err := strconv.ParseUint(c.Param("id"), 10, 64)
	if err != nil {
		response.Fail(c, 400, "invalid id")
		return
	}
	if err := h.svc.Publish(c.Request.Context(), userID, id); err != nil {
		response.Error(c, toAppErr(err))
		return
	}
	response.Success(c, gin.H{"success": true})
}

// UpdateTop 切换置顶状态（PUT /knowposts/:id/top）。
func (h *KnowPostHandler) UpdateTop(c *gin.Context) {
	userID, ok := middleware.GetUserID(c)
	if !ok {
		response.Error(c, errcode.ErrUnauthorized)
		return
	}
	id, err := strconv.ParseUint(c.Param("id"), 10, 64)
	if err != nil {
		response.Fail(c, 400, "invalid id")
		return
	}
	var req KnowPostTopPatchRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		response.Fail(c, 400, "invalid request: "+err.Error())
		return
	}
	if err := h.svc.UpdateTop(c.Request.Context(), userID, id, req.IsTop); err != nil {
		response.Error(c, toAppErr(err))
		return
	}
	response.Success(c, gin.H{"success": true})
}

// UpdateVisibility 更新可见性（PUT /knowposts/:id/visibility）。
func (h *KnowPostHandler) UpdateVisibility(c *gin.Context) {
	userID, ok := middleware.GetUserID(c)
	if !ok {
		response.Error(c, errcode.ErrUnauthorized)
		return
	}
	id, err := strconv.ParseUint(c.Param("id"), 10, 64)
	if err != nil {
		response.Fail(c, 400, "invalid id")
		return
	}
	var req KnowPostVisibilityPatchRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		response.Fail(c, 400, "invalid request: "+err.Error())
		return
	}
	if err := h.svc.UpdateVisibility(c.Request.Context(), userID, id, req.Visible); err != nil {
		response.Error(c, toAppErr(err))
		return
	}
	response.Success(c, gin.H{"success": true})
}

// Delete 软删除（DELETE /knowposts/:id）。
func (h *KnowPostHandler) Delete(c *gin.Context) {
	userID, ok := middleware.GetUserID(c)
	if !ok {
		response.Error(c, errcode.ErrUnauthorized)
		return
	}
	id, err := strconv.ParseUint(c.Param("id"), 10, 64)
	if err != nil {
		response.Fail(c, 400, "invalid id")
		return
	}
	if err := h.svc.Delete(c.Request.Context(), userID, id); err != nil {
		response.Error(c, toAppErr(err))
		return
	}
	response.Success(c, gin.H{"success": true})
}

// GetDetail 获取知文详情（GET /knowposts/:id）。
// 可选登录，登录用户额外获得点赞/收藏状态。
func (h *KnowPostHandler) GetDetail(c *gin.Context) {
	id, err := strconv.ParseUint(c.Param("id"), 10, 64)
	if err != nil {
		response.Fail(c, 400, "invalid id")
		return
	}
	var userID *uint64
	if uid, ok := middleware.GetUserID(c); ok {
		userID = &uid
	}
	resp, err := h.readSvc.GetDetail(c.Request.Context(), id, userID)
	if err != nil {
		response.Error(c, toAppErr(err))
		return
	}
	response.Success(c, resp)
}

// GetPublicFeed 获取公共 Feed（GET /knowposts/feed/public）。
// 可选附带当前用户的点赞/收藏状态。
func (h *KnowPostHandler) GetPublicFeed(c *gin.Context) {
	page := httputil.QueryInt(c, "page", 1)
	size := httputil.QueryInt(c, "size", 20)
	var userID *uint64
	if uid, ok := middleware.GetUserID(c); ok {
		userID = &uid
	}
	resp, err := h.feedSvc.GetPublicFeed(c.Request.Context(), page, size, userID)
	if err != nil {
		response.Error(c, errcode.ErrInternal.WithMsg("internal server error"))
		return
	}
	response.Success(c, resp)
}

// GetMyPublished 获取我的已发布列表（GET /knowposts/feed/mine）。
// 必须登录。
func (h *KnowPostHandler) GetMyPublished(c *gin.Context) {
	userID, ok := middleware.GetUserID(c)
	if !ok {
		response.Error(c, errcode.ErrUnauthorized)
		return
	}
	page := httputil.QueryInt(c, "page", 1)
	size := httputil.QueryInt(c, "size", 20)
	resp, err := h.feedSvc.GetMyPublished(c.Request.Context(), userID, page, size)
	if err != nil {
		response.Error(c, errcode.ErrInternal.WithMsg("internal server error"))
		return
	}
	response.Success(c, resp)
}

// toAppErr 将 error 统一转换为 *errcode.AppError。
// 已是 AppError 类型直接返回；其他类型包装为 ErrInternal 避免泄露内部细节。
func toAppErr(err error) *errcode.AppError {
	if appErr, ok := err.(*errcode.AppError); ok {
		return appErr
	}
	return errcode.ErrInternal
}
