package knowpost

import (
	"strconv"

	"github.com/gin-gonic/gin"
	"github.com/zhiguang/app/pkg/errcode"
	"github.com/zhiguang/app/pkg/middleware"
	"github.com/zhiguang/app/pkg/response"
)

// KnowPostHandler 暴露知文模块的 HTTP 接口，负责请求参数解析和响应组装。
type KnowPostHandler struct {
	svc     *KnowPostService
	feedSvc *KnowPostFeedService
}

// NewKnowPostHandler 创建处理器实例。
func NewKnowPostHandler(svc *KnowPostService, feedSvc *KnowPostFeedService) *KnowPostHandler {
	return &KnowPostHandler{svc: svc, feedSvc: feedSvc}
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

func (h *KnowPostHandler) CreateDraft(c *gin.Context) {
	userID, ok := middleware.GetUserID(c)
	if !ok {
		response.Error(c, errcode.ErrUnauthorized)
		return
	}
	id, err := h.svc.CreateDraft(userID)
	if err != nil {
		response.Error(c, errcode.ErrInternal.WithMsg(err.Error()))
		return
	}
	response.Created(c, gin.H{"id": strconv.FormatUint(id, 10)})
}

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
	if err := h.svc.ConfirmContent(userID, id, req.ObjectKey, req.Etag, req.Sha256, req.Size); err != nil {
		response.Error(c, toAppErr(err))
		return
	}
	response.Success(c, gin.H{"success": true})
}

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
	if err := h.svc.UpdateMetadata(userID, id, &req); err != nil {
		response.Error(c, toAppErr(err))
		return
	}
	response.Success(c, gin.H{"success": true})
}

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
	if err := h.svc.Publish(userID, id); err != nil {
		response.Error(c, toAppErr(err))
		return
	}
	response.Success(c, gin.H{"success": true})
}

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
	if err := h.svc.UpdateTop(userID, id, req.IsTop); err != nil {
		response.Error(c, toAppErr(err))
		return
	}
	response.Success(c, gin.H{"success": true})
}

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
	if err := h.svc.UpdateVisibility(userID, id, req.Visible); err != nil {
		response.Error(c, toAppErr(err))
		return
	}
	response.Success(c, gin.H{"success": true})
}

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
	if err := h.svc.Delete(userID, id); err != nil {
		response.Error(c, toAppErr(err))
		return
	}
	response.Success(c, gin.H{"success": true})
}

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
	resp, err := h.svc.GetDetail(id, userID)
	if err != nil {
		response.Error(c, toAppErr(err))
		return
	}
	response.Success(c, resp)
}

func (h *KnowPostHandler) GetPublicFeed(c *gin.Context) {
	page := queryInt(c, "page", 1)
	size := queryInt(c, "size", 20)
	var userID *uint64
	if uid, ok := middleware.GetUserID(c); ok {
		userID = &uid
	}
	resp, err := h.feedSvc.GetPublicFeed(page, size, userID)
	if err != nil {
		response.Error(c, errcode.ErrInternal.WithMsg(err.Error()))
		return
	}
	response.Success(c, resp)
}

func (h *KnowPostHandler) GetMyPublished(c *gin.Context) {
	userID, ok := middleware.GetUserID(c)
	if !ok {
		response.Error(c, errcode.ErrUnauthorized)
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

// --- [辅助函数] ---

func queryInt(c *gin.Context, key string, defaultVal int) int {
	s := c.Query(key)
	if s == "" {
		return defaultVal
	}
	v, err := strconv.Atoi(s)
	if err != nil {
		return defaultVal
	}
	return v
}

func toAppErr(err error) *errcode.AppError {
	if appErr, ok := err.(*errcode.AppError); ok {
		return appErr
	}
	return errcode.ErrInternal.WithMsg(err.Error())
}
