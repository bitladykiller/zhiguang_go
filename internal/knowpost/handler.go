package knowpost

import (
	"context"
	"strconv"

	"github.com/gin-gonic/gin"
	"github.com/zhiguang/app/pkg/errcode"
	"github.com/zhiguang/app/pkg/httputil"
	"github.com/zhiguang/app/pkg/middleware"
	"github.com/zhiguang/app/pkg/response"
)

// KnowPostWriteService 定义知文写操作对外暴露的业务方法。
//
// Handler 依赖此接口而非具体 *KnowPostService，使得 handler 可以独立于
// service 实现进行单元测试。
type KnowPostWriteService interface {
	CreateDraft(ctx context.Context, creatorID uint64, idempotencyKey string) (uint64, error)
	ConfirmContent(ctx context.Context, creatorID, id uint64, objectKey, etag, sha256 string, size uint64) error
	UpdateMetadata(ctx context.Context, creatorID, id uint64, req *KnowPostPatchRequest) error
	Publish(ctx context.Context, creatorID, id uint64) error
	UpdateTop(ctx context.Context, creatorID, id uint64, isTop bool) error
	UpdateVisibility(ctx context.Context, creatorID, id uint64, visible KnowPostVisibility) error
	Delete(ctx context.Context, creatorID, id uint64) error
}

// KnowPostReadService 定义知文读操作对外暴露的业务方法。
type KnowPostReadService interface {
	GetDetail(ctx context.Context, id uint64, currentUserID *uint64) (*KnowPostDetailResponse, error)
}

// KnowPostFeedServiceInterface 定义 Feed 流读操作对外暴露的业务方法。
type KnowPostFeedServiceInterface interface {
	GetPublicFeed(ctx context.Context, page, size int, currentUserID *uint64) (*FeedPageResponse, error)
	GetMyPublished(ctx context.Context, userID uint64, page, size int) (*FeedPageResponse, error)
	GetMineFeed(ctx context.Context, userID uint64, page, size int) (*FeedPageResponse, error)
}

// 编译期断言。
var (
	_ KnowPostWriteService          = (*KnowPostService)(nil)
	_ KnowPostReadService           = (*KnowPostService)(nil)
	_ KnowPostFeedServiceInterface = (*KnowPostFeedService)(nil)
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
//     /:id（详情）、/feed/public（公共 feed）
//     /feed/mine（我的已发布，需要登录）
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
	userID, ok := middleware.GetUserID(c)
	if !ok {
		response.Error(c, errcode.ErrUnauthorized)
		return
	}
	idempotencyKey := c.GetHeader("X-Idempotency-Key")
	id, err := h.svc.CreateDraft(c.Request.Context(), userID, idempotencyKey)
	if err != nil {
		middleware.RecordError(c, err)
		response.Error(c, httputil.ToAppError(err))
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
	userID, ok := middleware.GetUserID(c)
	if !ok {
		response.Error(c, errcode.ErrUnauthorized)
		return
	}
	id, err := strconv.ParseUint(c.Param("id"), 10, 64)
	if err != nil {
		response.Error(c, errcode.ErrBadRequest.WithMsg("invalid id"))
		return
	}
	var req KnowPostContentConfirmRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		response.Error(c, errcode.ErrBadRequest.WithMsg("invalid request"))
		return
	}
	if err := h.svc.ConfirmContent(c.Request.Context(), userID, id, req.ObjectKey, req.Etag, req.Sha256, req.Size); err != nil {
		middleware.RecordError(c, err)
		response.Error(c, httputil.ToAppError(err))
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
	userID, ok := middleware.GetUserID(c)
	if !ok {
		response.Error(c, errcode.ErrUnauthorized)
		return
	}
	id, err := strconv.ParseUint(c.Param("id"), 10, 64)
	if err != nil {
		response.Error(c, errcode.ErrBadRequest.WithMsg("invalid id"))
		return
	}
	var req KnowPostPatchRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		response.Error(c, errcode.ErrBadRequest.WithMsg("invalid request"))
		return
	}
	if err := h.svc.UpdateMetadata(c.Request.Context(), userID, id, &req); err != nil {
		middleware.RecordError(c, err)
		response.Error(c, httputil.ToAppError(err))
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
	userID, ok := middleware.GetUserID(c)
	if !ok {
		response.Error(c, errcode.ErrUnauthorized)
		return
	}
	id, err := strconv.ParseUint(c.Param("id"), 10, 64)
	if err != nil {
		response.Error(c, errcode.ErrBadRequest.WithMsg("invalid id"))
		return
	}
	if err := h.svc.Publish(c.Request.Context(), userID, id); err != nil {
		middleware.RecordError(c, err)
		response.Error(c, httputil.ToAppError(err))
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
	userID, ok := middleware.GetUserID(c)
	if !ok {
		response.Error(c, errcode.ErrUnauthorized)
		return
	}
	id, err := strconv.ParseUint(c.Param("id"), 10, 64)
	if err != nil {
		response.Error(c, errcode.ErrBadRequest.WithMsg("invalid id"))
		return
	}
	var req KnowPostTopPatchRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		response.Error(c, errcode.ErrBadRequest.WithMsg("invalid request"))
		return
	}
	if err := h.svc.UpdateTop(c.Request.Context(), userID, id, req.IsTop); err != nil {
		middleware.RecordError(c, err)
		response.Error(c, httputil.ToAppError(err))
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
	userID, ok := middleware.GetUserID(c)
	if !ok {
		response.Error(c, errcode.ErrUnauthorized)
		return
	}
	id, err := strconv.ParseUint(c.Param("id"), 10, 64)
	if err != nil {
		response.Error(c, errcode.ErrBadRequest.WithMsg("invalid id"))
		return
	}
	var req KnowPostVisibilityPatchRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		response.Error(c, errcode.ErrBadRequest.WithMsg("invalid request"))
		return
	}
	if err := h.svc.UpdateVisibility(c.Request.Context(), userID, id, req.Visible); err != nil {
		middleware.RecordError(c, err)
		response.Error(c, httputil.ToAppError(err))
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
	userID, ok := middleware.GetUserID(c)
	if !ok {
		response.Error(c, errcode.ErrUnauthorized)
		return
	}
	id, err := strconv.ParseUint(c.Param("id"), 10, 64)
	if err != nil {
		response.Error(c, errcode.ErrBadRequest.WithMsg("invalid id"))
		return
	}
	if err := h.svc.Delete(c.Request.Context(), userID, id); err != nil {
		middleware.RecordError(c, err)
		response.Error(c, httputil.ToAppError(err))
		return
	}
	response.Success(c, gin.H{"success": true})
}

// GetDetail 处理 GET /knowposts/:id。
//
// 功能：返回知文详情。当前用户可登录也可不登录。
// 登录用户会额外获得点赞/收藏状态。
//
// 请求：GET /knowposts/:id
func (h *KnowPostHandler) GetDetail(c *gin.Context) {
	id, err := strconv.ParseUint(c.Param("id"), 10, 64)
	if err != nil {
		response.Error(c, errcode.ErrBadRequest.WithMsg("invalid id"))
		return
	}
	var userID *uint64
	if uid, ok := middleware.GetUserID(c); ok {
		userID = &uid
	}
	resp, err := h.readSvc.GetDetail(c.Request.Context(), id, userID)
	if err != nil {
		middleware.RecordError(c, err)
		response.Error(c, httputil.ToAppError(err))
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
	page := httputil.QueryInt(c, "page", 1)
	size := httputil.QueryInt(c, "size", 20)
	var userID *uint64
	if uid, ok := middleware.GetUserID(c); ok {
		userID = &uid
	}
	resp, err := h.feedSvc.GetPublicFeed(c.Request.Context(), page, size, userID)
	if err != nil {
		middleware.RecordError(c, err)
		response.Error(c, httputil.ToAppError(err))
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
	userID, ok := middleware.GetUserID(c)
	if !ok {
		response.Error(c, errcode.ErrUnauthorized)
		return
	}
	page := httputil.QueryInt(c, "page", 1)
	size := httputil.QueryInt(c, "size", 20)
	resp, err := h.feedSvc.GetMineFeed(c.Request.Context(), userID, page, size)
	if err != nil {
		middleware.RecordError(c, err)
		response.Error(c, httputil.ToAppError(err))
		return
	}
	response.Success(c, resp)
}
