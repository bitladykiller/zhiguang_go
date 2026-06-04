package counter

import (
	"strings"

	"github.com/gin-gonic/gin"
	"github.com/zhiguang/app/pkg/errcode"
	"github.com/zhiguang/app/pkg/middleware"
	"github.com/zhiguang/app/pkg/response"
)

// CounterHandler 暴露计数器模块的 HTTP 接口。
type CounterHandler struct {
	svc *CounterService
}

func NewCounterHandler(svc *CounterService) *CounterHandler {
	return &CounterHandler{svc: svc}
}

// RegisterRoutes 注册计数器模块路由，所有接口都要求登录。
func (h *CounterHandler) RegisterRoutes(r *gin.RouterGroup) {
	ctr := r.Group("/counter")
	{
		ctr.POST("/like", h.Like)
		ctr.POST("/unlike", h.Unlike)
		ctr.POST("/fav", h.Fav)
		ctr.POST("/unfav", h.Unfav)
		ctr.GET("/counts", h.GetCounts)
		ctr.GET("/status", h.Status)
	}
}

// Like 处理 `POST /counter/like`。
func (h *CounterHandler) Like(c *gin.Context) {
	userID, ok := middleware.GetUserID(c)
	if !ok {
		response.Error(c, errcode.ErrUnauthorized)
		return
	}
	var req ToggleRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		response.Fail(c, 400, "invalid request")
		return
	}
	changed, err := h.svc.Like(c.Request.Context(), userID, req.EntityType, req.EntityID)
	if err != nil {
		response.Fail(c, 500, err.Error())
		return
	}
	response.Success(c, gin.H{"success": true, "changed": changed})
}

// Unlike 处理 `POST /counter/unlike`。
func (h *CounterHandler) Unlike(c *gin.Context) {
	userID, ok := middleware.GetUserID(c)
	if !ok {
		response.Error(c, errcode.ErrUnauthorized)
		return
	}
	var req ToggleRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		response.Fail(c, 400, "invalid request")
		return
	}
	changed, err := h.svc.Unlike(c.Request.Context(), userID, req.EntityType, req.EntityID)
	if err != nil {
		response.Fail(c, 500, err.Error())
		return
	}
	response.Success(c, gin.H{"success": true, "changed": changed})
}

// Fav 处理 `POST /counter/fav`。
func (h *CounterHandler) Fav(c *gin.Context) {
	userID, ok := middleware.GetUserID(c)
	if !ok {
		response.Error(c, errcode.ErrUnauthorized)
		return
	}
	var req ToggleRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		response.Fail(c, 400, "invalid request")
		return
	}
	changed, err := h.svc.Fav(c.Request.Context(), userID, req.EntityType, req.EntityID)
	if err != nil {
		response.Fail(c, 500, err.Error())
		return
	}
	response.Success(c, gin.H{"success": true, "changed": changed})
}

// Unfav 处理 `POST /counter/unfav`。
func (h *CounterHandler) Unfav(c *gin.Context) {
	userID, ok := middleware.GetUserID(c)
	if !ok {
		response.Error(c, errcode.ErrUnauthorized)
		return
	}
	var req ToggleRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		response.Fail(c, 400, "invalid request")
		return
	}
	changed, err := h.svc.Unfav(c.Request.Context(), userID, req.EntityType, req.EntityID)
	if err != nil {
		response.Fail(c, 500, err.Error())
		return
	}
	response.Success(c, gin.H{"success": true, "changed": changed})
}

// GetCounts 处理 `GET /counter/counts?entity_type=x&entity_id=y&metrics=like,fav`。
func (h *CounterHandler) GetCounts(c *gin.Context) {
	entityType := c.Query("entity_type")
	entityID := c.Query("entity_id")
	metricsStr := c.DefaultQuery("metrics", "like,fav")

	if entityType == "" || entityID == "" {
		response.Fail(c, 400, "entity_type and entity_id are required")
		return
	}

	metrics := strings.Split(metricsStr, ",")
	counts, err := h.svc.GetCounts(c.Request.Context(), entityType, entityID, metrics)
	if err != nil {
		response.Fail(c, 500, err.Error())
		return
	}
	response.Success(c, gin.H{"data": counts})
}

// Status 处理 `GET /counter/status?entity_type=x&entity_id=y`。
func (h *CounterHandler) Status(c *gin.Context) {
	userID, ok := middleware.GetUserID(c)
	if !ok {
		response.Error(c, errcode.ErrUnauthorized)
		return
	}
	entityType := c.Query("entity_type")
	entityID := c.Query("entity_id")
	if entityType == "" || entityID == "" {
		response.Fail(c, 400, "entity_type and entity_id are required")
		return
	}

	liked, _ := h.svc.IsLiked(c.Request.Context(), userID, entityType, entityID)
	faved, _ := h.svc.IsFaved(c.Request.Context(), userID, entityType, entityID)
	response.Success(c, gin.H{"is_liked": liked, "is_faved": faved})
}
