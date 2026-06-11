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
	svc CounterUseCase
}

func NewCounterHandler(svc CounterUseCase) *CounterHandler {
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

// Like 处理 POST /counter/like 请求。
//
// 功能：
//
//	为当前认证用户对指定实体打开点赞状态。
//
// 请求体（JSON）：
//   - entity_type: string, 必须 — 实体类型
//   - entity_id:   string, 必须 — 实体 ID
//
// 响应：
//   - 成功 200: {"code": 200, "message": "ok", "data": {"success": true, "changed": bool}}
//     changed=true 表示状态从未点赞变为已点赞；changed=false 表示重复点赞（已存在相同状态）
//   - 401: 未提供或无效的 Authorization Header
//   - 400: 请求体格式错误
//   - 500: 服务端错误（Redis 操作失败）
//
// 函数调用说明：
//   - middleware.GetUserID(c): 从 Gin 上下文中提取已认证的用户 ID（由 AuthMiddleware 注入）
//   - c.ShouldBindJSON(&req): Gin 提供的 JSON 请求体绑定，自动解析并校验字段
//   - response.Success / response.Error / response.Fail: 统一响应格式工具函数
//
// 权限：要求登录（需先经过 AuthMiddleware 鉴权）
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

// Unlike 处理 POST /counter/unlike 请求。
//
// 功能：
//
//	为当前认证用户取消对指定实体的点赞状态。
//
// 请求体与响应格式同 Like 接口，但操作方向相反。
//
//	changed=true 表示状态从已点赞变为未点赞。
//
// 权限：要求登录。
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

// Fav 处理 POST /counter/fav 请求。
//
// 功能：
//
//	为当前认证用户对指定实体打开收藏状态。
//
// 请求体与响应格式同 Like 接口。
//
//	changed=true 表示状态从未收藏变为已收藏。
//
// 权限：要求登录。
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

// Unfav 处理 POST /counter/unfav 请求。
//
// 功能：
//
//	为当前认证用户取消对指定实体的收藏状态。
//
// 请求体与响应格式同 Like 接口，但操作方向相反。
//
//	changed=true 表示状态从已收藏变为未收藏。
//
// 权限：要求登录。
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

// GetCounts 处理 GET /counter/counts 请求。
//
// 功能：
//
//	返回指定实体的点赞数和收藏数（以及其他请求的计数值）。
//
// 查询参数说明：
//   - entity_type: string, 必须 — 实体类型
//   - entity_id:   string, 必须 — 实体 ID
//   - metrics:     string, 可选 — 逗号分隔的指标名称列表，默认 "like,fav"
//
// 响应格式：
//
//	成功 200: {"code": 200, "message": "ok", "data": {"like": 42, "fav": 10}}
//
// 函数调用说明：
//   - c.Query("name"): Gin 中获取单个 URL 查询参数
//   - c.DefaultQuery("name", "default"): 类似 Query，但参数缺失时返回默认值
//   - strings.Split(str, ","): 标准库字符串分割，将 "like,fav" 转为 ["like", "fav"]
//
// 边界情况：
//   - entity_type 或 entity_id 为空 → 返回 400 错误
//   - metrics 参数缺失 → 按默认值 "like,fav" 查询
//   - 传入无效的指标名称 → 在响应中被忽略（不会报错）
//   - SDS 重建失败 → 对应计数值返回 0（静默降级）
//
// 权限：不要求登录（公开接口）
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

// Status 处理 GET /counter/status 请求。
//
// 功能：
//
//	返回当前登录用户对指定实体的点赞和收藏状态（是否已点/已收）。
//	这是一个用户维度的查询接口，与 GetCounts 的实体总维度不同。
//
// 查询参数说明：
//   - entity_type: string, 必须 — 实体类型
//   - entity_id:   string, 必须 — 实体 ID
//
// 响应格式：
//
//	成功 200: {"code": 200, "message": "ok", "data": {"is_liked": true, "is_faved": false}}
//
// 函数调用说明：
//   - h.svc.IsLiked() / h.svc.IsFaved():
//     直接读取 Redis 位图（GETBIT），不走 SDS 缓存。
//     保证状态读的是最新的位图值，具有强实时性。
//   - 返回值中的错误被静默忽略（_ 接收），
//     即使 Redis 临时不可用也返回 false 而非 500 错误。
//
// 边界情况：
//   - entity_type 或 entity_id 为空 → 返回 400 错误
//   - Redis 操作失败 → is_liked 和 is_faved 均为 false（乐观降级）
//
// 权限：要求登录（需要知道当前用户是谁）
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
