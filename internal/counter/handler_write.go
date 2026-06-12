package counter

import (
	"context"

	"github.com/gin-gonic/gin"
	"github.com/zhiguang/app/pkg/response"
)

type toggleAction func(ctx context.Context, userID uint64, entityType, entityID string) (bool, error)

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
	h.handleToggle(c, h.svc.Like)
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
	h.handleToggle(c, h.svc.Unlike)
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
	h.handleToggle(c, h.svc.Fav)
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
	h.handleToggle(c, h.svc.Unfav)
}

func (h *CounterHandler) handleToggle(c *gin.Context, action toggleAction) {
	userID, ok := requireUserID(c)
	if !ok {
		return
	}

	req, ok := bindToggleRequest(c)
	if !ok {
		return
	}

	changed, err := action(c.Request.Context(), userID, req.EntityType, req.EntityID)
	if err != nil {
		response.Fail(c, 500, err.Error())
		return
	}
	response.Success(c, gin.H{"success": true, "changed": changed})
}
