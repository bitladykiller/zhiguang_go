package profile

import (
	"strconv"

	"github.com/gin-gonic/gin"
	"github.com/zhiguang/app/pkg/errcode"
	"github.com/zhiguang/app/pkg/middleware"
	"github.com/zhiguang/app/pkg/response"
)

// ProfileHandler 负责处理资料相关接口。
type ProfileHandler struct {
	svc ProfileServiceInterface
}

// NewProfileHandler 创建资料 HTTP 处理器。
//
// 参数:
//   - svc: 资料服务实例，用于查询和更新用户资料
//
// 返回值:
//   - *ProfileHandler: 处理器实例
func NewProfileHandler(svc ProfileServiceInterface) *ProfileHandler {
	return &ProfileHandler{svc: svc}
}

// RegisterRoutes 在给定的路由组下注册资料相关的 HTTP 接口。
//
// 参数:
//   - r: Gin 路由组（通常是 /api/v1 下的子路由组）
//
// 注册的端点:
//   - GET /profiles/:id:   查询用户完整资料
//   - PATCH /profiles/:id: 更新用户资料（仅本人可操作）
func (h *ProfileHandler) RegisterRoutes(r *gin.RouterGroup) {
	prof := r.Group("/profiles")
	{
		prof.GET("/:id", h.GetProfile)
		prof.PATCH("/:id", h.UpdateProfile)
	}
}

// GetProfile 处理 GET /profiles/:id，根据路径中的用户 ID 返回完整资料。
//
// 请求参数:
//   - id (路径参数): 用户 ID
//
// 响应:
//   - 成功: HTTP 200 + auth.User JSON（含昵称、头像等公开字段，不含密码哈希）
//   - 失败: HTTP 400（ID 格式错误）、HTTP 404（用户不存在）
//
// 说明:
//   资料查询接口不限制访问权限，任何用户都可以查看其他用户的公开资料。
//  auth.User 结构体中的 PasswordHash 字段 json tag 为 "-"，在序列化时自动排除。
//
// 边界情况:
//   - id 非数值时返回 400
//   - 用户不存在时 service 层返回 ErrNotFound → 响应 HTTP 404
func (h *ProfileHandler) GetProfile(c *gin.Context) {
	id, err := strconv.ParseUint(c.Param("id"), 10, 64)
	if err != nil {
		response.Error(c, errcode.ErrBadRequest.WithMsg("invalid id"))
		return
	}

	user, appErr := h.svc.GetProfile(c.Request.Context(), id)
	if appErr != nil {
		response.Error(c, appErr)
		return
	}

	response.Success(c, user)
}

// UpdateProfile 处理 PATCH /profiles/:id，更新用户资料的部分字段。
//
// 请求参数:
//   - id (路径参数): 要更新的用户 ID
//
// 请求体 (JSON):
//   - nickname: 昵称（可选）
//   - avatar: 头像 URL（可选）
//   - bio: 个人简介（可选）
//   - gender: 性别（可选）
//   - birthday: 生日（可选）
//   - school: 学校（可选）
//   - tags_json: 标签 JSON（可选）
//
// 响应:
//   - 成功: HTTP 200 + { success: true }
//   - 失败: HTTP 401（未登录）、HTTP 403（无权修改他人资料）、
//           HTTP 400（无更新字段）、HTTP 500（更新失败）
//
// 鉴权流程:
//   1. 从 JWT 上下文中获取当前登录用户 ID
//   2. 比较 callerID 和 targetID：不同则返回 403
//   3. handler 层不做完整权限校验（由 service 层在 callerID != targetID 时返回 ErrForbidden）
//
// 边界情况:
//   - 未携带 JWT Token → middleware 未设置 userID → 401
//   - callerID != id → 403（不能修改他人的资料）
//   - 请求体为空或所有字段为 nil → 400（没有需要更新的字段）
//   - 数据库更新失败 → 500（内部错误）
func (h *ProfileHandler) UpdateProfile(c *gin.Context) {
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

	if userID != id {
		response.Error(c, errcode.ErrForbidden)
		return
	}

	var req ProfilePatchRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		response.Error(c, errcode.ErrBadRequest.WithMsg("invalid request"))
		return
	}

	if appErr := h.svc.UpdateProfile(c.Request.Context(), userID, id, &req); appErr != nil {
		response.Error(c, appErr)
		return
	}

	response.Success(c, gin.H{"success": true})
}
