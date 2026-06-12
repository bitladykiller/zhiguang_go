package auth

import (
	"github.com/gin-gonic/gin"
	"github.com/zhiguang/app/pkg/response"
)

// Me 获取当前登录用户的个人信息（GET /auth/me）。
//
// 该路由受 middleware.AuthMiddleware 保护（见 RegisterRoutes），
// 未携带有效 access token 的请求会被 401 拦截。
//
// 流程：
//  1. 通过 middleware.GetUserID(c) 从已解析的 JWT 中提取当前用户 ID
//  2. 调用 AuthService.CurrentUser 查询用户详细信息
//  3. 返回用户资料（手机号、昵称、头像、学校等）
//
// 参数:
//   - c: Gin 上下文（已通过 AuthMiddleware 注入了解析后的用户信息）
//
// 返回值: 无
//
// 异常处理:
//   - 未携带 token 或 token 无效 -> HTTP 401 Unauthorized
//   - 用户已删除（令牌有效但 db 查不到） -> 对应业务错误码
func (h *AuthHandler) Me(c *gin.Context) {
	userID, ok := currentUserID(c)
	if !ok {
		return
	}

	data, appErr := h.svc.CurrentUser(c.Request.Context(), userID)
	if appErr != nil {
		response.Error(c, appErr)
		return
	}
	response.Success(c, data)
}
