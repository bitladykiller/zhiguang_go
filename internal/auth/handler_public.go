package auth

import (
	"github.com/gin-gonic/gin"
	"github.com/zhiguang/app/pkg/response"
)

// SendCode 处理验证码发送请求（POST /auth/send-code）。
//
// 步骤：
//  1. 使用 c.ShouldBindJSON 将请求 JSON Body 绑定到 SendCodeRequest 结构体
//  2. 调用 AuthService.SendCode 执行业务逻辑
//  3. 根据结果返回成功响应或业务错误
//
// Gin 绑定（ShouldBindJSON）说明：
//   - 从 HTTP 请求 Body 读取 JSON，按结构体 tag（`json:"xxx"`）映射到字段
//   - 自动校验 Content-Type 是否为 application/json
//   - 绑定失败返回 error 后直接返回 400 + 错误详情
//   - 支持结构体嵌套、忽略空值、类型转换等
//
// 参数:
//   - c: Gin 上下文，包含 HTTP 请求/响应信息
//
// 返回值: 无（结果通过 response 包写入 HTTP 响应体）
//
// 异常处理:
//   - JSON 绑定失败 -> HTTP 400 + 错误详情
//   - 业务逻辑失败（如超过每日上限） -> 对应业务错误码
func (h *AuthHandler) SendCode(c *gin.Context) {
	var req SendCodeRequest
	if !bindJSON(c, &req) {
		return
	}

	data, appErr := h.svc.SendCode(c.Request.Context(), &req)
	if appErr != nil {
		response.Error(c, appErr)
		return
	}
	response.Success(c, data)
}

// Register 处理用户注册请求（POST /auth/register）。
//
// 完整注册流程：
//  1. 绑定请求 JSON 到 RegisterRequest 结构体（包含验证码、密码等）
//  2. 通过 extractClientInfo(c) 提取客户端 IP 和 User-Agent（用于审计日志）
//  3. 调用 AuthService.Register 执行注册逻辑（验证码校验 -> 创建用户 -> 颁发令牌）
//  4. 成功返回 201 Created + 用户信息和令牌对
//
// 参数:
//   - c: Gin 上下文
//
// 返回值: 无
//
// 异常处理:
//   - JSON 绑定失败 -> HTTP 400 + 错误详情
//   - 验证码错误 -> 对应业务错误码
//   - 手机号/邮箱已注册 -> 对应业务错误码
func (h *AuthHandler) Register(c *gin.Context) {
	var req RegisterRequest
	if !bindJSON(c, &req) {
		return
	}

	data, appErr := h.svc.Register(c.Request.Context(), &req, extractClientInfo(c))
	if appErr != nil {
		response.Error(c, appErr)
		return
	}
	response.Created(c, data)
}

// Login 处理用户登录请求（POST /auth/login）。
//
// 登录流程：
//  1. 绑定请求 JSON 到 LoginRequest 结构体（包含标识、密码、标识类型）
//  2. 通过 extractClientInfo(c) 提取客户端信息用于审计日志
//  3. 调用 AuthService.Login 校验密码并颁发 access token + refresh token
//  4. 记录登录审计日志（成功/失败均记录）
//
// 参数:
//   - c: Gin 上下文
//
// 返回值: 无
//
// 异常处理:
//   - JSON 绑定失败 -> HTTP 400
//   - 密码错误/用户不存在 -> 对应业务错误码（不暴露具体哪个错了，防枚举）
func (h *AuthHandler) Login(c *gin.Context) {
	var req LoginRequest
	if !bindJSON(c, &req) {
		return
	}

	data, appErr := h.svc.Login(c.Request.Context(), &req, extractClientInfo(c))
	if appErr != nil {
		response.Error(c, appErr)
		return
	}
	response.Success(c, data)
}

// Refresh 处理令牌刷新请求（POST /auth/refresh）。
//
// 令牌轮换流程：
//  1. 绑定请求 JSON 到 TokenRefreshRequest（包含 refresh_token 字段）
//  2. 调用 AuthService.Refresh 执行刷新逻辑：
//     a. 校验 refresh token 是否在 Redis 白名单中
//     b. 解析 refresh token 获取用户 ID
//     c. 吊销旧 refresh token（令牌轮换：一次刷新 = 一次吊销）
//     d. 生成新的 access token + refresh token
//     e. 新 refresh token 存入 Redis 白名单
//  3. 返回新令牌对
//
// 参数:
//   - c: Gin 上下文
//
// 返回值: 无
//
// 异常处理:
//   - JSON 绑定失败 -> HTTP 400
//   - refresh token 无效/已过期/已被吊销 -> 对应业务错误码
func (h *AuthHandler) Refresh(c *gin.Context) {
	var req TokenRefreshRequest
	if !bindJSON(c, &req) {
		return
	}

	data, appErr := h.svc.Refresh(c.Request.Context(), &req)
	if appErr != nil {
		response.Error(c, appErr)
		return
	}
	response.Success(c, data)
}

// Logout 处理用户登出请求（POST /auth/logout）。
//
// 登出流程：
//  1. 绑定请求 JSON 到 TokenRefreshRequest（接收 refresh_token）
//  2. 调用 AuthService.Logout 从 Redis 白名单中移除该 refresh token
//  3. 返回登出成功确认
//
// 注意：access token 仍有效直到自然过期（JWT 无状态，无法被服务端吊销）。
// 通过缩短 access token TTL（通常 15-30 分钟）将风险窗口控制在可接受范围。
// 对于安全敏感场景，应维护 access token 黑名单作为补充。
//
// 参数:
//   - c: Gin 上下文
//
// 返回值: 无（返回 {"message": "logged out"}）
//
// 异常处理:
//   - JSON 绑定失败 -> HTTP 400
//   - token 吊销内部失败不阻止成功响应（非关键路径，不影响用户感知）
func (h *AuthHandler) Logout(c *gin.Context) {
	var req TokenRefreshRequest
	if !bindJSON(c, &req) {
		return
	}

	h.svc.Logout(c.Request.Context(), &req)
	response.Success(c, gin.H{"message": "logged out"})
}

// ResetPassword 处理密码重置请求（POST /auth/reset-password）。
//
// 密码重置流程：
//  1. 绑定请求 JSON 到 PasswordResetRequest（包含标识、验证码、新密码）
//  2. 调用 AuthService.ResetPassword 执行重置：
//     a. 校验验证码（验证码正确后才能重置）
//     b. 对新密码进行 bcrypt 哈希
//     c. 更新数据库中该用户的密码哈希
//     d. 吊销该用户的所有 refresh token（RevokeAll），强制其他设备重新登录
//  3. 返回成功确认
//
// 参数:
//   - c: Gin 上下文
//
// 返回值: 无（返回 {"message": "password reset successful"}）
//
// 异常处理:
//   - JSON 绑定失败 -> HTTP 400
//   - 验证码错误/已过期 -> 对应业务错误码
//   - 用户不存在/密码更新失败 -> 对应业务错误码
func (h *AuthHandler) ResetPassword(c *gin.Context) {
	var req PasswordResetRequest
	if !bindJSON(c, &req) {
		return
	}

	if appErr := h.svc.ResetPassword(c.Request.Context(), &req); appErr != nil {
		response.Error(c, appErr)
		return
	}
	response.Success(c, gin.H{"message": "password reset successful"})
}
