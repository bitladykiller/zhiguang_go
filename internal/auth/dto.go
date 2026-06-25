package auth

import "time"

// —— 请求 DTO ——

// SendCodeRequest 是 `POST /auth/send-code` 的请求体。
//
// 字段说明：
//   - Identifier：用户标识，手机号或邮箱地址。
//   - IdentifierType：标识类型（PHONE / EMAIL），决定验证码的发送渠道。
//   - Scene：验证码使用场景（REGISTER / LOGIN / RESET_PASSWORD），
//     不同场景使用不同 Redis 键前缀，互不干扰。
type SendCodeRequest struct {
	Identifier     string            `json:"identifier" binding:"required"`
	IdentifierType IdentifierType    `json:"identifier_type" binding:"required"`
	Scene          VerificationScene `json:"scene" binding:"required"`
}

// SendCodeResponse 是 `POST /auth/send-code` 的响应体。
// 包含 ExpireSeconds 以便前端展示验证码有效期倒计时。
type SendCodeResponse struct {
	Identifier    string            `json:"identifier"`
	Scene         VerificationScene `json:"scene"`
	ExpireSeconds int               `json:"expire_seconds"`
}

// RegisterRequest 是 `POST /auth/register` 的请求体。
// Code 为必填字段（验证码）；Password 可选，不传时表示后续可通过验证码登录。
// AgreeTerms 必须为 true，否则注册会被拒绝。
type RegisterRequest struct {
	Identifier     string         `json:"identifier" binding:"required"`
	IdentifierType IdentifierType `json:"identifier_type" binding:"required"`
	Code           string         `json:"code" binding:"required"`
	Password       string         `json:"password"`
	AgreeTerms     bool           `json:"agree_terms"`
}

// LoginRequest 是 `POST /auth/login` 的请求体。
// 支持两种登录策略：
//   - 密码登录：传入 Password，不使用 Code。
//   - 验证码登录：传入 Code，不使用 Password。
type LoginRequest struct {
	Identifier     string         `json:"identifier" binding:"required"`
	IdentifierType IdentifierType `json:"identifier_type" binding:"required"`
	Password       string         `json:"password"`
	Code           string         `json:"code"`
}

// TokenRefreshRequest 是 `POST /auth/refresh` 与 `POST /auth/logout` 的请求体。
type TokenRefreshRequest struct {
	RefreshToken string `json:"refresh_token" binding:"required"`
}

// PasswordResetRequest 是 `POST /auth/reset-password` 的请求体。
type PasswordResetRequest struct {
	Identifier     string         `json:"identifier" binding:"required"`
	IdentifierType IdentifierType `json:"identifier_type" binding:"required"`
	Code           string         `json:"code" binding:"required"`
	NewPassword    string         `json:"new_password" binding:"required"`
}

// —— 响应 DTO ——

// AuthUserResponse 是鉴权接口中返回的用户资料结构。
// 指针字段表示数据库中允许为 NULL 的可选信息（如头像、学校、个人签名等）。
type AuthUserResponse struct {
	ID       uint64     `json:"id"`
	Nickname string     `json:"nickname"`
	Avatar   *string    `json:"avatar,omitempty"`
	Phone    *string    `json:"phone,omitempty"`
	ZgID     *string    `json:"zg_id,omitempty"`
	Birthday *time.Time `json:"birthday,omitempty"`
	School   *string    `json:"school,omitempty"`
	Bio      *string    `json:"bio,omitempty"`
	Gender   *string    `json:"gender,omitempty"`
	TagsJson *string    `json:"tags_json,omitempty"`
}

// TokenResponse 是鉴权接口中返回的令牌数据结构。
// AccessToken 短期有效（默认 15 分钟），RefreshToken 长期有效（默认 7 天）。
type TokenResponse struct {
	AccessToken           string    `json:"access_token"`
	AccessTokenExpiresAt  time.Time `json:"access_token_expires_at"`
	RefreshToken          string    `json:"refresh_token"`
	RefreshTokenExpiresAt time.Time `json:"refresh_token_expires_at"`
}

// AuthResponse 是注册、登录、刷新令牌接口使用的顶层响应结构。
type AuthResponse struct {
	User  AuthUserResponse `json:"user"`
	Token TokenResponse    `json:"token"`
}
