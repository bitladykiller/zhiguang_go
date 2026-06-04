package auth

import "time"

// SendCodeRequest 是 `POST /auth/send-code` 的请求体。
type SendCodeRequest struct {
	Identifier     string            `json:"identifier" binding:"required"`
	IdentifierType IdentifierType    `json:"identifier_type" binding:"required"`
	Scene          VerificationScene `json:"scene" binding:"required"`
}

// SendCodeResponse 是 `POST /auth/send-code` 的响应体。
type SendCodeResponse struct {
	Identifier    string            `json:"identifier"`
	Scene         VerificationScene `json:"scene"`
	ExpireSeconds int               `json:"expire_seconds"`
}

// RegisterRequest 是 `POST /auth/register` 的请求体。
type RegisterRequest struct {
	Identifier     string         `json:"identifier" binding:"required"`
	IdentifierType IdentifierType `json:"identifier_type" binding:"required"`
	Code           string         `json:"code" binding:"required"`
	Password       string         `json:"password"`
	AgreeTerms     bool           `json:"agree_terms"`
}

// LoginRequest 是 `POST /auth/login` 的请求体。
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

// AuthUserResponse 是鉴权接口中返回的用户资料结构。
type AuthUserResponse struct {
	ID       uint64     `json:"id"`
	Nickname string     `json:"nickname"`
	Avatar   *string    `json:"avatar,omitempty"`
	Phone    *string    `json:"phone,omitempty"`
	ZgId     *string    `json:"zg_id,omitempty"`
	Birthday *time.Time `json:"birthday,omitempty"`
	School   *string    `json:"school,omitempty"`
	Bio      *string    `json:"bio,omitempty"`
	Gender   *string    `json:"gender,omitempty"`
	TagsJson *string    `json:"tags_json,omitempty"`
}

// TokenResponse 是鉴权接口中返回的令牌数据结构。
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
