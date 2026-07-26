// Package auth 实现基于 JWT 的鉴权体系，包含 RS256 签名、
// 验证码流程、刷新令牌白名单以及审计日志能力。
//
// 架构组成：
//   - JwtService：负责 RSA 密钥管理、令牌签发与校验
//   - VerificationService：基于 Redis 的验证码生成与校验
//   - AuthService：编排注册、登录、刷新令牌等业务流程
//   - RefreshTokenStore：基于 Redis 管理刷新令牌生命周期
package auth

import (
	"time"

	"github.com/zhiguang/app/internal/model"
)

// User 映射到 users 表，类型别名指向共享模型。
// PasswordHash 出于安全考虑不会参与 JSON 序列化（model.User 中已标记 json:"-"）。
type User = model.User

// LoginLog 映射到登录审计表 login_logs。
type LoginLog struct {
	ID         uint64    `db:"id" json:"id"`
	UserID     *uint64   `db:"user_id" json:"user_id,omitempty"`
	Identifier string    `db:"identifier" json:"identifier"`
	Channel    string    `db:"channel" json:"channel"`
	IP         *string   `db:"ip" json:"ip,omitempty"`
	UserAgent  *string   `db:"user_agent" json:"user_agent,omitempty"`
	Status     string    `db:"status" json:"status"`
	CreatedAt  time.Time `db:"created_at" json:"created_at"`
}

// ============================================================================
// 非模型类型
// ============================================================================

// ClientInfo 携带从 HTTP 请求中提取出的客户端 IP 和 User-Agent。
type ClientInfo struct {
	IP        string
	UserAgent string
}

// ============================================================================
// 枚举类型
// ============================================================================

// IdentifierType 用于区分手机号登录和邮箱登录。
type IdentifierType string

const (
	IdentifierPhone IdentifierType = "PHONE"
	IdentifierEmail IdentifierType = "EMAIL"
)

// VerificationScene 表示验证码发送所处的业务场景。
type VerificationScene string

const (
	SceneRegister      VerificationScene = "REGISTER"
	SceneLogin         VerificationScene = "LOGIN"
	SceneResetPassword VerificationScene = "RESET_PASSWORD"
)

// VerificationCodeStatus 表示一次验证码校验的结果状态。
type VerificationCodeStatus string

const (
	StatusNotFound        VerificationCodeStatus = "NOT_FOUND"
	StatusExpired         VerificationCodeStatus = "EXPIRED"
	StatusMismatch        VerificationCodeStatus = "MISMATCH"
	StatusTooManyAttempts VerificationCodeStatus = "TOO_MANY_ATTEMPTS"
	StatusSuccess         VerificationCodeStatus = "SUCCESS"
)

// 登录渠道常量
const (
	ChannelPassword = "PASSWORD"
	ChannelCode     = "CODE"
)

// 登录状态常量
const (
	LoginStatusSuccess = "SUCCESS"
	LoginStatusFailed  = "FAILED"
)

// ============================================================================
// 结果类型
// ============================================================================

// VerificationCheckResult 封装 Verify() 的校验结果。
type VerificationCheckResult struct {
	Success bool                   `json:"success"`
	Status  VerificationCodeStatus `json:"status"`
}

// SendCodeResult 封装 SendCode() 的返回结果。
type SendCodeResult struct {
	Identifier    string            `json:"identifier"`
	Scene         VerificationScene `json:"scene"`
	ExpireSeconds int               `json:"expire_seconds"`
}

// TokenPair 保存访问令牌、刷新令牌及其过期信息。
type TokenPair struct {
	AccessToken           string    `json:"access_token"`
	AccessTokenExpiresAt  time.Time `json:"access_token_expires_at"`
	RefreshToken          string    `json:"refresh_token"`
	RefreshTokenExpiresAt time.Time `json:"refresh_token_expires_at"`
	RefreshTokenID        string    `json:"-"`
}
