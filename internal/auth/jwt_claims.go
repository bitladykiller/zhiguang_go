package auth

import "github.com/golang-jwt/jwt/v5"

// JwtClaims 在 jwt.RegisteredClaims 基础上扩展了业务字段。
// UID 与 TokenKind 是内部字段名；JSON tag 仍保持为 "uid" 与 "token_type"，
// 以兼容现有 JWT 协议格式。UserID() 与 TokenType() 用于实现 middleware.TokenClaims。
type JwtClaims struct {
	jwt.RegisteredClaims
	UID       uint64 `json:"uid"`
	TokenKind string `json:"token_type"`
	Nickname  string `json:"nickname,omitempty"`
}

// UserID 返回内嵌的用户 ID，用于实现 middleware.TokenClaims。
func (c *JwtClaims) UserID() uint64 { return c.UID }

// TokenType 返回令牌类型字符串，用于实现 middleware.TokenClaims。
func (c *JwtClaims) TokenType() string { return c.TokenKind }
