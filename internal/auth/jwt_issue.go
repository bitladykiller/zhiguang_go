package auth

import (
	"fmt"
	"time"

	"github.com/golang-jwt/jwt/v5"
	"github.com/google/uuid"
)

// IssueTokenPair 生成访问令牌（短期）和刷新令牌（长期）。
//
// 流程：
//  1. 生成刷新令牌的 UUID（用于 Redis 白名单管理）。
//  2. 根据配置的 AccessTokenTTL / RefreshTokenTTL 计算过期时间。
//  3. 分别签发 access token（tokenType = "access"）和 refresh token（tokenType = "refresh"）。
//  4. 返回 TokenPair 结构体，其中 RefreshTokenID 用于后续白名单存储（不会序列化到 JSON）。
//
// 参数：
//   - user: 需要签发令牌的用户对象，用于提取 UID 和 Nickname 放入 token claims
//
// 返回值：
//   - *TokenPair: 包含 access token、refresh token 及其过期时间
//   - error: 如果 JWT 签名失败则返回错误
//
// 函数调用说明：
//   - uuid.New().String():
//     google/uuid 库，生成 V4 UUID（随机 UUID），用作 refresh token 的唯一标识符。
//     后续通过此 ID 在 Redis 白名单中管理 refresh token 的有效性。
func (s *JWTService) IssueTokenPair(user *User) (*TokenPair, error) {
	refreshTokenID := uuid.NewString()
	now := time.Now()
	accessExpiresAt := now.Add(s.config.AccessTokenTTL)
	refreshExpiresAt := now.Add(s.config.RefreshTokenTTL)

	accessToken, err := s.encode(user, now, accessExpiresAt, tokenTypeAccess, uuid.NewString())
	if err != nil {
		return nil, err
	}

	refreshToken, err := s.encode(user, now, refreshExpiresAt, tokenTypeRefresh, refreshTokenID)
	if err != nil {
		return nil, err
	}

	return &TokenPair{
		AccessToken:           accessToken,
		AccessTokenExpiresAt:  accessExpiresAt,
		RefreshToken:          refreshToken,
		RefreshTokenExpiresAt: refreshExpiresAt,
		RefreshTokenID:        refreshTokenID,
	}, nil
}

// encode 根据自定义 claims 生成带 RS256 签名的 JWT 字符串。
//
// 参数：
//   - user: 用户对象，用于提取 UID 和 Nickname
//   - issuedAt: 签发时间
//   - expiresAt: 过期时间
//   - tokenType: 令牌类型标识（"access" 或 "refresh"）
//   - tokenID: JWT ID（jti），access token 使用随机 UUID，refresh token 使用特定的 UUID
//
// 返回值：
//   - string: 已签名的完整 JWT 字符串
//   - error: 如果签名过程失败则返回错误
//
// 函数调用说明：
//   - jwt.NewWithClaims(jwt.SigningMethodRS256, claims):
//     创建一个新的 JWT token 对象。
//     使用 RS256 签名算法（RSA PKCS#1 v1.5 with SHA-256）。
//   - token.Header["kid"] = s.config.KeyID:
//     在 JWT header 中设置 Key ID。当有多个 RSA 密钥对轮换时，
//     客户端可以通过 kid 字段选择正确的公钥进行校验。
//   - token.SignedString(s.privateKey):
//     使用私钥对 token 进行签名，生成最终的 JWT 字符串。
func (s *JWTService) encode(user *User, issuedAt, expiresAt time.Time, tokenType, tokenID string) (string, error) {
	claims := &JWTClaims{
		RegisteredClaims: jwt.RegisteredClaims{
			Issuer:    s.config.Issuer,
			IssuedAt:  jwt.NewNumericDate(issuedAt),
			ExpiresAt: jwt.NewNumericDate(expiresAt),
			Subject:   fmt.Sprintf("%d", user.ID),
			ID:        tokenID,
		},
		UID:       user.ID,
		TokenKind: tokenType,
		Nickname:  user.Nickname,
	}

	token := jwt.NewWithClaims(jwt.SigningMethodRS256, claims)
	token.Header["kid"] = s.config.KeyID

	return token.SignedString(s.privateKey)
}
