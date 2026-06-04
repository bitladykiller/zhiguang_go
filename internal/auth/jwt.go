package auth

import (
	"crypto/rsa"
	"crypto/x509"
	"encoding/pem"
	"fmt"
	"os"
	"time"

	"github.com/golang-jwt/jwt/v5"
	"github.com/google/uuid"
	"github.com/zhiguang/app/pkg/config"
	"github.com/zhiguang/app/pkg/middleware"
)

// JwtService 负责创建和校验使用 RS256 签名的 JWT。
// 它会在启动时通过 NewJwtService 从 PEM 文件中加载 RSA 公私钥。
type JwtService struct {
	privateKey *rsa.PrivateKey
	publicKey  *rsa.PublicKey
	config     *config.JwtConfig
}

// NewJwtService 从配置指定的路径加载 RSA 密钥，并创建 JwtService。
func NewJwtService(cfg *config.JwtConfig) (*JwtService, error) {
	privateKey, err := loadPrivateKey(cfg.PrivateKeyPath)
	if err != nil {
		return nil, fmt.Errorf("failed to load private key from %s: %w", cfg.PrivateKeyPath, err)
	}

	publicKey, err := loadPublicKey(cfg.PublicKeyPath)
	if err != nil {
		return nil, fmt.Errorf("failed to load public key from %s: %w", cfg.PublicKeyPath, err)
	}

	return &JwtService{
		privateKey: privateKey,
		publicKey:  publicKey,
		config:     cfg,
	}, nil
}

// IssueTokenPair 生成访问令牌（短期）和刷新令牌（长期）。
// 刷新令牌 ID 会写入 Redis 白名单，以支持后续吊销。
func (s *JwtService) IssueTokenPair(user *User) (*TokenPair, error) {
	refreshTokenID := uuid.New().String()
	now := time.Now()
	accessExpiresAt := now.Add(s.config.AccessTokenTTL)
	refreshExpiresAt := now.Add(s.config.RefreshTokenTTL)

	accessToken, err := s.encode(user, now, accessExpiresAt, "access", uuid.New().String())
	if err != nil {
		return nil, err
	}

	refreshToken, err := s.encode(user, now, refreshExpiresAt, "refresh", refreshTokenID)
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

// ValidateToken 使用 RS256 公钥校验解析 JWT 字符串。
// 成功时返回解析后的 claims，失败时返回令牌无效或过期错误。
func (s *JwtService) ValidateToken(tokenStr string) (middleware.TokenClaims, error) {
	token, err := jwt.ParseWithClaims(tokenStr, &JwtClaims{}, func(token *jwt.Token) (interface{}, error) {
		if _, ok := token.Method.(*jwt.SigningMethodRSA); !ok {
			return nil, fmt.Errorf("unexpected signing method: %v", token.Header["alg"])
		}
		return s.publicKey, nil
	})
	if err != nil {
		return nil, err
	}

	if claims, ok := token.Claims.(*JwtClaims); ok && token.Valid {
		return claims, nil
	}

	return nil, fmt.Errorf("invalid token")
}

// encode 根据自定义 claims 生成带签名的 RS256 JWT。
func (s *JwtService) encode(user *User, issuedAt, expiresAt time.Time, tokenType, tokenID string) (string, error) {
	claims := &JwtClaims{
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

// loadPrivateKey 读取 PEM 编码的 RSA 私钥，兼容 PKCS#8 与 PKCS#1。
func loadPrivateKey(path string) (*rsa.PrivateKey, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}

	block, _ := pem.Decode(data)
	if block == nil {
		return nil, fmt.Errorf("no PEM block found")
	}

	// 先尝试解析 PKCS#8（较新的标准格式）
	key, err := x509.ParsePKCS8PrivateKey(block.Bytes)
	if err != nil {
		// 如果失败则回退到 PKCS#1（较旧格式）
		key, err = x509.ParsePKCS1PrivateKey(block.Bytes)
		if err != nil {
			return nil, fmt.Errorf("failed to parse private key: %w", err)
		}
	}

	rsaKey, ok := key.(*rsa.PrivateKey)
	if !ok {
		return nil, fmt.Errorf("key is not an RSA private key")
	}

	return rsaKey, nil
}

// loadPublicKey 读取 PEM 编码的 RSA 公钥（SubjectPublicKeyInfo 格式）。
func loadPublicKey(path string) (*rsa.PublicKey, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}

	block, _ := pem.Decode(data)
	if block == nil {
		return nil, fmt.Errorf("no PEM block found")
	}

	key, err := x509.ParsePKIXPublicKey(block.Bytes)
	if err != nil {
		return nil, fmt.Errorf("failed to parse public key: %w", err)
	}

	rsaKey, ok := key.(*rsa.PublicKey)
	if !ok {
		return nil, fmt.Errorf("key is not an RSA public key")
	}

	return rsaKey, nil
}
