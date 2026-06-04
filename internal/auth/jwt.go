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

// JwtService 负责使用 RS256 签名创建和校验 JWT。
//
// 设计决策：
//   - 使用 RS256（非对称签名）而非 HS256（对称签名）：
//     + 公钥可以安全分发给其他微服务或前端 SDK 用于本地校验
//     + 私钥仅在当前服务持有，降低了密钥泄漏的影响范围
//   - 双令牌模式：短期 access token + 长期 refresh token 的组合
//   - 刷新令牌使用 Redis 白名单管理，支持主动吊销
//   - Access Token 中嵌入用户昵称（Nickname），避免每次请求都查一次数据库
type JwtService struct {
	privateKey *rsa.PrivateKey
	publicKey  *rsa.PublicKey
	config     *config.JwtConfig
}

// NewJwtService 从配置指定的路径加载 RSA 密钥，并创建 JwtService。
//
// 参数：
//   - cfg: JWT 配置（签发者、KeyID、私钥路径、公钥路径、令牌 TTL）
//
// 返回值：
//   - *JwtService: 初始化完成的 JWT 服务实例
//   - error: 如果密钥文件无法读取或解析则返回错误
//
// 函数调用说明：
//   - loadPrivateKey() 和 loadPublicKey():
//     读取 PEM 编码的 RSA 密钥文件并解析为 Go 的 rsa.PrivateKey / rsa.PublicKey。
//     支持 PKCS#8 和 PKCS#1 两种私钥格式，兼容 openssl 生成的不同格式。
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

// ValidateToken 使用 RS256 公钥校验并解析 JWT 字符串。
//
// 参数：
//   - tokenStr: 完整的 JWT 字符串（形如 "eyJhbGciOiJSUzI1NiIs..."）
//
// 返回值：
//   - middleware.TokenClaims: 解析后的 claims（包含 UserID 和 TokenType）
//   - error: 如果签名校验失败、令牌过期或算法不匹配则返回错误
//
// 函数调用说明：
//   - jwt.ParseWithClaims(tokenStr, &JwtClaims{}, keyFunc):
//     golang-jwt 库的解析函数。
//     第一个参数是 JWT 字符串。
//     第二个参数是自定义 claims 结构体（JwtClaims）的指针，解析后会自动填充。
//     第三个参数是一个 keyFunc 回调，用于返回验证密钥。
//     内部会自动校验：签名（RS256）、过期时间（exp）、签发时间（iat）等标准字段。
//   - token.Method.(*jwt.SigningMethodRSA):
//     类型断言，检查 JWT header 中声明的签名算法是否为 RSA 族算法。
//     如果不是则拒绝，防止攻击者使用 HS256 等对称算法欺骗验证（算法混淆攻击）。
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

// loadPrivateKey 读取 PEM 编码的 RSA 私钥文件并解析为 Go 的 rsa.PrivateKey。
//
// 支持两种私钥格式（兼容性处理）：
//   - PKCS#8：较新的标准格式，由 openssl genpkey 生成（推荐）
//   - PKCS#1：较旧的 RSA-only 格式，由 openssl genrsa 生成
//
// 函数调用说明：
//   - os.ReadFile(path):
//     一次性读取整个 PEM 文件到 []byte。
//   - pem.Decode(data):
//     解码 PEM 块（找到 "-----BEGIN XXX-----" 和 "-----END XXX-----" 之间的内容）。
//     返回 *pem.Block 结构体，包含类型（如 "RSA PRIVATE KEY"）和 DER 编码的字节。
//   - x509.ParsePKCS8PrivateKey(block.Bytes):
//     x509 包提供的 PKCS#8 格式私钥解析函数。
//     PKCS#8 是一种通用的私钥格式，支持 RSA、ECDSA、Ed25519 等多种算法。
//     返回 interface{}，需要通过类型断言转为具体的 *rsa.PrivateKey。
//   - x509.ParsePKCS1PrivateKey(block.Bytes):
//     x509 包提供的 PKCS#1 格式 RSA 私钥解析函数。
//     这是 RSA 私钥的传统格式，只支持 RSA。
//     如果 PKCS#8 解析失败，会回退尝试此格式。
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

// loadPublicKey 读取 PEM 编码的 RSA 公钥并解析为 Go 的 rsa.PublicKey。
//
// 函数调用说明：
//   - x509.ParsePKIXPublicKey(block.Bytes):
//     解析 SubjectPublicKeyInfo（PKIX）格式的公钥。
//     这是标准的 X.509 公钥格式，支持 RSA、ECDSA 等多种算法。
//     对应 openssl 的 `openssl rsa -pubin -in public.pem -RSAPublicKey_in` 格式。
//     注意与 x509.ParsePKCS1PublicKey() 的区别：前者是通用的 PKIX 格式，
//     后者是 RSA-only 的 PKCS#1 格式。
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
