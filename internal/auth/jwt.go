package auth

import (
	"crypto/rsa"
	"fmt"

	"github.com/zhiguang/app/pkg/config"
)

const (
	tokenTypeAccess  = "access"
	tokenTypeRefresh = "refresh"
)

// JWTService 负责使用 RS256 签名创建和校验 JWT。
//
// 为了避免服务构造、令牌签发、令牌校验和密钥加载继续堆在同一文件里，
// 当前按职责拆分为：
//   - jwt.go: 服务结构体与构造函数
//   - jwt_issue.go: access/refresh token 签发
//   - jwt_validate.go: token 校验
//   - jwt_keys.go: RSA PEM 密钥加载
type JWTService struct {
	privateKey *rsa.PrivateKey
	publicKey  *rsa.PublicKey
	config     *config.JWTConfig
}

// NewJWTService 从配置指定的路径加载 RSA 密钥，并创建 JWTService。
//
// 参数：
//   - cfg: JWT 配置（签发者、KeyID、私钥路径、公钥路径、令牌 TTL）
//
// 返回值：
//   - *JWTService: 初始化完成的 JWT 服务实例
//   - error: 如果密钥文件无法读取或解析则返回错误
//
// 函数调用说明：
//   - loadPrivateKey() 和 loadPublicKey():
//     读取 PEM 编码的 RSA 密钥文件并解析为 Go 的 rsa.PrivateKey / rsa.PublicKey。
//     支持 PKCS#8 和 PKCS#1 两种私钥格式，兼容 openssl 生成的不同格式。
func NewJWTService(cfg *config.JWTConfig) (*JWTService, error) {
	privateKey, err := loadPrivateKey(cfg.PrivateKeyPath)
	if err != nil {
		return nil, fmt.Errorf("failed to load private key from %s: %w", cfg.PrivateKeyPath, err)
	}

	publicKey, err := loadPublicKey(cfg.PublicKeyPath)
	if err != nil {
		return nil, fmt.Errorf("failed to load public key from %s: %w", cfg.PublicKeyPath, err)
	}

	return &JWTService{
		privateKey: privateKey,
		publicKey:  publicKey,
		config:     cfg,
	}, nil
}
