package auth

import (
	"context"
	"crypto/rand"
	"fmt"
	"math/big"
	"regexp"
	"strings"
	"time"
	"unicode"

	"github.com/redis/go-redis/v9"
	"github.com/zhiguang/app/pkg/config"
	"github.com/zhiguang/app/pkg/errcode"
	"github.com/zhiguang/app/pkg/redislock"
	"go.uber.org/zap"
)

var (
	phoneRegex = regexp.MustCompile(`^1[3-9]\d{9}$`)
	emailRegex = regexp.MustCompile(`^[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}$`)
)

// AuthService 编排整个鉴权域的业务流程：
// 包括发送验证码、注册、登录、刷新令牌、登出、重置密码、读取当前用户信息。
//
// 设计模式：
//   - Facade：对 JwtService、VerificationService、仓储等依赖提供统一业务入口
//   - Strategy：同一个登录接口同时支持密码登录和验证码登录两种策略
type AuthService struct {
	repo                 *AuthRepository
	verifSvc             *VerificationService
	jwtSvc               *JwtService
	tokenStore           RefreshTokenStore
	redis                *redis.Client
	cfg                  *config.AuthConfig
	refreshLockOptions   redislock.Options
	refreshLockRetryWait time.Duration
	refreshLockTimeout   time.Duration
}

// NewAuthService 使用完整依赖创建 AuthService。
//
// 参数：
//   - repo: 用户仓储，负责 users 表 / login_logs 表的 CRUD
//   - verifSvc: 验证码服务，负责验证码的生成与校验
//   - jwtSvc: JWT 服务，负责令牌签发与校验
//   - tokenStore: 刷新令牌白名单存储（Redis）
//   - redisClient: Redis 客户端，用于 auth 域内的分布式锁协调
//   - cfg: 鉴权配置（jwt TTL、密码策略等）
func NewAuthService(
	repo *AuthRepository,
	verifSvc *VerificationService,
	jwtSvc *JwtService,
	tokenStore RefreshTokenStore,
	redisClient *redis.Client,
	cfg *config.AuthConfig,
) *AuthService {
	return &AuthService{
		repo:                 repo,
		verifSvc:             verifSvc,
		jwtSvc:               jwtSvc,
		tokenStore:           tokenStore,
		redis:                redisClient,
		cfg:                  cfg,
		refreshLockOptions:   refreshSessionLockOptions(cfg),
		refreshLockRetryWait: refreshSessionRetryInterval(cfg),
		refreshLockTimeout:   refreshSessionLockTimeout(cfg),
	}
}

// recordLoginLog 写入登录审计日志（异步、失败不阻塞主流程）。
//
// 参数：
//   - userID: 用户 ID（登录失败时可能为 0）
//   - identifier: 登录使用的标识（手机号/邮箱）
//   - channel: 登录渠道（PASSWORD / CODE / REGISTER）
//   - status: 登录结果（SUCCESS / FAILED）
//   - client: 客户端 IP 和 User-Agent
func (s *AuthService) recordLoginLog(ctx context.Context, userID uint64, identifier, channel, status string, client ClientInfo) {
	log := LoginLog{
		Identifier: identifier,
		Channel:    channel,
		Status:     status,
	}
	if userID != 0 {
		log.UserID = &userID
	}
	if client.IP != "" {
		log.IP = &client.IP
	}
	if client.UserAgent != "" {
		log.UserAgent = &client.UserAgent
	}
	s.repo.RecordLoginLog(ctx, &log)
}

// ============================================================================
// 纯函数
// ============================================================================

// validateIdentifier 校验用户标识格式是否合法。
//
// 参数：
//   - idType: 标识类型（PHONE / EMAIL）
//   - identifier: 已规范化的标识字符串
//
// 返回 error 当：
//   - 手机号不匹配正则 ^1[3-9]\d{9}$（13-19 开头的 11 位数字）
//   - 邮箱不匹配标准邮箱正则
//
// 函数调用说明：
//   - regexp.MatchString(pattern, s):
//     Go 的 regexp 包。检查字符串 s 是否匹配正则表达式 pattern。
//     注意：该函数会编译正则，每次调用都编译。在生产热门路径上
//     应该使用 regexp.MustCompile 预编译以提高性能。
//     这里因为不是高频调用，所以使用了方便的 MatchString 快捷方式。
func validateIdentifier(idType IdentifierType, identifier string) error {
	switch idType {
	case IdentifierPhone:
		if !phoneRegex.MatchString(identifier) {
			return fmt.Errorf("invalid phone number format")
		}
	case IdentifierEmail:
		if !emailRegex.MatchString(identifier) {
			return fmt.Errorf("invalid email format")
		}
	}
	return nil
}

// validatePassword 校验密码是否满足强度策略。
//
// 策略规则：
//   - 长度 >= cfg.MinLength（最小长度）
//   - 至少包含 1 个字母
//   - 至少包含 1 个数字
//
// 函数调用说明：
//   - unicode.IsLetter(ch): 判断字符是否为 Unicode 字母（含中文、日文等）。
//     这里实际上过于宽松——中文也是 letter。如果需要更严格的校验，
//     应该用 regexp.MatchString。
//   - unicode.IsDigit(ch): 判断字符是否为十进制数字（0-9）。
func validatePassword(password string, cfg config.PasswordConfig) error {
	if len(password) < cfg.MinLength {
		return fmt.Errorf("password must be at least %d characters", cfg.MinLength)
	}
	hasLetter, hasDigit := false, false
	for _, ch := range password {
		if unicode.IsLetter(ch) {
			hasLetter = true
		}
		if unicode.IsDigit(ch) {
			hasDigit = true
		}
	}
	if !hasLetter {
		return fmt.Errorf("password must contain at least one letter")
	}
	if !hasDigit {
		return fmt.Errorf("password must contain at least one digit")
	}
	return nil
}

// normalizeIdentifier 规范化用户标识。
//
// 规范规则：
//   - 所有类型：去除首尾空格
//   - 邮箱：转为小写（邮箱地址大小写不敏感）
//   - 手机号：不做额外格式化（仅去空格）
//
// 返回值：规范化后的标识字符串。
func normalizeIdentifier(idType IdentifierType, identifier string) string {
	identifier = strings.TrimSpace(identifier)
	if idType == IdentifierEmail {
		identifier = strings.ToLower(identifier)
	}
	return identifier
}

// ensureVerificationSuccess 将验证码校验结果转换为业务错误。
//
// 映射关系：
//   - StatusNotFound / StatusExpired → ErrVerificationNotFound
//   - StatusMismatch → ErrVerificationMismatch
//   - StatusTooManyAttempts → ErrVerificationTooManyAttempts
//   - StatusSuccess → nil（通过）
func ensureVerificationSuccess(result *VerificationCheckResult) *errcode.AppError {
	if result.Success {
		return nil
	}
	switch result.Status {
	case StatusNotFound, StatusExpired:
		return errcode.ErrVerificationNotFound
	case StatusMismatch:
		return errcode.ErrVerificationMismatch
	case StatusTooManyAttempts:
		return errcode.ErrVerificationTooManyAttempts
	default:
		return errcode.ErrVerificationNotFound
	}
}

// generateNickname 生成随机昵称。
//
// 格式："知光用户" + 8 位随机字母数字后缀。
// 例如："知光用户a3k9x7b2"
//
// 函数调用说明：
//   - rand.Int(rand.Reader, big.NewInt(n)):
//     crypto/rand 包提供的密码学安全的随机数生成器。
//     rand.Reader 是系统的熵源（Linux 上读取 /dev/urandom）。
//     与 math/rand 相比，crypto/rand 是真正的随机，不可预测。
//     big.NewInt(n) 指定随机数的上限（0 ≤ result < n）。
//   - charset[n.Int64()]:
//     用生成的随机数从字符集中选出一个字符。
//     循环 8 次，生成 8 位随机字符组成后缀。
const nicknameSuffixLen = 8

func generateNickname() string {
	const charset = "abcdefghijklmnopqrstuvwxyz0123456789"
	suffix := make([]byte, nicknameSuffixLen)
	for i := range suffix {
		n, err := rand.Int(rand.Reader, big.NewInt(int64(len(charset))))
		if err != nil {
			zap.L().Warn("failed to generate secure random number for nickname", zap.Error(err))
			n = big.NewInt(0)
		}
		suffix[i] = charset[n.Int64()]
	}
	return "知光用户" + string(suffix)
}

// mapUserToResponse 将领域模型 User 映射为响应 DTO AuthUserResponse。
//
// 安全说明：User 中包含 PasswordHash 字段（json:"-"不会序列化），
// 但 mapUserToResponse 只复制非敏感字段，确保密码哈希永远不会出现在 API 响应中。
func mapUserToResponse(user *User) AuthUserResponse {
	return AuthUserResponse{
		ID:       user.ID,
		Nickname: user.Nickname,
		Avatar:   user.Avatar,
		Phone:    user.Phone,
		ZgID:     user.ZgID,
		Birthday: user.Birthday,
		School:   user.School,
		Bio:      user.Bio,
		Gender:   user.Gender,
		TagsJson: user.TagsJSON,
	}
}

// mapTokenToResponse 将 TokenPair 映射为 TokenResponse DTO。
//
// 注意：TokenPair.RefreshTokenID 字段的 json 标签为 "-"（仅内部使用），
// 映射时会忽略该字段，不会暴露刷新令牌 ID 给前端。
func mapTokenToResponse(pair *TokenPair) TokenResponse {
	return TokenResponse{
		AccessToken:           pair.AccessToken,
		AccessTokenExpiresAt:  pair.AccessTokenExpiresAt,
		RefreshToken:          pair.RefreshToken,
		RefreshTokenExpiresAt: pair.RefreshTokenExpiresAt,
	}
}
