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
	"golang.org/x/crypto/bcrypt"
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
	}
}

// SendCode 发送验证码。
//
// 业务流程：
//  1. 规范化用户标识（邮箱转小写、清除首尾空格）。
//  2. 验证标识格式是否合法（手机号正则 / 邮箱正则）。
//  3. 根据 Scene 校验前置条件：
//     - Register: 标识不能已存在
//     - Login / ResetPassword: 标识必须已注册
//  4. 委托 VerificationService.SendCode 完成验证码生成与 Redis 存储。
//
// 参数：
//   - req: 发送验证码请求（标识符、标识类型、业务场景）
//
// 返回值：
//   - SendCodeResponse: 包含标识符、场景和验证码过期时间
//   - *errcode.AppError: 验证失败或内部错误时返回
//
// 边界情况：
//   - 发送间隔内重复调用不会抛出错误，而是返回正常响应但不发送新验证码
//     （防短信轰炸，见 VerificationService.SendCode 的 interval 检查逻辑）
func (s *AuthService) SendCode(ctx context.Context, req *SendCodeRequest) (SendCodeResponse, *errcode.AppError) {
	normalized := normalizeIdentifier(req.IdentifierType, req.Identifier)
	if err := validateIdentifier(req.IdentifierType, normalized); err != nil {
		return SendCodeResponse{}, errcode.ErrBadRequest.WithMsg(err.Error())
	}

	exists := s.repo.IdentifierExists(ctx, req.IdentifierType, normalized)
	switch req.Scene {
	case SceneRegister:
		if exists {
			return SendCodeResponse{}, errcode.ErrIdentifierExists
		}
	case SceneLogin, SceneResetPassword:
		if !exists {
			return SendCodeResponse{}, errcode.ErrIdentifierNotFound
		}
	}

	result, err := s.verifSvc.SendCode(ctx, req.Scene, normalized)
	if err != nil {
		return SendCodeResponse{}, errcode.ErrInternal.WithMsg(err.Error())
	}

	return SendCodeResponse{
		Identifier:    result.Identifier,
		Scene:         result.Scene,
		ExpireSeconds: result.ExpireSeconds,
	}, nil
}

// Register 注册新用户。
//
// 业务流程：
//  1. 检查是否同意服务条款（AgreeTerms 必须为 true）。
//  2. 验证标识格式并检查唯一性。
//  3. 校验验证码（VerificationService.Verify）。
//  4. 如果提供了密码，做 bcrypt 哈希（需满足密码强度策略）。
//  5. 在 users 表中创建用户记录。
//  6. 签发访问令牌和刷新令牌对。
//  7. 将刷新令牌 ID 存入 Redis 白名单。
//  8. 记录注册登录日志。
//
// 参数：
//   - req: 注册请求（标识符、验证码、密码、协议同意）
//   - clientInfo: 客户端 IP 和 User-Agent，用于登录审计日志
//
// 返回值：
//   - AuthResponse: 包含用户资料和令牌对
//   - *errcode.AppError: 注册失败时返回业务错误码
//
// 函数调用说明：
//   - bcrypt.GenerateFromPassword():
//     golang.org/x/crypto/bcrypt 包，使用 bcrypt 算法对密码进行哈希。
//     参数为密码 []byte 和 cost 值（越高越安全但也越慢，默认 10，当前配置为 12）。
//     返回固定长度的哈希字符串（60 字符），每次调用结果不同（自动加盐）。
func (s *AuthService) Register(ctx context.Context, req *RegisterRequest, clientInfo ClientInfo) (AuthResponse, *errcode.AppError) {
	if !req.AgreeTerms {
		return AuthResponse{}, errcode.ErrTermsNotAccepted
	}

	normalized := normalizeIdentifier(req.IdentifierType, req.Identifier)
	if err := validateIdentifier(req.IdentifierType, normalized); err != nil {
		return AuthResponse{}, errcode.ErrBadRequest.WithMsg(err.Error())
	}

	if s.repo.IdentifierExists(ctx, req.IdentifierType, normalized) {
		return AuthResponse{}, errcode.ErrIdentifierExists
	}

	checkResult := s.verifSvc.Verify(ctx, SceneRegister, normalized, req.Code)
	if err := ensureVerificationSuccess(checkResult); err != nil {
		return AuthResponse{}, err
	}

	var passwordHash *string
	if req.Password != "" {
		if err := validatePassword(req.Password, s.cfg.Password); err != nil {
			return AuthResponse{}, errcode.ErrBadRequest.WithMsg(err.Error())
		}
		hash, err := bcrypt.GenerateFromPassword([]byte(req.Password), s.cfg.Password.BcryptCost)
		if err != nil {
			return AuthResponse{}, errcode.ErrInternal.WithMsg("failed to hash password")
		}
		h := string(hash)
		passwordHash = &h
	}

	user := &User{
		Nickname:     generateNickname(),
		PasswordHash: passwordHash,
	}
	switch req.IdentifierType {
	case IdentifierPhone:
		user.Phone = &normalized
	case IdentifierEmail:
		user.Email = &normalized
	}

	if err := s.repo.CreateUser(ctx, user); err != nil {
		return AuthResponse{}, errcode.ErrInternal.WithMsg("failed to create user")
	}

	tokenPair, err := s.jwtSvc.IssueTokenPair(user)
	if err != nil {
		return AuthResponse{}, errcode.ErrInternal.WithMsg("failed to issue tokens")
	}

	if err := s.tokenStore.StoreToken(ctx, user.ID, tokenPair.RefreshTokenID, s.cfg.Jwt.RefreshTokenTTL); err != nil {
		return AuthResponse{}, errcode.ErrInternal.WithMsg("failed to persist refresh token")
	}
	s.recordLoginLog(ctx, user.ID, normalized, "REGISTER", LoginStatusSuccess, clientInfo)

	return AuthResponse{
		User:  mapUserToResponse(user),
		Token: mapTokenToResponse(tokenPair),
	}, nil
}

// Login 用户登录。支持密码登录和验证码登录两种方式。
//
// 业务流程：
//  1. 根据标识符（手机号/邮箱）查询用户。
//  2. 根据请求中提供的信息决定鉴权方式：
//     a. Code 非空 → 验证码登录（调用 VerificationService.Verify）
//     b. Password 非空 → 密码登录（bcrypt.CompareHashAndPassword）
//  3. 签发新令牌对。
//  4. 刷新令牌 ID 存入 Redis 白名单。
//  5. 记录登录日志（成功或失败均记录）。
//
// 参数：
//   - req: 登录请求（标识符、密码或验证码）
//   - clientInfo: 客户端信息（用于审计日志）
//
// 返回值：
//   - AuthResponse: 用户信息和令牌对
//   - *errcode.AppError: 登录失败的业务错误
//
// 函数调用说明：
//   - bcrypt.CompareHashAndPassword():
//     比较密码哈希和明文密码。
//     第一个参数是数据库中存储的 bcrypt 哈希（60 字符字符串的 []byte）。
//     第二个参数是用户输入的明文密码的 []byte。
//     如果匹配返回 nil，不匹配返回错误。
//     该函数会自动从哈希中提取 salt 和 cost 参数，无需额外配置。
//
// 边界情况：
//   - 登录失败时仍会记录 login_logs（status = FAILED），用于安全审计。
//   - 验证码登录成功后会删除该验证码（防止重复使用）。
func (s *AuthService) Login(ctx context.Context, req *LoginRequest, clientInfo ClientInfo) (AuthResponse, *errcode.AppError) {
	normalized := normalizeIdentifier(req.IdentifierType, req.Identifier)
	if err := validateIdentifier(req.IdentifierType, normalized); err != nil {
		return AuthResponse{}, errcode.ErrBadRequest.WithMsg(err.Error())
	}

	user, err := s.repo.FindUserByIdentifier(ctx, req.IdentifierType, normalized)
	if err != nil {
		return AuthResponse{}, errcode.ErrIdentifierNotFound
	}

	channel := ChannelPassword
	if req.Code != "" {
		channel = ChannelCode
		checkResult := s.verifSvc.Verify(ctx, SceneLogin, normalized, req.Code)
		if err := ensureVerificationSuccess(checkResult); err != nil {
			s.recordLoginLog(ctx, user.ID, normalized, channel, LoginStatusFailed, clientInfo)
			return AuthResponse{}, err
		}
	} else {
		if req.Password == "" || user.PasswordHash == nil {
			s.recordLoginLog(ctx, user.ID, normalized, channel, LoginStatusFailed, clientInfo)
			return AuthResponse{}, errcode.ErrInvalidCredentials
		}
		if err := bcrypt.CompareHashAndPassword([]byte(*user.PasswordHash), []byte(req.Password)); err != nil {
			s.recordLoginLog(ctx, user.ID, normalized, channel, LoginStatusFailed, clientInfo)
			return AuthResponse{}, errcode.ErrInvalidCredentials
		}
	}

	tokenPair, err := s.jwtSvc.IssueTokenPair(user)
	if err != nil {
		return AuthResponse{}, errcode.ErrInternal.WithMsg("failed to issue tokens")
	}

	if err := s.tokenStore.StoreToken(ctx, user.ID, tokenPair.RefreshTokenID, s.cfg.Jwt.RefreshTokenTTL); err != nil {
		return AuthResponse{}, errcode.ErrInternal.WithMsg("failed to persist refresh token")
	}
	s.recordLoginLog(ctx, user.ID, normalized, channel, LoginStatusSuccess, clientInfo)

	return AuthResponse{
		User:  mapUserToResponse(user),
		Token: mapTokenToResponse(tokenPair),
	}, nil
}

// Refresh 刷新令牌对（使用 refresh token 换取新的 access token + refresh token）。
//
// 令牌轮换机制：
//  1. 验证 refresh token 的 JWT 签名（JwtService.ValidateToken）。
//  2. 检查 token type 是否为 "refresh"。
//  3. 获取 user 级别分布式锁，串行化 refresh 与 RevokeAll。
//  4. 查询 Redis 白名单确认该令牌 ID 仍有效（未被吊销）。
//  5. 吊销旧刷新令牌（从 Redis 白名单删除）。
//  6. 根据旧令牌中的用户 ID 重新查询用户信息。
//  7. 签发新的令牌对。
//  8. 将新刷新令牌 ID 存入 Redis 白名单。
//
// 参数：
//   - req: 包含旧 refresh token 的请求
//
// 返回值：
//   - AuthResponse: 新的用户信息和令牌对
//   - *errcode.AppError: 刷新失败时返回
//
// 安全说明：
//
//	使用令牌轮换（Token Rotation），每次刷新都吊销旧令牌。
//	如果攻击者窃取了 refresh token，在它被轮换后，合法用户再次刷新时会失败，
//	此时可以推断出令牌可能被窃取并采取相应措施。
func (s *AuthService) Refresh(ctx context.Context, req *TokenRefreshRequest) (AuthResponse, *errcode.AppError) {
	claims, err := s.jwtSvc.ValidateToken(req.RefreshToken)
	if err != nil {
		return AuthResponse{}, errcode.ErrRefreshTokenInvalid
	}

	jwtClaims, ok := claims.(*JwtClaims)
	if !ok || jwtClaims.TokenKind != "refresh" {
		return AuthResponse{}, errcode.ErrRefreshTokenInvalid
	}
	lock, appErr := s.acquireRefreshSessionLock(ctx, jwtClaims.UID)
	if appErr != nil {
		return AuthResponse{}, appErr
	}
	defer lock.Release()
	if !s.tokenStore.IsTokenValid(ctx, jwtClaims.UID, jwtClaims.ID) {
		return AuthResponse{}, errcode.ErrRefreshTokenInvalid
	}

	if err := s.tokenStore.RevokeToken(ctx, jwtClaims.UID, jwtClaims.ID); err != nil {
		return AuthResponse{}, errcode.ErrInternal.WithMsg("failed to revoke refresh token")
	}

	user, err := s.repo.FindUserByID(ctx, claims.UserID())
	if err != nil {
		return AuthResponse{}, errcode.ErrIdentifierNotFound
	}

	tokenPair, err := s.jwtSvc.IssueTokenPair(user)
	if err != nil {
		return AuthResponse{}, errcode.ErrInternal.WithMsg("failed to issue tokens")
	}
	if err := s.tokenStore.StoreToken(ctx, user.ID, tokenPair.RefreshTokenID, s.cfg.Jwt.RefreshTokenTTL); err != nil {
		return AuthResponse{}, errcode.ErrInternal.WithMsg("failed to persist refresh token")
	}

	return AuthResponse{
		User:  mapUserToResponse(user),
		Token: mapTokenToResponse(tokenPair),
	}, nil
}

// Logout 退出登录，吊销传入的 refresh token。
//
// 这是尽力而为的操作：即使 token 已过期或格式非法也不会返回错误。
// 调用方不需要针对错误做特殊处理。
//
// 参数：
//   - req: 包含需要吊销的 refresh token 的请求
func (s *AuthService) Logout(ctx context.Context, req *TokenRefreshRequest) {
	claims, err := s.jwtSvc.ValidateToken(req.RefreshToken)
	if err != nil || claims.TokenType() != "refresh" {
		return
	}
	if jwtClaims, ok := claims.(*JwtClaims); ok {
		s.tokenStore.RevokeToken(ctx, jwtClaims.UID, jwtClaims.ID)
	}
}

// ResetPassword 重置密码。需要先通过验证码验证身份。
//
// 业务流程：
//  1. 验证标识格式是否合法。
//  2. 通过标识符查找用户（确保用户存在）。
//  3. 校验验证码（VerificationService.Verify）。
//  4. 验证新密码满足强度策略（长度、字母、数字）。
//  5. 使用 bcrypt 对新密码进行哈希。
//  6. 获取 user 级别分布式锁，阻止并发 Refresh 在修改密码期间绕过全量吊销。
//  7. 更新数据库中的密码哈希。
//  8. 吊销该用户的所有 refresh token（强制所有设备重新登录）。
//
// 参数：
//   - req: 密码重置请求（标识符、验证码、新密码）
//
// 返回值：
//   - *errcode.AppError: 重置失败时返回业务错误
func (s *AuthService) ResetPassword(ctx context.Context, req *PasswordResetRequest) *errcode.AppError {
	normalized := normalizeIdentifier(req.IdentifierType, req.Identifier)
	if err := validateIdentifier(req.IdentifierType, normalized); err != nil {
		return errcode.ErrBadRequest.WithMsg(err.Error())
	}

	user, err := s.repo.FindUserByIdentifier(ctx, req.IdentifierType, normalized)
	if err != nil {
		return errcode.ErrIdentifierNotFound
	}

	checkResult := s.verifSvc.Verify(ctx, SceneResetPassword, normalized, req.Code)
	if err := ensureVerificationSuccess(checkResult); err != nil {
		return err
	}

	if err := validatePassword(req.NewPassword, s.cfg.Password); err != nil {
		return errcode.ErrBadRequest.WithMsg(err.Error())
	}

	hash, err := bcrypt.GenerateFromPassword([]byte(req.NewPassword), s.cfg.Password.BcryptCost)
	if err != nil {
		return errcode.ErrInternal.WithMsg("failed to hash password")
	}

	lock, appErr := s.acquireRefreshSessionLock(ctx, user.ID)
	if appErr != nil {
		return appErr
	}
	defer lock.Release()

	if err := s.repo.UpdatePassword(ctx, user.ID, string(hash)); err != nil {
		return errcode.ErrInternal.WithMsg("failed to update password")
	}
	if err := s.tokenStore.RevokeAll(ctx, user.ID); err != nil {
		return errcode.ErrInternal.WithMsg("failed to revoke refresh tokens")
	}
	return nil
}

// acquireRefreshSessionLock 获取用户 refresh token 会话锁。
//
// WHY 抽成单独辅助函数：
//   - Refresh 与 ResetPassword 需要共享同一把 user 级别锁，避免策略漂移。
//   - 错误统一映射为内部错误，业务流程只关注“是否拿到锁”。
func (s *AuthService) acquireRefreshSessionLock(ctx context.Context, userID uint64) (*redislock.Lock, *errcode.AppError) {
	if s.redis == nil {
		return nil, errcode.ErrInternal.WithMsg("redis client is unavailable")
	}
	if ctx == nil {
		ctx = context.Background()
	}

	lock, err := redislock.AcquireWithRetry(
		ctx,
		s.redis,
		refreshSessionLockKey(userID),
		s.refreshLockOptions,
		s.refreshLockRetryWait,
	)
	if err != nil {
		return nil, errcode.ErrInternal.WithMsg("failed to acquire refresh session lock")
	}
	return lock, nil
}

// CurrentUser 返回当前登录用户的资料。
//
// 参数：
//   - userID: 用户 ID（从 JWT claims 中获取）
//
// 返回值：
//   - AuthUserResponse: 用户公开资料（不含密码哈希）
//   - *errcode.AppError: 用户不存在时返回 404
func (s *AuthService) CurrentUser(ctx context.Context, userID uint64) (AuthUserResponse, *errcode.AppError) {
	user, err := s.repo.FindUserByID(ctx, userID)
	if err != nil {
		return AuthUserResponse{}, errcode.ErrIdentifierNotFound
	}
	return mapUserToResponse(user), nil
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
		matched, _ := regexp.MatchString(`^1[3-9]\d{9}$`, identifier)
		if !matched {
			return fmt.Errorf("invalid phone number format")
		}
	case IdentifierEmail:
		matched, _ := regexp.MatchString(`^[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}$`, identifier)
		if !matched {
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
func generateNickname() string {
	const charset = "abcdefghijklmnopqrstuvwxyz0123456789"
	suffix := make([]byte, 8)
	for i := range suffix {
		n, _ := rand.Int(rand.Reader, big.NewInt(int64(len(charset))))
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
		ZgId:     user.ZgId,
		Birthday: user.Birthday,
		School:   user.School,
		Bio:      user.Bio,
		Gender:   user.Gender,
		TagsJson: user.TagsJson,
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
