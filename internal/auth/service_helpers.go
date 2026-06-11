package auth

import (
	"context"
	"crypto/rand"
	"fmt"
	"math/big"
	"regexp"
	"strings"
	"unicode"

	"github.com/zhiguang/app/pkg/config"
	"github.com/zhiguang/app/pkg/errcode"
	"github.com/zhiguang/app/pkg/redislock"
)

// acquireRefreshSessionLock 获取用户 refresh token 会话锁。
//
// Refresh 与 ResetPassword 共享同一把 user 级别锁，
// 避免“密码刚重置，但并发 refresh 仍成功签出了新会话”的竞态。
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

// recordLoginLog 记录登录审计日志。失败不阻断主流程。
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

// validateIdentifier 校验用户标识格式是否合法。
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

// validatePassword 校验密码是否满足当前策略。
func validatePassword(password string, cfg config.PasswordConfig) error {
	if len(password) < cfg.MinLength {
		return fmt.Errorf("password must be at least %d characters", cfg.MinLength)
	}

	hasLetter := false
	hasDigit := false
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

// normalizeIdentifier 规范化手机号/邮箱等用户标识。
func normalizeIdentifier(idType IdentifierType, identifier string) string {
	identifier = strings.TrimSpace(identifier)
	if idType == IdentifierEmail {
		identifier = strings.ToLower(identifier)
	}
	return identifier
}

// ensureVerificationSuccess 把验证码校验结果映射为统一业务错误。
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

// generateNickname 生成默认昵称。
func generateNickname() string {
	const charset = "abcdefghijklmnopqrstuvwxyz0123456789"
	suffix := make([]byte, 8)
	for i := range suffix {
		n, _ := rand.Int(rand.Reader, big.NewInt(int64(len(charset))))
		suffix[i] = charset[n.Int64()]
	}
	return "知光用户" + string(suffix)
}

// mapUserToResponse 把领域模型映射成对外响应 DTO。
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

// mapTokenToResponse 把内部令牌对映射成 API DTO。
func mapTokenToResponse(pair *TokenPair) TokenResponse {
	return TokenResponse{
		AccessToken:           pair.AccessToken,
		AccessTokenExpiresAt:  pair.AccessTokenExpiresAt,
		RefreshToken:          pair.RefreshToken,
		RefreshTokenExpiresAt: pair.RefreshTokenExpiresAt,
	}
}
