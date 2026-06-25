package auth

import (
	"context"

	"github.com/zhiguang/app/pkg/errcode"
	"github.com/zhiguang/app/pkg/redislock"
	"go.uber.org/zap"
	"golang.org/x/crypto/bcrypt"
)

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
		if err := s.tokenStore.RevokeToken(ctx, jwtClaims.UID, jwtClaims.ID); err != nil {
			zap.L().Warn("failed to revoke refresh token during logout", zap.Uint64("userID", jwtClaims.UID), zap.String("tokenID", jwtClaims.ID), zap.Error(err))
		}
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
		return errcode.ErrVerificationNotFound.WithMsg(err.Error())
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
//   - 错误统一映射为内部错误，业务流程只关注"是否拿到锁"。
func (s *AuthService) acquireRefreshSessionLock(ctx context.Context, userID uint64) (*redislock.Lock, *errcode.AppError) {
	if s.redis == nil {
		return nil, errcode.ErrInternal.WithMsg("redis client is unavailable")
	}
	if s.refreshLockTimeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, s.refreshLockTimeout)
		defer cancel()
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
