package auth

import (
	"context"

	"github.com/zhiguang/app/pkg/errcode"
	"golang.org/x/crypto/bcrypt"
)

// authenticateUser 根据登录请求鉴权用户。
//
// 支持密码登录和验证码登录两种方式。
func (s *AuthService) authenticateUser(ctx context.Context, user *User, normalized string, req *LoginRequest) (string, *errcode.AppError) {
	channel := ChannelPassword
	if req.Code != "" {
		channel = ChannelCode
		checkResult := s.verifSvc.Verify(ctx, SceneLogin, normalized, req.Code)
		if err := ensureVerificationSuccess(checkResult); err != nil {
			return channel, err
		}
	} else if req.Password == "" || user.PasswordHash == nil {
		return channel, errcode.ErrInvalidCredentials
	} else if err := bcrypt.CompareHashAndPassword([]byte(*user.PasswordHash), []byte(req.Password)); err != nil {
		return channel, errcode.ErrInvalidCredentials
	}
	return channel, nil
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

	channel, appErr := s.authenticateUser(ctx, user, normalized, req)
	if appErr != nil {
		s.recordLoginLog(ctx, user.ID, normalized, channel, LoginStatusFailed, clientInfo)
		return AuthResponse{}, appErr
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
