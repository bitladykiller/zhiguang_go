package auth

import (
	"context"

	"github.com/zhiguang/app/pkg/errcode"
	"golang.org/x/crypto/bcrypt"
)

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

	user, appErr := s.createUser(ctx, req, normalized)
	if appErr != nil {
		return AuthResponse{}, appErr
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

// createUser 创建用户记录并返回 User 模型。
func (s *AuthService) createUser(ctx context.Context, req *RegisterRequest, normalized string) (*User, *errcode.AppError) {
	var passwordHash *string
	if req.Password != "" {
		if err := validatePassword(req.Password, s.cfg.Password); err != nil {
			return nil, errcode.ErrBadRequest.WithMsg(err.Error())
		}
		hash, err := bcrypt.GenerateFromPassword([]byte(req.Password), s.cfg.Password.BcryptCost)
		if err != nil {
			return nil, errcode.ErrInternal.WithMsg("failed to hash password")
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
		return nil, errcode.ErrInternal.WithMsg("failed to create user")
	}
	return user, nil
}
