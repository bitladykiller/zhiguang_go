package auth

import (
	"context"

	"github.com/zhiguang/app/pkg/errcode"
	"golang.org/x/crypto/bcrypt"
)

// SendCode 发送验证码。
//
// 这里统一处理标识规范化、格式校验和场景前置条件，
// 真正的验证码生成与存储仍委托给 VerificationService。
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

// Register 注册新用户并签发首个令牌对。
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
		value := string(hash)
		passwordHash = &value
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
	if err := s.tokenStore.StoreToken(user.ID, tokenPair.RefreshTokenID, s.cfg.JWT.RefreshTokenTTL); err != nil {
		return AuthResponse{}, errcode.ErrInternal.WithMsg("failed to persist refresh token")
	}

	s.recordLoginLog(ctx, user.ID, normalized, "REGISTER", LoginStatusSuccess, clientInfo)
	return AuthResponse{
		User:  mapUserToResponse(user),
		Token: mapTokenToResponse(tokenPair),
	}, nil
}

// ResetPassword 重置密码，并强制吊销该用户的全部 refresh token。
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
	if err := s.tokenStore.RevokeAll(user.ID); err != nil {
		return errcode.ErrInternal.WithMsg("failed to revoke refresh tokens")
	}
	return nil
}
