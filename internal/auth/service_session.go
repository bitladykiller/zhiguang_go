package auth

import (
	"context"

	"github.com/zhiguang/app/pkg/errcode"
	"golang.org/x/crypto/bcrypt"
)

// Login 用户登录，支持密码登录和验证码登录两种方式。
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
	if err := s.tokenStore.StoreToken(user.ID, tokenPair.RefreshTokenID, s.cfg.JWT.RefreshTokenTTL); err != nil {
		return AuthResponse{}, errcode.ErrInternal.WithMsg("failed to persist refresh token")
	}

	s.recordLoginLog(ctx, user.ID, normalized, channel, LoginStatusSuccess, clientInfo)
	return AuthResponse{
		User:  mapUserToResponse(user),
		Token: mapTokenToResponse(tokenPair),
	}, nil
}

// Refresh 使用 refresh token 换取新的 access token 与 refresh token。
func (s *AuthService) Refresh(ctx context.Context, req *TokenRefreshRequest) (AuthResponse, *errcode.AppError) {
	claims, err := s.jwtSvc.ValidateToken(req.RefreshToken)
	if err != nil {
		return AuthResponse{}, errcode.ErrRefreshTokenInvalid
	}

	jwtClaims, ok := claims.(*JWTClaims)
	if !ok || jwtClaims.TokenKind != "refresh" {
		return AuthResponse{}, errcode.ErrRefreshTokenInvalid
	}

	lock, appErr := s.acquireRefreshSessionLock(ctx, jwtClaims.UID)
	if appErr != nil {
		return AuthResponse{}, appErr
	}
	defer lock.Release()

	if !s.tokenStore.IsTokenValid(jwtClaims.UID, jwtClaims.ID) {
		return AuthResponse{}, errcode.ErrRefreshTokenInvalid
	}
	if err := s.tokenStore.RevokeToken(jwtClaims.UID, jwtClaims.ID); err != nil {
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
	if err := s.tokenStore.StoreToken(user.ID, tokenPair.RefreshTokenID, s.cfg.JWT.RefreshTokenTTL); err != nil {
		return AuthResponse{}, errcode.ErrInternal.WithMsg("failed to persist refresh token")
	}

	return AuthResponse{
		User:  mapUserToResponse(user),
		Token: mapTokenToResponse(tokenPair),
	}, nil
}

// Logout 退出登录，尽力吊销传入的 refresh token。
func (s *AuthService) Logout(_ context.Context, req *TokenRefreshRequest) {
	claims, err := s.jwtSvc.ValidateToken(req.RefreshToken)
	if err != nil || claims.TokenType() != "refresh" {
		return
	}
	if jwtClaims, ok := claims.(*JWTClaims); ok {
		if err := s.tokenStore.RevokeToken(jwtClaims.UID, jwtClaims.ID); err != nil {
			return
		}
	}
}

// CurrentUser 返回当前登录用户的公开资料。
func (s *AuthService) CurrentUser(ctx context.Context, userID uint64) (AuthUserResponse, *errcode.AppError) {
	user, err := s.repo.FindUserByID(ctx, userID)
	if err != nil {
		return AuthUserResponse{}, errcode.ErrIdentifierNotFound
	}
	return mapUserToResponse(user), nil
}
