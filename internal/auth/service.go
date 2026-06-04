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
	"golang.org/x/crypto/bcrypt"
)

// AuthService 编排整个鉴权域的业务流程：
// 包括发送验证码、注册、登录、刷新令牌、登出、重置密码、读取当前用户信息。
//
// 设计模式：
//   - Facade：对 JwtService、VerificationService、仓储等依赖提供统一业务入口
//   - Strategy：同一个登录接口同时支持密码登录和验证码登录两种策略
type AuthService struct {
	repo       *AuthRepository
	verifSvc   *VerificationService
	jwtSvc     *JwtService
	tokenStore RefreshTokenStore
	cfg        *config.AuthConfig
}

// NewAuthService 使用完整依赖创建 AuthService。
func NewAuthService(
	repo *AuthRepository,
	verifSvc *VerificationService,
	jwtSvc *JwtService,
	tokenStore RefreshTokenStore,
	cfg *config.AuthConfig,
) *AuthService {
	return &AuthService{
		repo:       repo,
		verifSvc:   verifSvc,
		jwtSvc:     jwtSvc,
		tokenStore: tokenStore,
		cfg:        cfg,
	}
}
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

	result, err := s.verifSvc.SendCode(req.Scene, normalized)
	if err != nil {
		return SendCodeResponse{}, errcode.ErrInternal.WithMsg(err.Error())
	}

	return SendCodeResponse{
		Identifier:    result.Identifier,
		Scene:         result.Scene,
		ExpireSeconds: result.ExpireSeconds,
	}, nil
}

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

	checkResult := s.verifSvc.Verify(SceneRegister, normalized, req.Code)
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

	s.tokenStore.StoreToken(user.ID, tokenPair.RefreshTokenID, s.cfg.Jwt.RefreshTokenTTL)
	s.recordLoginLog(ctx, user.ID, normalized, "REGISTER", LoginStatusSuccess, clientInfo)

	return AuthResponse{
		User:  mapUserToResponse(user),
		Token: mapTokenToResponse(tokenPair),
	}, nil
}

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
		checkResult := s.verifSvc.Verify(SceneLogin, normalized, req.Code)
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

	s.tokenStore.StoreToken(user.ID, tokenPair.RefreshTokenID, s.cfg.Jwt.RefreshTokenTTL)
	s.recordLoginLog(ctx, user.ID, normalized, channel, LoginStatusSuccess, clientInfo)

	return AuthResponse{
		User:  mapUserToResponse(user),
		Token: mapTokenToResponse(tokenPair),
	}, nil
}

func (s *AuthService) Refresh(ctx context.Context, req *TokenRefreshRequest) (AuthResponse, *errcode.AppError) {
	claims, err := s.jwtSvc.ValidateToken(req.RefreshToken)
	if err != nil {
		return AuthResponse{}, errcode.ErrRefreshTokenInvalid
	}

	jwtClaims, ok := claims.(*JwtClaims)
	if !ok || jwtClaims.TokenKind != "refresh" {
		return AuthResponse{}, errcode.ErrRefreshTokenInvalid
	}
	if !s.tokenStore.IsTokenValid(jwtClaims.UID, jwtClaims.ID) {
		return AuthResponse{}, errcode.ErrRefreshTokenInvalid
	}

	s.tokenStore.RevokeToken(jwtClaims.UID, jwtClaims.ID)

	user, err := s.repo.FindUserByID(ctx, claims.UserID())
	if err != nil {
		return AuthResponse{}, errcode.ErrIdentifierNotFound
	}

	tokenPair, err := s.jwtSvc.IssueTokenPair(user)
	if err != nil {
		return AuthResponse{}, errcode.ErrInternal.WithMsg("failed to issue tokens")
	}
	s.tokenStore.StoreToken(user.ID, tokenPair.RefreshTokenID, s.cfg.Jwt.RefreshTokenTTL)

	return AuthResponse{
		User:  mapUserToResponse(user),
		Token: mapTokenToResponse(tokenPair),
	}, nil
}

func (s *AuthService) Logout(_ context.Context, req *TokenRefreshRequest) {
	claims, err := s.jwtSvc.ValidateToken(req.RefreshToken)
	if err != nil || claims.TokenType() != "refresh" {
		return
	}
	if jwtClaims, ok := claims.(*JwtClaims); ok {
		s.tokenStore.RevokeToken(jwtClaims.UID, jwtClaims.ID)
	}
}

func (s *AuthService) ResetPassword(ctx context.Context, req *PasswordResetRequest) *errcode.AppError {
	normalized := normalizeIdentifier(req.IdentifierType, req.Identifier)
	if err := validateIdentifier(req.IdentifierType, normalized); err != nil {
		return errcode.ErrBadRequest.WithMsg(err.Error())
	}

	user, err := s.repo.FindUserByIdentifier(ctx, req.IdentifierType, normalized)
	if err != nil {
		return errcode.ErrIdentifierNotFound
	}

	checkResult := s.verifSvc.Verify(SceneResetPassword, normalized, req.Code)
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
	if err := s.repo.UpdatePassword(ctx, user.ID, string(hash)); err != nil {
		return errcode.ErrInternal.WithMsg("failed to update password")
	}

	s.tokenStore.RevokeAll(user.ID)
	return nil
}

func (s *AuthService) CurrentUser(ctx context.Context, userID uint64) (AuthUserResponse, *errcode.AppError) {
	user, err := s.repo.FindUserByID(ctx, userID)
	if err != nil {
		return AuthUserResponse{}, errcode.ErrIdentifierNotFound
	}
	return mapUserToResponse(user), nil
}

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

func normalizeIdentifier(idType IdentifierType, identifier string) string {
	identifier = strings.TrimSpace(identifier)
	if idType == IdentifierEmail {
		identifier = strings.ToLower(identifier)
	}
	return identifier
}

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

func generateNickname() string {
	const charset = "abcdefghijklmnopqrstuvwxyz0123456789"
	suffix := make([]byte, 8)
	for i := range suffix {
		n, _ := rand.Int(rand.Reader, big.NewInt(int64(len(charset))))
		suffix[i] = charset[n.Int64()]
	}
	return "知光用户" + string(suffix)
}

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

func mapTokenToResponse(pair *TokenPair) TokenResponse {
	return TokenResponse{
		AccessToken:           pair.AccessToken,
		AccessTokenExpiresAt:  pair.AccessTokenExpiresAt,
		RefreshToken:          pair.RefreshToken,
		RefreshTokenExpiresAt: pair.RefreshTokenExpiresAt,
	}
}
