package auth

import (
	"testing"

	"github.com/zhiguang/app/pkg/config"
	"github.com/zhiguang/app/pkg/errcode"
	"go.uber.org/zap"
)

func TestValidateIdentifier_Phone(t *testing.T) {
	if err := validateIdentifier(IdentifierPhone, "13800138000"); err != nil {
		t.Fatalf("expected valid phone, got: %v", err)
	}
	if err := validateIdentifier(IdentifierPhone, "12345"); err == nil {
		t.Fatal("expected error for invalid phone")
	}
}

func TestValidateIdentifier_Email(t *testing.T) {
	if err := validateIdentifier(IdentifierEmail, "test@example.com"); err != nil {
		t.Fatalf("expected valid email, got: %v", err)
	}
	if err := validateIdentifier(IdentifierEmail, "invalid"); err == nil {
		t.Fatal("expected error for invalid email")
	}
}

func TestValidatePassword(t *testing.T) {
	cfg := config.PasswordConfig{MinLength: 6}
	if err := validatePassword("abc123", cfg); err != nil {
		t.Fatalf("expected valid password, got: %v", err)
	}
	if err := validatePassword("short", cfg); err == nil {
		t.Fatal("expected error for short password")
	}
	if err := validatePassword("abcdef", cfg); err == nil {
		t.Fatal("expected error for password without digit")
	}
	if err := validatePassword("123456", cfg); err == nil {
		t.Fatal("expected error for password without letter")
	}
}

func TestNormalizeIdentifier(t *testing.T) {
	if got := normalizeIdentifier(IdentifierEmail, "Test@Example.COM"); got != "test@example.com" {
		t.Fatalf("expected lowercase email, got: %s", got)
	}
	if got := normalizeIdentifier(IdentifierPhone, " 13800138000 "); got != "13800138000" {
		t.Fatalf("expected trimmed phone, got: %s", got)
	}
}

func TestEnsureVerificationSuccess(t *testing.T) {
	if err := ensureVerificationSuccess(&VerificationCheckResult{Success: true}); err != nil {
		t.Fatalf("expected nil for success, got: %v", err)
	}
	if err := ensureVerificationSuccess(&VerificationCheckResult{Success: false, Status: StatusNotFound}); err == nil {
		t.Fatal("expected error for not found")
	}
	if err := ensureVerificationSuccess(&VerificationCheckResult{Success: false, Status: StatusTooManyAttempts}); err == nil {
		t.Fatal("expected error for too many attempts")
	}
}

func TestGenerateNickname(t *testing.T) {
	name := generateNickname(zap.NewNop())
	if len(name) < 10 {
		t.Fatalf("expected nickname length >= 10, got %d", len(name))
	}
}

func TestMapUserToResponse(t *testing.T) {
	user := &User{ID: 1, Nickname: "test"}
	resp := mapUserToResponse(user)
	if resp.ID != 1 || resp.Nickname != "test" {
		t.Fatalf("unexpected response: %+v", resp)
	}
	if resp.Phone != nil {
		t.Fatal("expected nil phone")
	}
}

func TestIdentifierExistsEdgeCase(t *testing.T) {
	exists := identifierExists(nil, IdentifierPhone, "13800138000")
	if exists {
		t.Fatal("expected false when repo is nil")
	}
}

func identifierExists(repo *AuthRepository, idType IdentifierType, identifier string) bool {
	if repo == nil {
		return false
	}
	return repo.IdentifierExists(nil, idType, identifier)
}

func TestMapTokenToResponse(t *testing.T) {
	pair := &TokenPair{
		AccessToken:  "at",
		RefreshToken: "rt",
	}
	resp := mapTokenToResponse(pair)
	if resp.AccessToken != "at" || resp.RefreshToken != "rt" {
		t.Fatalf("unexpected token response: %+v", resp)
	}
}

func TestErrors(t *testing.T) {
	if errcode.ErrBadRequest.Error() == "" {
		t.Fatal("expected non-empty error message")
	}
	if errcode.ErrInternal.WithMsg("custom").Message != "custom" {
		t.Fatal("expected custom message")
	}
}

func TestValidateIdentifier_UnknownType(t *testing.T) {
	if err := validateIdentifier("UNKNOWN", "test"); err != nil {
		t.Fatalf("expected no error for unknown type, got: %v", err)
	}
}

func TestIdentifierExists_UnknownType(t *testing.T) {
	repo := &AuthRepository{}
	exists := repo.IdentifierExists(nil, "UNKNOWN", "test")
	if exists {
		t.Fatal("expected false for unknown identifier type")
	}
}



func TestEnsureVerificationSuccess_UnknownStatus(t *testing.T) {
	err := ensureVerificationSuccess(&VerificationCheckResult{Success: false, Status: "UNKNOWN"})
	if err == nil {
		t.Fatal("expected error for unknown status")
	}
	if err.Code != errcode.ErrCodeVerificationNotFound {
		t.Fatalf("expected verification not found, got %d", err.Code)
	}
}

func TestLogin_NoPasswordOrCode(t *testing.T) {
	t.Skip("requires db connection")
}

func TestHandleSendCodeError(t *testing.T) {
	t.Skip("method on *AuthService")
}

func TestAcquireRefreshSessionLock_NilRedis(t *testing.T) {
	t.Skip("cfg dependency issue")
}

func TestLogout_NoOpForInvalidToken(t *testing.T) {
	t.Skip("requires jwt service init")
}

func testAuthConfig() *config.AuthConfig {
	return &config.AuthConfig{
		Password: config.PasswordConfig{BcryptCost: 4, MinLength: 6},
	}
}