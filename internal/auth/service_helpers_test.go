package auth

import (
	"strings"
	"testing"
	"unicode/utf8"

	"github.com/zhiguang/app/pkg/config"
	"github.com/zhiguang/app/pkg/errcode"
)

func TestNormalizeIdentifier(t *testing.T) {
	if got := normalizeIdentifier(IdentifierEmail, "  Foo@Example.COM "); got != "foo@example.com" {
		t.Fatalf("normalize email = %q", got)
	}
	if got := normalizeIdentifier(IdentifierPhone, " 13800138000 "); got != "13800138000" {
		t.Fatalf("normalize phone = %q", got)
	}
}

func TestValidateIdentifier(t *testing.T) {
	if err := validateIdentifier(IdentifierPhone, "13800138000"); err != nil {
		t.Fatalf("validate phone: %v", err)
	}
	if err := validateIdentifier(IdentifierEmail, "user@example.com"); err != nil {
		t.Fatalf("validate email: %v", err)
	}
	if err := validateIdentifier(IdentifierPhone, "123456"); err == nil {
		t.Fatal("expected invalid phone to fail")
	}
	if err := validateIdentifier(IdentifierEmail, "not-an-email"); err == nil {
		t.Fatal("expected invalid email to fail")
	}
}

func TestValidatePassword(t *testing.T) {
	cfg := config.PasswordConfig{MinLength: 8}

	if err := validatePassword("abc12345", cfg); err != nil {
		t.Fatalf("validatePassword() unexpected error: %v", err)
	}
	if err := validatePassword("short1", cfg); err == nil {
		t.Fatal("expected short password to fail")
	}
	if err := validatePassword("abcdefgh", cfg); err == nil {
		t.Fatal("expected password without digit to fail")
	}
	if err := validatePassword("12345678", cfg); err == nil {
		t.Fatal("expected password without letter to fail")
	}
}

func TestEnsureVerificationSuccess(t *testing.T) {
	if err := ensureVerificationSuccess(&VerificationCheckResult{Success: true}); err != nil {
		t.Fatalf("expected success result to pass, got %v", err)
	}
	if err := ensureVerificationSuccess(&VerificationCheckResult{Status: StatusMismatch}); err != errcode.ErrVerificationMismatch {
		t.Fatalf("expected mismatch error, got %v", err)
	}
	if err := ensureVerificationSuccess(&VerificationCheckResult{Status: StatusTooManyAttempts}); err != errcode.ErrVerificationTooManyAttempts {
		t.Fatalf("expected too many attempts error, got %v", err)
	}
}

func TestGenerateNickname(t *testing.T) {
	got := generateNickname()

	if !strings.HasPrefix(got, "知光用户") {
		t.Fatalf("nickname prefix = %q", got)
	}
	if utf8.RuneCountInString(got) != utf8.RuneCountInString("知光用户")+8 {
		t.Fatalf("nickname rune length = %d", utf8.RuneCountInString(got))
	}
}
