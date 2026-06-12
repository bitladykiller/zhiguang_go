package auth

import (
	"testing"
	"time"

	"github.com/zhiguang/app/pkg/config"
)

func TestVerificationRedisKeys(t *testing.T) {
	scene := SceneRegister
	identifier := "user@example.com"
	now := time.Date(2026, 6, 12, 8, 0, 0, 0, time.UTC)

	if got := verificationCodeKey(scene, identifier); got != "vc:code:REGISTER:user@example.com" {
		t.Fatalf("verificationCodeKey() = %q", got)
	}
	if got := verificationIntervalKey(scene, identifier); got != "vc:interval:REGISTER:user@example.com" {
		t.Fatalf("verificationIntervalKey() = %q", got)
	}
	if got := verificationAttemptKey(scene, identifier); got != "vc:attempts:REGISTER:user@example.com" {
		t.Fatalf("verificationAttemptKey() = %q", got)
	}
	if got := verificationDailyKey(scene, identifier, now); got != "vc:daily:REGISTER:user@example.com:20260612" {
		t.Fatalf("verificationDailyKey() = %q", got)
	}
}

func TestGenerateCodeLengthAndDigits(t *testing.T) {
	if got := generateCode(0); got != "" {
		t.Fatalf("generateCode(0) = %q, want empty string", got)
	}

	code := generateCode(8)
	if len(code) != 8 {
		t.Fatalf("len(generateCode(8)) = %d, want 8", len(code))
	}
	for _, ch := range code {
		if ch < '0' || ch > '9' {
			t.Fatalf("generateCode(8) contains non-digit rune %q", ch)
		}
	}
}

func TestVerificationResultHelpers(t *testing.T) {
	failed := fail(StatusMismatch)
	if failed.Success {
		t.Fatal("fail() should return Success=false")
	}
	if failed.Status != StatusMismatch {
		t.Fatalf("fail() status = %q, want %q", failed.Status, StatusMismatch)
	}

	ok := success()
	if !ok.Success {
		t.Fatal("success() should return Success=true")
	}
	if ok.Status != StatusSuccess {
		t.Fatalf("success() status = %q, want %q", ok.Status, StatusSuccess)
	}
}

func TestSendCodeResultUsesTTLSeconds(t *testing.T) {
	svc := &VerificationService{
		config: &config.VerificationConfig{
			TTL: 5 * time.Minute,
		},
	}

	result := svc.sendCodeResult(SceneLogin, "13800138000")
	if result.Identifier != "13800138000" {
		t.Fatalf("Identifier = %q", result.Identifier)
	}
	if result.Scene != SceneLogin {
		t.Fatalf("Scene = %q", result.Scene)
	}
	if result.ExpireSeconds != 300 {
		t.Fatalf("ExpireSeconds = %d, want 300", result.ExpireSeconds)
	}
}
