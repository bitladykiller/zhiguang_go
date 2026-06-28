package auth

import (
	"context"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
	"github.com/zhiguang/app/pkg/config"
	"go.uber.org/zap"
)

func newTestRedisVerification(t *testing.T) (*miniredis.Miniredis, *VerificationService) {
	t.Helper()
	mr := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})

	cfg := &config.VerificationConfig{
		CodeLength:        6,
		TTL:               5 * time.Minute,
		MaxAttempts:       3,
		SendInterval:      60 * time.Second,
		DailyLimit:        5,
		OperationTimeoutMs: 0,
	}
	svc := NewVerificationService(rdb, cfg, zap.NewNop())
	return mr, svc
}

func TestNewVerificationService(t *testing.T) {
	t.Run("with logger", func(t *testing.T) {
		mr := miniredis.RunT(t)
		rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
		svc := NewVerificationService(rdb, &config.VerificationConfig{}, zap.NewNop())
		if svc == nil {
			t.Fatal("svc is nil")
		}
	})
}

func TestVerification_SendCode(t *testing.T) {
	t.Run("success", func(t *testing.T) {
		mr, svc := newTestRedisVerification(t)
		_ = mr
		result, err := svc.SendCode(context.Background(), SceneRegister, "13800138000")
		if err != nil {
			t.Fatalf("send code: %v", err)
		}
		if result.Identifier != "13800138000" {
			t.Fatalf("expected identifier '13800138000', got '%s'", result.Identifier)
		}
		if result.Scene != SceneRegister {
			t.Fatalf("expected scene REGISTER, got '%s'", result.Scene)
		}
		if result.ExpireSeconds <= 0 {
			t.Fatalf("expected positive expire seconds, got %d", result.ExpireSeconds)
		}
	})

	t.Run("interval limit", func(t *testing.T) {
		mr, svc := newTestRedisVerification(t)
		_ = mr
		_, err := svc.SendCode(context.Background(), SceneLogin, "13800138001")
		if err != nil {
			t.Fatalf("first send: %v", err)
		}
		_, err = svc.SendCode(context.Background(), SceneLogin, "13800138001")
		if err != nil {
			t.Fatalf("second send during interval should not error: %v", err)
		}
	})

	t.Run("daily limit exceeded", func(t *testing.T) {
		mr, svc := newTestRedisVerification(t)
		ctx := context.Background()
		for i := 0; i < 5; i++ {
			dailyKey := "vc:daily:REGISTER:13800138002:" + time.Now().Format("20060102")
			if _, err := mr.Incr(dailyKey, 1); err != nil {
				t.Fatalf("incr daily: %v", err)
			}
		}
		_, err := svc.SendCode(ctx, SceneRegister, "13800138002")
		if err == nil || err.Error() != "超过每日上限" {
			t.Fatalf("expected daily limit exceeded, got: %v", err)
		}
	})
}

func TestVerification_Verify(t *testing.T) {
	t.Run("not found", func(t *testing.T) {
		mr, svc := newTestRedisVerification(t)
		_ = mr
		vr := svc.Verify(context.Background(), SceneRegister, "nonexistent", "123456")
		if vr.Success {
			t.Fatal("expected failure for non-existent code")
		}
		if vr.Status != StatusNotFound {
			t.Fatalf("expected NOT_FOUND, got '%s'", vr.Status)
		}
	})
}

func TestGenerateCode(t *testing.T) {
	t.Run("length is correct", func(t *testing.T) {
		code := generateCode(6, zap.NewNop())
		if len(code) != 6 {
			t.Fatalf("expected length 6, got %d", len(code))
		}
	})
	t.Run("all digits", func(t *testing.T) {
		code := generateCode(4, zap.NewNop())
		for _, ch := range code {
			if ch < '0' || ch > '9' {
				t.Fatalf("unexpected character '%c' in code", ch)
			}
		}
	})
	t.Run("different lengths", func(t *testing.T) {
		for _, l := range []int{4, 6, 8} {
			code := generateCode(l, zap.NewNop())
			if len(code) != l {
				t.Fatalf("expected length %d, got %d", l, len(code))
			}
		}
	})
}

func TestFailAndSuccess(t *testing.T) {
	t.Run("fail", func(t *testing.T) {
		r := fail(StatusNotFound)
		if r.Success || r.Status != StatusNotFound {
			t.Fatalf("unexpected: %+v", r)
		}
		r2 := fail(StatusMismatch)
		if r2.Success || r2.Status != StatusMismatch {
			t.Fatalf("unexpected: %+v", r2)
		}
		r3 := fail(StatusTooManyAttempts)
		if r3.Success || r3.Status != StatusTooManyAttempts {
			t.Fatalf("unexpected: %+v", r3)
		}
	})

	t.Run("success", func(t *testing.T) {
		r := success()
		if !r.Success || r.Status != StatusSuccess {
			t.Fatalf("unexpected: %+v", r)
		}
	})
}
