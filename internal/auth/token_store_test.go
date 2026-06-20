package auth

import (
	"context"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
	"go.uber.org/zap"
)

func newTestRedisTokenStore(t *testing.T) (*miniredis.Miniredis, *RedisRefreshTokenStore) {
	t.Helper()
	mr := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	store := NewRedisRefreshTokenStore(rdb, zap.NewNop())
	return mr, store
}

func TestNewRedisRefreshTokenStore(t *testing.T) {
	t.Run("with logger", func(t *testing.T) {
		mr := miniredis.RunT(t)
		rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
		store := NewRedisRefreshTokenStore(rdb, zap.NewNop())
		if store == nil {
			t.Fatal("store is nil")
		}
	})
}

func TestStoreToken(t *testing.T) {
	t.Run("success", func(t *testing.T) {
		_, store := newTestRedisTokenStore(t)
		err := store.StoreToken(context.Background(), 1, "token-abc", time.Minute)
		if err != nil {
			t.Fatalf("store token: %v", err)
		}
	})
}

func TestIsTokenValid(t *testing.T) {
	t.Run("valid token", func(t *testing.T) {
		mr, store := newTestRedisTokenStore(t)
		ctx := context.Background()
		mr.Set("rt:1:valid-token", "1")
		valid := store.IsTokenValid(ctx, 1, "valid-token")
		if !valid {
			t.Fatal("expected valid token")
		}
	})

	t.Run("invalid token", func(t *testing.T) {
		_, store := newTestRedisTokenStore(t)
		valid := store.IsTokenValid(context.Background(), 1, "nonexistent")
		if valid {
			t.Fatal("expected invalid token")
		}
	})

	t.Run("redis error returns invalid", func(t *testing.T) {
		mr, store := newTestRedisTokenStore(t)
		mr.Close()
		valid := store.IsTokenValid(context.Background(), 1, "any")
		if valid {
			t.Fatal("expected false on redis error")
		}
	})
}

func TestRevokeToken(t *testing.T) {
	mr, store := newTestRedisTokenStore(t)
	ctx := context.Background()
	mr.Set("rt:1:revoke-me", "1")

	err := store.RevokeToken(ctx, 1, "revoke-me")
	if err != nil {
		t.Fatalf("revoke: %v", err)
	}
	if mr.Exists("rt:1:revoke-me") {
		t.Fatal("token should be revoked")
	}
}

func TestRevokeAll(t *testing.T) {
	mr, store := newTestRedisTokenStore(t)
	ctx := context.Background()

	mr.Set("rt:1:t1", "1")
	mr.Set("rt:1:t2", "1")
	mr.Set("rt:1:t3", "1")
	mr.Set("rt:2:other", "1")

	err := store.RevokeAll(ctx, 1)
	if err != nil {
		t.Fatalf("revoke all: %v", err)
	}
	if mr.Exists("rt:1:t1") || mr.Exists("rt:1:t2") || mr.Exists("rt:1:t3") {
		t.Fatal("all user 1 tokens should be revoked")
	}
	if !mr.Exists("rt:2:other") {
		t.Fatal("other user's token should not be affected")
	}
}