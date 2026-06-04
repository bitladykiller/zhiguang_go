package auth

import (
	"context"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
)

// RefreshTokenStore 定义刷新令牌白名单管理接口。
//
// 白名单模式解决了 JWT 本身无法被吊销的问题：颁发时将 token ID 存入 Redis，
// 吊销时从 Redis 删除。校验刷新令牌时检查其 token ID 是否仍存在于 Redis 中。
// 这支持以下能力：
//   - 令牌轮换（Token Rotation）：每次刷新时吊销旧令牌、颁发新令牌
//   - 定向吊销：使某个特定的刷新令牌失效
//   - 批量吊销：使用户的全部刷新令牌失效（例如重置密码后）
type RefreshTokenStore interface {
	StoreToken(userID uint64, tokenID string, ttl time.Duration) error
	IsTokenValid(userID uint64, tokenID string) bool
	RevokeToken(userID uint64, tokenID string) error
	RevokeAll(userID uint64) error
}

// RedisRefreshTokenStore 使用 Redis 实现 RefreshTokenStore。
// 键格式：`rt:{userID}:{tokenID}` -> "1"
//
// WHY：以 userID 作为 key 前缀可以使同一个用户的所有令牌 ID 聚集在一起，
// RevokeAll 可以通过 SCAN rt:{userID}:* 模式高效遍历删除。
type RedisRefreshTokenStore struct {
	redis *redis.Client
}

// NewRedisRefreshTokenStore 创建一个基于 Redis 的刷新令牌存储。
func NewRedisRefreshTokenStore(redisClient *redis.Client) *RedisRefreshTokenStore {
	return &RedisRefreshTokenStore{redis: redisClient}
}

// StoreToken 按给定 TTL 保存刷新令牌 ID。
func (s *RedisRefreshTokenStore) StoreToken(userID uint64, tokenID string, ttl time.Duration) error {
	key := fmt.Sprintf("rt:%d:%s", userID, tokenID)
	return s.redis.Set(context.Background(), key, "1", ttl).Err()
}

// IsTokenValid 检查刷新令牌是否仍存在于白名单中。
func (s *RedisRefreshTokenStore) IsTokenValid(userID uint64, tokenID string) bool {
	key := fmt.Sprintf("rt:%d:%s", userID, tokenID)
	exists, _ := s.redis.Exists(context.Background(), key).Result()
	return exists > 0
}

// RevokeToken 从白名单中移除单个刷新令牌。
func (s *RedisRefreshTokenStore) RevokeToken(userID uint64, tokenID string) error {
	key := fmt.Sprintf("rt:%d:%s", userID, tokenID)
	return s.redis.Del(context.Background(), key).Err()
}

// RevokeAll 使用 SCAN 非阻塞遍历并删除某个用户的全部刷新令牌。
func (s *RedisRefreshTokenStore) RevokeAll(userID uint64) error {
	pattern := fmt.Sprintf("rt:%d:*", userID)
	ctx := context.Background()
	iter := s.redis.Scan(ctx, 0, pattern, 100).Iterator()
	for iter.Next(ctx) {
		s.redis.Del(ctx, iter.Val())
	}
	return iter.Err()
}
