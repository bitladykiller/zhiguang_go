package auth

import (
	"context"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
	"go.uber.org/zap"
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
	StoreToken(ctx context.Context, userID uint64, tokenID string, ttl time.Duration) error
	IsTokenValid(ctx context.Context, userID uint64, tokenID string) bool
	RevokeToken(ctx context.Context, userID uint64, tokenID string) error
	RevokeAll(ctx context.Context, userID uint64) error
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

// StoreToken 保存刷新令牌 ID 到 Redis 白名单中。
//
// 键格式：rt:{userID}:{tokenID}
// 值固定为 "1"（仅作为占位符，表示该令牌存在）
// 借助 Redis TTL 自动过期机制实现刷新令牌的自然过期。
//
// 参数:
//   - userID: 用户 ID，作为 key 的前缀，便于按用户维度管理令牌（如 RevokeAll）
//   - tokenID: 刷新令牌的唯一标识（JWT 的 jti 声明）
//   - ttl: 令牌有效期，Redis 将在此时间后自动删除该键
//
// 返回值:
//   - error: Redis 操作异常时返回
func (s *RedisRefreshTokenStore) StoreToken(ctx context.Context, userID uint64, tokenID string, ttl time.Duration) error {
	key := fmt.Sprintf("rt:%d:%s", userID, tokenID)
	return s.redis.Set(ctx, key, "1", ttl).Err()
}

// IsTokenValid 检查刷新令牌是否仍存在于 Redis 白名单中。
//
// 通过 Redis EXISTS 命令判断键是否存在。存在表示令牌未被吊销且未过期。
// 已过期（TTL 归零自动删除）或已被吊销（RevokeToken/RevokeAll 删除）的令牌返回 false。
//
// 参数:
//   - userID: 用户 ID
//   - tokenID: 刷新令牌 ID（JWT 的 jti 声明）
//
// 返回值:
//   - bool: true 表示令牌有效（存在且未过期），false 表示已被吊销或已过期
//
// 边界情况:
//   - Redis 连接异常时 s.redis.Exists 返回 error，代码忽略后 exists 为 0，返回 false（fail-closed 策略）
func (s *RedisRefreshTokenStore) IsTokenValid(ctx context.Context, userID uint64, tokenID string) bool {
	key := fmt.Sprintf("rt:%d:%s", userID, tokenID)
	exists, err := s.redis.Exists(ctx, key).Result()
	if err != nil {
		zap.L().Warn("failed to check token existence", zap.String("key", key), zap.Error(err))
	}
	return exists > 0
}

// RevokeToken 从 Redis 白名单中删除单个刷新令牌，使其立即失效。
//
// 用于令牌轮换场景：刷新 access token 时，吊销旧 refresh token 并颁发新 refresh token。
// 直接使用 DEL 命令删除键，不关心键是否存在（DEL 对不存在的键返回 0 但不报错）。
//
// 参数:
//   - userID: 用户 ID
//   - tokenID: 要吊销的刷新令牌 ID
//
// 返回值:
//   - error: Redis 操作异常时返回
func (s *RedisRefreshTokenStore) RevokeToken(ctx context.Context, userID uint64, tokenID string) error {
	key := fmt.Sprintf("rt:%d:%s", userID, tokenID)
	return s.redis.Del(ctx, key).Err()
}

// RevokeAll 吊销指定用户的全部刷新令牌（使用 Redis SCAN 非阻塞遍历）。
//
// 适用场景：用户重置密码、账号被盗恢复、安全策略要求批量下线等。
//
// 使用 SCAN 而非 KEYS 的原因：
//   - KEYS 会阻塞 Redis 直到返回全部结果，大 key 数量下会阻塞其他请求
//   - SCAN 分批返回，每次只处理一小批，对主流程影响极小
//
// Redis SCAN 命令说明：
//   - 基于游标（cursor）分批遍历，与 KEYS 不同，SCAN 不会阻塞 Redis 主线程
//   - 第一个参数 cursor 初始为 0（开始遍历），后续每轮返回新的游标，游标变为 0 时遍历结束
//   - 第二个参数 pattern 使用通配符匹配键（本例为 rt:{userID}:*）
//   - 第三个参数 count 建议每批扫描条数（本例为 100），非精确值，Redis 会尽力返回约 count 条
//   - redis/go-redis/v9 的 Scan().Iterator() 封装了游标迭代逻辑，自动处理多轮遍历
//   - 注意 SCAN 在遍历期间不能保证不重复/不遗漏（弱一致性），但对令牌吊销场景可接受
//
// 参数:
//   - userID: 用户 ID，用于匹配 rt:{userID}:* 模式的所有键
//
// 返回值:
//   - error: 遍历异常（iter.Err()）时返回，删除失败不中断（日志记录即可）
//
// 边界情况:
//   - 如果用户没有任何白名单令牌，迭代器直接结束，不执行任何操作，返回 nil
//   - 遍历过程中新创建的令牌可能不会被本次扫描到（弱一致性），可忽略
func (s *RedisRefreshTokenStore) RevokeAll(ctx context.Context, userID uint64) error {
	pattern := fmt.Sprintf("rt:%d:*", userID)
	iter := s.redis.Scan(ctx, 0, pattern, 100).Iterator()
	for iter.Next(ctx) {
		if err := s.redis.Del(ctx, iter.Val()).Err(); err != nil {
			zap.L().Warn("failed to delete refresh token during RevokeAll", zap.String("key", iter.Val()), zap.Error(err))
		}
	}
	return iter.Err()
}
