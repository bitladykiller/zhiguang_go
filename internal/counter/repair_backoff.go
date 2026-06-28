package counter

import (
	"context"
	"fmt"
	"time"

	"go.uber.org/zap"
)

// ============================================================================
// 退避
// ============================================================================

const (
	backoffBaseMs = 500
	backoffMaxMs  = 30000
)

func (s *CounterService) backoffKey(entityType, entityID string) string {
	return fmt.Sprintf("backoff:sds-rebuild:until:%s:%s", entityType, entityID)
}

func (s *CounterService) backoffExpKey(entityType, entityID string) string {
	return fmt.Sprintf("backoff:sds-rebuild:exp:%s:%s", entityType, entityID)
}

// inBackoff 检查当前实体是否处于退避期。
//
// 功能：
//
//	读取 Redis 中的退避截止时间戳（毫秒级 Unix 时间戳），
//	如果当前时间小于截止时间，返回 true，表示应跳过重建。
//
// 退避机制用于防止频繁失败的重建请求压垮 Redis。
// 当重建因限流、锁抢占或其他错误失败时，会设置退避期。
//
// 参数：
//   - entityType: 实体类型
//   - entityID:   实体 ID
//
// 返回值：
//   - bool: true=处于退避期，应跳过重建；false=不在退避期，可以重建
//
// 注意：
//
//	如果 Redis 中不存在退避键（GET 返回 redis.Nil），
//	.Int64() 会返回 0 和错误，此时视为不在退避期，返回 false。
func (s *CounterService) inBackoff(ctx context.Context, entityType, entityID string) bool {
	until, err := s.redis.Get(ctx, s.backoffKey(entityType, entityID)).Int64()
	if err != nil {
		return false
	}
	return time.Now().UnixMilli() < until
}

// escalateBackoff 提升指定实体的退避等级（指数退避）。
//
// 功能：
//  1. 读取当前的退避指数（exp），初始为 0。
//  2. 计算退避时长：baseMs << exp（指数增长），上限 30 秒。
//  3. 设置退避截止时间戳（当前时间 + 退避时长）。
//  4. 退避指数 +1（下次退避时间翻倍）。
//  5. 删除限流器键，让新的退避期从零开始累加。
//
// 指数退避时间序列：500ms → 1s → 2s → 4s → 8s → 16s → 30s（cap）
//
// 参数：
//   - entityType: 实体类型
//   - entityID:   实体 ID
//
// 函数调用说明：
//   - s.redis.Get(ctx, key).Int():
//     .Int() 将 Redis 返回的字符串值解析为 int 类型。
//     如果键不存在，返回 0（零值）。
//   - s.redis.Set(ctx, key, value, expiration):
//     Set 的过期时间为 0 表示永不过期，由 resetBackoff 或下次 escalate 时覆盖。
func (s *CounterService) escalateBackoff(ctx context.Context, entityType, entityID string) {
	expKey := s.backoffExpKey(entityType, entityID)
	attemptCount, _ := s.redis.Get(ctx, expKey).Int()
	if attemptCount > 62 {
		attemptCount = 62
	}

	ms := int64(backoffBaseMs) << attemptCount
	if ms > backoffMaxMs {
		ms = backoffMaxMs
	}
	until := time.Now().UnixMilli() + ms

	pipe := s.redis.Pipeline()
	pipe.Set(ctx, s.backoffKey(entityType, entityID), until, 2*time.Hour)
	pipe.Set(ctx, expKey, attemptCount+1, 2*time.Hour)
	pipe.Del(ctx, s.rateLimiterKey(entityType, entityID))
	if _, err := pipe.Exec(ctx); err != nil {
		s.logger.Warn("escalateBackoff pipeline exec failed", zap.Error(err))
	}
}

// resetBackoff 重置指定实体的退避状态。
//
// 功能：
//
//	删除 Redis 中的退避截止键、退避指数键和限流器键，
//	使该实体可以从零开始接受下一次重建。
//
// 调用时机：
//
//	当 SDS 成功重建后调用，清除之前的失败状态。
//
// 参数：
//   - entityType: 实体类型
//   - entityID:   实体 ID
func (s *CounterService) resetBackoff(ctx context.Context, entityType, entityID string) {
	pipe := s.redis.Pipeline()
	pipe.Del(ctx, s.backoffKey(entityType, entityID))
	pipe.Del(ctx, s.backoffExpKey(entityType, entityID))
	pipe.Del(ctx, s.rateLimiterKey(entityType, entityID))
	if _, err := pipe.Exec(ctx); err != nil {
		s.logger.Warn("resetBackoff pipeline exec failed", zap.Error(err))
	}
}