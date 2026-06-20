package counter

import (
	"context"
	"fmt"
	"time"
)

const (
	rebuildRateWindow  = 60 * time.Second
	rebuildRatePermits = 5
)

// ============================================================================
// 限流
// ============================================================================

func (s *CounterService) rateLimiterKey(entityType, entityID string) string {
	return fmt.Sprintf("rl:sds-rebuild:%s:%s", entityType, entityID)
}

// allowedByRateLimiter 检查当前是否允许触发 SDS 重建（基于 Redis 的简单限流器）。
//
// 功能：
//  1. 使用 Lua 脚本原子递增限流器计数。
//  2. 如果这是该时间窗口内的第一次递增（val==1），原子设置 60 秒过期时间。
//  3. 如果当前计数 <= 5（允许的每分钟最大重建次数），返回 true。
//
// 参数：
//   - ctx:        context.Context
//   - entityType: 实体类型
//   - entityID:   实体 ID
//
// 返回值：
//   - bool: true=允许重建，false=已超过限流阈值
//
// 函数调用说明：
//   - rateLimitScript.Run(ctx, redis, []string{key}, 60):
//     执行 RATE_LIMIT_LUA 脚本，原子执行 INCR + 条件 EXPIRE。
//     当 val == 1（第一次请求）时自动设置 60 秒过期时间。
//     后续请求只递增计数，不再重复设置过期时间。
//
// 边界情况：
//   - Lua 脚本执行失败时返回 0，视为允许（降级策略：宁可多重建也不拒绝）
func (s *CounterService) allowedByRateLimiter(ctx context.Context, entityType, entityID string) bool {
	key := s.rateLimiterKey(entityType, entityID)
	count, err := rateLimitScript.Run(ctx, s.redis, []string{key}, int(rebuildRateWindow/time.Second)).Int()
	if err != nil {
		return true
	}
	return count <= rebuildRatePermits
}
