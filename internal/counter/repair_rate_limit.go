// Package counter — 限流与退避辅助函数。
//
// 本文件包含 SDS 重建场景下的限流器实现，基于 Redis 原子计数限制单实体
// 每分钟的最大重建次数，防止失败重建请求过度消耗 Redis 资源。
package counter

import (
	"context"
	"fmt"
)

const (
	maxRebuildsPerMinute   = 5
	rateLimitWindowSeconds = 60
)

// ============================================================================
// 限流
// ============================================================================

func (s *CounterService) rateLimiterKey(entityType, entityID string) string {
	return fmt.Sprintf("rl:sds-rebuild:%s:%s", entityType, entityID)
}

// allowedByRateLimiter 检查是否允许触发 SDS 重建。
// 使用 Lua 脚本原子递增计数，首次设置 60s 过期，阈值 5 次/分钟。
// Lua 执行失败时降级为允许（宁可多重建也不拒绝）。
func (s *CounterService) allowedByRateLimiter(ctx context.Context, entityType, entityID string) bool {
	key := s.rateLimiterKey(entityType, entityID)
	val, err := rateLimitScript.Run(ctx, s.redis, []string{key}, rateLimitWindowSeconds).Int()
	if err != nil {
		return true
	}
	return val <= maxRebuildsPerMinute
}
