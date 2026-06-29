package counter

import (
	"context"
	"fmt"

	"github.com/zhiguang/app/pkg/config"
)

// ============================================================================
// 限流
// ============================================================================

func (s *CounterService) rateLimiterKey(entityType, entityID string) string {
	return fmt.Sprintf("rl:sds-rebuild:%s:%s", entityType, entityID)
}

// allowedByRateLimiter 检查当前是否允许触发 SDS 重建（基于 Redis 的简单限流器）。
func (s *CounterService) allowedByRateLimiter(ctx context.Context, entityType, entityID string) bool {
	key := s.rateLimiterKey(entityType, entityID)

	windowSec := config.DefaultRebuildRateWindowSec
	permits := config.DefaultRebuildRatePermits
	if s.rebuildRateCfg != nil {
		if s.rebuildRateCfg.WindowSeconds > 0 {
			windowSec = s.rebuildRateCfg.WindowSeconds
		}
		if s.rebuildRateCfg.Permits > 0 {
			permits = s.rebuildRateCfg.Permits
		}
	}

	count, err := rateLimitScript.Run(ctx, s.redis, []string{key}, windowSec).Int()
	if err != nil {
		return true
	}
	return count <= permits
}
