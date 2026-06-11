package counter

import (
	"context"
	"fmt"
	"time"
)

func (s *CounterService) backoffKey(entityType, entityID string) string {
	return fmt.Sprintf("backoff:sds-rebuild:until:%s:%s", entityType, entityID)
}

func (s *CounterService) backoffExpKey(entityType, entityID string) string {
	return fmt.Sprintf("backoff:sds-rebuild:exp:%s:%s", entityType, entityID)
}

func (s *CounterService) rateLimiterKey(entityType, entityID string) string {
	return fmt.Sprintf("rl:sds-rebuild:%s:%s", entityType, entityID)
}

func (s *CounterService) inBackoff(ctx context.Context, entityType, entityID string) bool {
	until, err := s.redis.Get(ctx, s.backoffKey(entityType, entityID)).Int64()
	if err != nil {
		return false
	}
	return time.Now().UnixMilli() < until
}

func (s *CounterService) escalateBackoff(ctx context.Context, entityType, entityID string) {
	expKey := s.backoffExpKey(entityType, entityID)
	exp, _ := s.redis.Get(ctx, expKey).Int()

	ms := int64(500) << exp
	if ms > 30000 {
		ms = 30000
	}
	until := time.Now().UnixMilli() + ms

	s.redis.Set(ctx, s.backoffKey(entityType, entityID), until, 0)
	s.redis.Set(ctx, expKey, exp+1, 0)
	s.redis.Del(ctx, s.rateLimiterKey(entityType, entityID))
}

func (s *CounterService) resetBackoff(ctx context.Context, entityType, entityID string) {
	s.redis.Del(ctx, s.backoffKey(entityType, entityID))
	s.redis.Del(ctx, s.backoffExpKey(entityType, entityID))
	s.redis.Del(ctx, s.rateLimiterKey(entityType, entityID))
}

func (s *CounterService) allowedByRateLimiter(ctx context.Context, entityType, entityID string) bool {
	key := s.rateLimiterKey(entityType, entityID)
	val, err := rateLimitScript.Run(ctx, s.redis, []string{key}, 60).Int()
	if err != nil {
		return true
	}
	return val <= 5
}
