// Package counter — 用户维度计数操作。
//
// 本文件专门处理用户维度的计数指标（following、follower、posts），
// 与实体维度的 toggle 操作（like、fav）分离到不同文件。
//
// 用户计数通过 INCR_SDS_FIELD_LUA 原子递增 SDS 中的槽位，
// 不经过 Kafka 异步链路（因为 relation.EventProcessor 已在消费端直接调用）。
package counter

import (
	"context"
	"fmt"
	"strconv"
)

// UserCounter 是 CounterService 提供的用户维度计数接口的轻量视图。
//
// 将用户计数操作从庞大的 CounterService 中分离出来，
// 使得 relation 包只需要注入这个窄接口，而不依赖整个 CounterService。
type UserCounter struct {
	svc *CounterService
}

// NewUserCounter 创建一个用户维度计数操作器。
func NewUserCounter(svc *CounterService) *UserCounter {
	return &UserCounter{svc: svc}
}

// IncrementFollowings 增量更新用户维度的关注数。
func (u *UserCounter) IncrementFollowings(ctx context.Context, userID uint64, delta int) error {
	return u.incrementUserMetric(ctx, userID, "following", delta)
}

// IncrementFollowers 增量更新用户维度的粉丝数。
func (u *UserCounter) IncrementFollowers(ctx context.Context, userID uint64, delta int) error {
	return u.incrementUserMetric(ctx, userID, "follower", delta)
}

// incrementUserMetric 增量更新用户维度的计数指标。
func (u *UserCounter) incrementUserMetric(ctx context.Context, userID uint64, metric string, delta int) error {
	if _, ok := nameToIdx[metric]; !ok {
		return fmt.Errorf("unknown metric: %s", metric)
	}
	key := SdsKey("user", strconv.FormatUint(userID, 10))
	return u.svc.redis.HIncrBy(ctx, key, metric, int64(delta)).Err()
}