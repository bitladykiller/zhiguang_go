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

// FollowerCount 返回用户的粉丝数。
//
// 供扩散模块做「大 V」快速判定。计数缺失或 Redis 异常时返回 (0, false)，
// 调用方据此走不依赖该计数的慢路径，而不是把缺失当成 0 粉丝。
//
// WHY 要显式返回 known 标志：
//
//	粉丝计数是 Redis 中的增量值，可能因消费失败、键过期或重建而缺失。
//	若把「读不到」等同于「粉丝数为 0」，一个真实的大 V 会被判成普通作者，
//	触发一次几十万粉丝的写扩散风暴。
func (u *UserCounter) FollowerCount(ctx context.Context, userID uint64) (int64, bool) {
	if u == nil || u.svc == nil || u.svc.redis == nil {
		return 0, false
	}
	key := SdsKey("user", strconv.FormatUint(userID, 10))
	val, err := u.svc.redis.HGet(ctx, key, "follower").Int64()
	if err != nil {
		return 0, false
	}
	if val < 0 {
		return 0, true
	}
	return val, true
}
