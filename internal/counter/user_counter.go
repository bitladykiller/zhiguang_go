// Package counter — 用户维度计数操作。
//
// 本文件专门处理用户维度的计数指标（following、follower、posts），
// 与实体维度的 toggle 操作（like、fav）分离到不同文件。
//
// 关注/粉丝计数经 ApplyFollowDeltaOnce（去重落标 + 双向 HINCRBY 同段 Lua）由
// relation 消费端触发；早期的裸 IncrementFollowings/IncrementFollowers 已随
// 幂等协议纠序移除——裸增量无法与去重标记原子绑定，正是旧协议丢事件的根源。
package counter

import (
	"context"
	"fmt"
	"strconv"
	"time"

	"github.com/redis/go-redis/v9"
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

// applyFollowDeltaScript 原子执行「去重落标 + 关注/粉丝双向计数」。
//
//	KEYS[1] 去重标记键（由调用方给出，标识一次业务事件）
//	KEYS[2] 发起方用户计数 Hash（following 字段）
//	KEYS[3] 目标方用户计数 Hash（follower 字段）
//	ARGV[1] 标记 TTL（秒）
//	ARGV[2] delta（+1 / -1）
//
// 返回 1=首次执行（计数已完成），0=重复投递（跳过计数）。
var applyFollowDeltaScript = redis.NewScript(`
if redis.call('SET', KEYS[1], '1', 'NX', 'EX', ARGV[1]) then
  redis.call('HINCRBY', KEYS[2], 'following', ARGV[2])
  redis.call('HINCRBY', KEYS[3], 'follower', ARGV[2])
  return 1
end
return 0
`)

// ApplyFollowDeltaOnce 原子地「若首次见到 dedupeKey，则对关注双方计数 ±delta」。
//
// WHY 计数必须与去重标记在同一段 Lua 里：
//
//	HIncrBy 不幂等。若「落标」与「计数」分两次调用，进程在两者之间崩溃后
//	重投会命中标记而跳过计数——关注数从此永久少一。原子合并后只有两种终态：
//	「标记+计数都发生」或「都未发生」，重投在前者跳过、在后者重做，恰好各得其所。
//
// WHY 方法放在 counter 而不是调用方（relation）：
//
//	cnt:user:{id} 的键 schema 归 counter 所有。调用方若自拼键名，
//	等于把本模块的存储布局复制了一份出去——布局演进时必然漏改。
func (u *UserCounter) ApplyFollowDeltaOnce(ctx context.Context, dedupeKey string, ttl time.Duration, fromUserID, toUserID uint64, delta int64) (first bool, err error) {
	if u == nil || u.svc == nil || u.svc.redis == nil {
		return false, nil
	}
	res, err := applyFollowDeltaScript.Run(ctx, u.svc.redis,
		[]string{
			dedupeKey,
			SdsKey("user", strconv.FormatUint(fromUserID, 10)),
			SdsKey("user", strconv.FormatUint(toUserID, 10)),
		},
		int(ttl.Seconds()), delta,
	).Int()
	if err != nil {
		return false, fmt.Errorf("apply follow delta once: %w", err)
	}
	return res == 1, nil
}
