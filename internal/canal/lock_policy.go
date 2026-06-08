package canal

import (
	"fmt"
	"time"

	"github.com/zhiguang/app/pkg/redislock"
)

const (
	defaultCanalLeaderLockTTL  = 15 * time.Second
	defaultCanalLeaderOpTimout = time.Second
)

// leaderLockKey 返回 Canal bridge 的实例级 leader 锁 key。
//
// WHY 只允许一个实例持有：
//   - Canal -> Kafka 桥接本质上是在“复制 binlog 变更”。
//   - 多实例同时连接并处理同一 destination，会把同一批 outbox 事件重复写进 Kafka。
func (b *Bridge) leaderLockKey() string {
	destination := "default"
	if b != nil && b.cfg != nil && b.cfg.Destination != "" {
		destination = b.cfg.Destination
	}
	return fmt.Sprintf("lock:canal:bridge:%s", destination)
}

func canalLeaderLockOptions() redislock.Options {
	return redislock.Options{
		TTL:              defaultCanalLeaderLockTTL,
		WatchdogInterval: defaultCanalLeaderLockTTL / 3,
		OpTimeout:        defaultCanalLeaderOpTimout,
	}
}
