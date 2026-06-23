package counter

import (
	"github.com/redis/go-redis/v9"
	"github.com/zhiguang/app/pkg/config"
	"github.com/zhiguang/app/pkg/redislock"
	"go.uber.org/zap"
)

// CounterService 提供原子化的计数开关操作。
//
// 设计模式：
//   - Strategy（策略模式）：Like/Unlike/Fav/Unfav 都是 toggle(add/remove) 的不同变体，
//     共用同一套位图操作逻辑，只是传入的 metric 和 op 参数不同。
//   - Circuit Breaker（断路器模式）：SDS 重建失败后采用指数退避，
//     退避时间呈指数增长（500ms → 1s → 2s → ... → 30s cap），
//     避免持续失败的请求压迫数据库。
//   - Distributed Lock（分布式锁模式）：通过 Redis SETNX 防止多个服务实例
//     同时对同一个 SDS 执行重建操作。
//
// 数据流：
//
//	toggle (Lua) → 修改位图 → 发送 Kafka 事件（异步） → 消费者批量聚合 → flush 到 cnt:*
type CounterService struct {
	redis              *redis.Client
	producer           CounterEventPublisher
	rebuildLockOptions redislock.Options
	failureRecorder    CounterFailureRecorder
	failureTopic       string
	messageIDGenerator MessageIDGenerator
	logger             *zap.Logger
}

// NewCounterService 创建计数器服务实例。
//
// 参数：
//   - rdb: Redis 客户端，用于执行 Lua 脚本和 SDS/Bitmap 操作
//   - producer: Kafka 事件生产者，用于异步发布计数变更事件
//   - failureRecorder: 失败消息持久化器，nil 表示不做失败记录
//   - failureTopic: 失败消息对应的 Kafka topic 名
//   - messageIDGenerator: 消息 ID 生成器，nil 表示使用随机 UUID
func NewCounterService(
	rdb *redis.Client,
	producer CounterEventPublisher,
	cfg *config.CounterConfig,
	failureRecorder CounterFailureRecorder,
	failureTopic string,
	messageIDGenerator MessageIDGenerator,
	logger *zap.Logger,
) *CounterService {
	return &CounterService{
		redis:              rdb,
		producer:           producer,
		rebuildLockOptions: rebuildLockOptions(cfg),
		failureRecorder:    failureRecorder,
		failureTopic:       failureTopic,
		messageIDGenerator: messageIDGenerator,
		logger:             logger,
	}
}
