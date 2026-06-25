package counter

import (
	"context"
	"sync"

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
//   - Worker Pool（工作池模式）：Kafka 事件发布通过有界 channel + 固定 goroutine 池执行，
//     避免高并发下 goroutine 无限制增长。
//
// 数据流：
//
//	toggle (Lua) → 修改位图 → 发送 Kafka 事件（异步 worker pool） → 消费者批量聚合 → flush 到 cnt:*
type CounterService struct {
	redis              *redis.Client
	producer           CounterEventPublisher
	rebuildLockOptions redislock.Options
	failureRecorder    CounterFailureRecorder
	failureTopic       string
	messageIDGenerator MessageIDGenerator
	logger             *zap.Logger

	workerCh chan *CounterEvent
	workerWg sync.WaitGroup
}

// CounterServiceOption 定义 CounterService 的可选依赖注入函数。
type CounterServiceOption func(*CounterService)

// WithFailureRecorder 设置失败消息持久化器，nil 表示不做失败记录。
func WithFailureRecorder(r CounterFailureRecorder) CounterServiceOption {
	return func(s *CounterService) {
		s.failureRecorder = r
	}
}

// WithFailureTopic 设置失败消息对应的 Kafka topic 名。
func WithFailureTopic(topic string) CounterServiceOption {
	return func(s *CounterService) {
		s.failureTopic = topic
	}
}

// WithMessageIDGenerator 设置消息 ID 生成器，nil 表示使用随机 UUID。
func WithMessageIDGenerator(gen MessageIDGenerator) CounterServiceOption {
	return func(s *CounterService) {
		s.messageIDGenerator = gen
	}
}

// WithLogger 设置日志记录器。
func WithLogger(l *zap.Logger) CounterServiceOption {
	return func(s *CounterService) {
		s.logger = l
	}
}

// WithRebuildLockConfig 根据配置设置重建分布式锁选项。
func WithRebuildLockConfig(cfg *config.CounterConfig) CounterServiceOption {
	return func(s *CounterService) {
		s.rebuildLockOptions = counterRebuildLockOptions(cfg)
	}
}

// NewCounterService 创建计数器服务实例。
//
// 参数：
//   - rdb: Redis 客户端，用于执行 Lua 脚本和 SDS/Bitmap 操作
//   - producer: Kafka 事件生产者，用于异步发布计数变更事件
//   - opts: 可选依赖注入函数（failureRecorder、failureTopic、messageIDGenerator、logger、rebuildLockConfig）
func NewCounterService(
	rdb *redis.Client,
	producer CounterEventPublisher,
	opts ...CounterServiceOption,
) *CounterService {
	svc := &CounterService{
		redis:    rdb,
		producer: producer,
		workerCh: make(chan *CounterEvent, 256),
	}
	for _, o := range opts {
		o(svc)
	}
	svc.startWorkers()
	return svc
}

const counterWorkerPoolSize = 64

func (s *CounterService) startWorkers() {
	for i := 0; i < counterWorkerPoolSize; i++ {
		s.workerWg.Add(1)
		go func() {
			defer s.workerWg.Done()
			for evt := range s.workerCh {
				pubCtx, cancel := context.WithTimeout(context.Background(), publishTimeout)
				s.publishCounterEvent(pubCtx, evt)
				cancel()
			}
		}()
	}
}

// StopWorkers 停止后台 worker pool，等待所有正在进行的发布完成。
// 应在服务关闭时调用。
func (s *CounterService) StopWorkers() {
	close(s.workerCh)
	s.workerWg.Wait()
}
