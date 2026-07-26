package bootstrap

import (
	"time"

	"github.com/redis/go-redis/v9"
	"go.uber.org/zap"

	"github.com/zhiguang/app/internal/counter"
	"github.com/zhiguang/app/internal/fanout"
	"github.com/zhiguang/app/internal/messaging"
	"github.com/zhiguang/app/internal/outbox"
	"github.com/zhiguang/app/internal/relation"
	"github.com/zhiguang/app/pkg/config"
)

// initFanout 装配推拉结合的信息流扩散。
//
// 数据流：
//
//	发布知文（事务内写 outbox 表）
//	  → Canal 捕获 binlog → Kafka canal-outbox
//	  → fanout.Consumer 过滤 KnowPostPublished
//	  → fanout.Service.FanoutPost
//	      · 无条件写作者发件箱 authorbox:{authorID}（拉路数据源）
//	      · 普通作者：推送到粉丝收件箱 timeline:{fanID}
//	      · 大 V：跳过推送，由读者在读取时拉取
//	读取首页
//	  → fanout.TimelineReader.HomeTimeline：收件箱 ⊕ 所关注大 V 的发件箱 → 归并分页
//
// WHY 复用 canal-outbox 而非独立的 fanout 主题：
//
//	原设计留了一个 fanout 主题和一个 FanoutPublisher，但生产代码里从无调用方，
//	该主题永远没有生产者，收件箱恒为空，写扩散形同虚设。
//	即使补上生产者，「事务提交 + Kafka 投递」也是一次双写，中间崩溃就会丢事件。
//	复用事务性 outbox 链路后，投递保证与搜索投影完全一致。
//
// Kafka 未配置时返回 nil consumer，扩散写路径静默关闭；
// 此时读路径仍可工作（收件箱为空，只走拉路），不阻塞服务启动。
func initFanout(
	redisClient redis.UniversalClient,
	relSvc *relation.RelationService,
	counterSvc *counter.CounterService,
	cfg *config.Config,
	logger *zap.Logger,
) (*fanout.Service, *fanout.TimelineReader, *fanout.Consumer) {
	fanoutCfg := fanoutConfigFrom(cfg)

	var followerCounter fanout.FollowerCounter
	if counterSvc != nil {
		followerCounter = counter.NewUserCounter(counterSvc)
	}

	svc := fanout.NewService(redisClient, relSvc, followerCounter, logger, fanoutCfg)
	reader := fanout.NewTimelineReader(redisClient, relSvc, svc.Celebrities(), logger, fanoutCfg)

	if len(cfg.Kafka.Brokers) == 0 {
		logger.Warn("Kafka 未配置：扩散写路径已禁用，首页信息流将只走拉路")
		return svc, reader, nil
	}

	consumer := fanout.NewConsumer(
		messaging.NewKafkaReaderWithGroupFromLatest(&cfg.Kafka, outbox.CanalOutboxTopic, outbox.FanoutConsumerGroup),
		svc,
		logger,
	)
	return svc, reader, consumer
}

// fanoutConfigFrom 把全局配置映射为扩散参数，未配置的字段回落到默认值。
func fanoutConfigFrom(cfg *config.Config) fanout.Config {
	out := fanout.DefaultConfig()
	if cfg == nil {
		return out
	}
	fc := cfg.Fanout
	if fc.CelebrityThreshold > 0 {
		out.CelebrityThreshold = fc.CelebrityThreshold
	}
	if fc.BatchSize > 0 {
		out.FanoutBatchSize = fc.BatchSize
	}
	if fc.MaxFans > 0 {
		out.FanoutMaxFans = fc.MaxFans
	}
	if fc.TimelineMaxItems > 0 {
		out.TimelineMaxItems = fc.TimelineMaxItems
	}
	if fc.TimelineTTLHours > 0 {
		out.TimelineTTL = time.Duration(fc.TimelineTTLHours) * time.Hour
	}
	if fc.AuthorBoxMaxItems > 0 {
		out.AuthorBoxMaxItems = fc.AuthorBoxMaxItems
	}
	if fc.AuthorBoxTTLHours > 0 {
		out.AuthorBoxTTL = time.Duration(fc.AuthorBoxTTLHours) * time.Hour
	}
	if fc.FollowBackfillLimit >= 0 {
		out.FollowBackfillLimit = fc.FollowBackfillLimit
	}
	if fc.MaxPullAuthors > 0 {
		out.MaxPullAuthors = fc.MaxPullAuthors
	}
	return out
}
