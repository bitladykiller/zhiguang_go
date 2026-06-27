package bootstrap

import (
	"github.com/redis/go-redis/v9"
	"github.com/zhiguang/app/internal/fanout"
	"github.com/zhiguang/app/internal/outbox"
	"github.com/zhiguang/app/internal/relation"
	"github.com/zhiguang/app/internal/server"
	"github.com/zhiguang/app/pkg/config"
	"go.uber.org/zap"
)

// initFanout 初始化写扩散模块的消费者。
//
// 流程：
//  1. 用户发布知文时，在事务内向 outbox 表写入 FanoutEvent
//  2. Canal 捕获 outbox 变更后写入 Kafka topic "fanout"
//  3. FanoutConsumer 消费后，将 post_id 写入所有粉丝的 timeline ZSet
//
// 如果 Kafka 未配置，fanout 功能静默降级（不做扩散，走读扩散 fallback）。
func initFanout(
	redisClient redis.UniversalClient,
	relSvc *relation.RelationService,
	cfg *config.Config,
	logger *zap.Logger,
) server.BackgroundRunner {
	if len(cfg.Kafka.Brokers) == 0 {
		logger.Warn("kafka not configured, fanout disabled")
		return nil
	}

	fanoutCfg := fanout.DefaultConfig()
	fanoutSvc := fanout.NewService(redisClient, relSvc, logger, fanoutCfg)
	return fanout.NewFanoutConsumer(cfg.Kafka.Brokers, outbox.FanoutConsumerGroup, outbox.FanoutTopic, fanoutSvc, logger)
}
