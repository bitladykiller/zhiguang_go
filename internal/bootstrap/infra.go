package bootstrap

import (
	"context"

	"github.com/coocood/freecache"
	"github.com/jmoiron/sqlx"
	"github.com/redis/go-redis/v9"
	"github.com/segmentio/kafka-go"
	"go.uber.org/zap"

	"github.com/zhiguang/app/internal/cache"
	"github.com/zhiguang/app/internal/database"
	"github.com/zhiguang/app/internal/knowpost"
	"github.com/zhiguang/app/internal/messaging"
	"github.com/zhiguang/app/internal/outbox"
	"github.com/zhiguang/app/internal/server"
	"github.com/zhiguang/app/pkg/config"
)

// InfraDeps 汇总跨领域共享的基础设施依赖。
//
// 这里存放的是“进程级共享资源”，例如 DB、Redis、Kafka writer、本地缓存和
// 雪花 ID 生成器。领域 wiring 会从中选择自己真正需要的依赖再完成构造。
type InfraDeps struct {
	Config            *config.Config
	Logger            *zap.Logger
	DB                *sqlx.DB
	Redis             *redis.Client
	KafkaWriter       *kafka.Writer
	CanalOutboxWriter *kafka.Writer
	DetailCache       *freecache.Cache
	FeedPublicCache   *freecache.Cache
	FeedMineCache     *freecache.Cache
	HotKeyDetector    *cache.HotKeyDetector
	IDGen             *knowpost.SnowflakeIdGenerator
}

// buildInfra 创建跨领域共享的基础设施，并返回对应的清理函数。
//
// 这样可以把“资源创建”和“业务装配”拆开：
//   - infra.go 只负责连接和客户端
//   - *_wiring.go 只负责把这些资源组织成领域模块
func buildInfra(cfg *config.Config, logger *zap.Logger) (*InfraDeps, []server.CleanupFunc, error) {
	db, err := database.NewDB(&cfg.Database)
	if err != nil {
		return nil, nil, err
	}

	redisClient := database.NewRedisClient(&cfg.Redis)
	kafkaWriter := messaging.NewKafkaWriter(&cfg.Kafka)
	canalOutboxWriter := messaging.NewTopicWriter(&cfg.Kafka, outbox.CanalOutboxTopic, false)

	idGen, err := knowpost.NewSnowflakeIdGenerator(&cfg.IDGenerator)
	if err != nil {
		_ = kafkaWriter.Close()
		_ = canalOutboxWriter.Close()
		_ = redisClient.Close()
		_ = db.Close()
		return nil, nil, err
	}

	logger.Info("snowflake generator initialized",
		zap.Int("machine_id", idGen.MachineID()),
		zap.Int("worker_id", idGen.WorkerID()),
		zap.Int64("node_id", idGen.NodeID()),
	)

	infra := &InfraDeps{
		Config:            cfg,
		Logger:            logger,
		DB:                db,
		Redis:             redisClient,
		KafkaWriter:       kafkaWriter,
		CanalOutboxWriter: canalOutboxWriter,
		DetailCache:       freecache.NewCache(cfg.Cache.L2.PublicCfg.MaxSize * 1024 * 1024),
		FeedPublicCache:   freecache.NewCache(cfg.Cache.L2.PublicCfg.MaxSize * 1024 * 1024),
		FeedMineCache:     freecache.NewCache(cfg.Cache.L2.MineCfg.MaxSize * 1024 * 1024),
		HotKeyDetector:    cache.NewHotKeyDetector(&cfg.Cache.HotKey, redisClient),
		IDGen:             idGen,
	}

	cleanup := []server.CleanupFunc{
		func(context.Context) error { return kafkaWriter.Close() },
		func(context.Context) error { return canalOutboxWriter.Close() },
		func(context.Context) error { return redisClient.Close() },
		func(context.Context) error { return db.Close() },
	}

	return infra, cleanup, nil
}
