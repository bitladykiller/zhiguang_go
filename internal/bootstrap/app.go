package bootstrap

import (
	"context"
	"fmt"
	"time"

	"github.com/coocood/freecache"
	"go.uber.org/zap"

	"github.com/zhiguang/app/internal/cache"
	"github.com/zhiguang/app/internal/canal"
	"github.com/zhiguang/app/internal/database"
	"github.com/zhiguang/app/internal/messaging"
	"github.com/zhiguang/app/internal/outbox"
	"github.com/zhiguang/app/internal/server"
	"github.com/zhiguang/app/pkg/config"
	pkgmw "github.com/zhiguang/app/pkg/middleware"
)

func InitializeApp(configPath string) (*server.App, error) {
	logger, err := zap.NewProduction()
	if err != nil {
		return nil, err
	}

	cfg, err := config.LoadConfig(configPath)
	if err != nil {
		return nil, err
	}
	cfg.ApplyDefaults()
	if err := cfg.Validate(); err != nil {
		return nil, err
	}

	db, err := database.NewDB(&cfg.Database, logger)
	if err != nil {
		return nil, err
	}
	redisClient, err := database.NewRedisClient(&cfg.Redis, logger)
	if err != nil {
		return nil, err
	}

	if err := database.RunMigrations(db, logger); err != nil {
		return nil, fmt.Errorf("database migration: %w", err)
	}

	kafkaWriter := messaging.NewKafkaWriter(&cfg.Kafka)
	canalOutboxWriter := messaging.NewTopicWriter(&cfg.Kafka, outbox.CanalOutboxTopic, false)

	sharedFreeCache := newFreeCacheWithConfig(cfg)
	hotKeyDetector := cache.NewHotKeyDetector(&cfg.Cache.HotKey, redisClient, logger)

	authHandler, jwtSvc, err := initAuth(db, redisClient, cfg, logger)
	if err != nil {
		return nil, err
	}

	idGen, err := initIDGenerator(cfg, logger)
	if err != nil {
		return nil, err
	}

	counterHandler, counterSvc, counterAggConsumer, err := initCounter(db, redisClient, kafkaWriter, idGen, cfg, logger)
	if err != nil {
		return nil, err
	}

	kpHandler, _, _ := initKnowPost(db, redisClient, sharedFreeCache, hotKeyDetector, cfg, idGen, counterSvc, logger)

	relHandler, relSvc := initRelation(db, redisClient, idGen, logger, &cfg.Relation)

	fanoutConsumer := initFanout(redisClient, relSvc, cfg, logger)

	initCtx, initCancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer initCancel()
	searchHandler, searchOutboxConsumer, relationOutboxConsumer := initSearch(initCtx, db, redisClient, counterSvc, cfg, logger)

	llmHandler := initLLM(cfg, logger)
	storageHandler := initStorage(cfg, logger)
	profileHandler := initProfile(db)

	handlerSet := &server.HandlerSet{
		Auth:     authHandler,
		KnowPost: kpHandler,
		Counter:  counterHandler,
		Relation: relHandler,
		Search:   searchHandler,
		LLM:      llmHandler,
		Storage:  storageHandler,
		Profile:  profileHandler,
	}

	if cfg.Server.RateLimit.Enabled {
		rateLimiter := pkgmw.NewRateLimiter(redisClient, cfg.Server.RateLimit, logger)
		handlerSet.RateLimiter = rateLimiter
	}

	healthChecker := server.NewHealthChecker(db, redisClient)
	router := server.NewRouter(handlerSet, logger, jwtSvc, healthChecker, cfg)

	backgroundRunners := make([]server.BackgroundRunner, 0, 4)
	backgroundRunners = append(backgroundRunners, counterAggConsumer, &hotKeyRunner{d: hotKeyDetector})

	if fanoutConsumer != nil {
		backgroundRunners = append(backgroundRunners, fanoutConsumer)
	}

	if cfg.Canal.Enabled {
		canalBridge := canal.NewBridge(&cfg.Canal, canalOutboxWriter, logger)
		backgroundRunners = append(backgroundRunners, canalBridge, relationOutboxConsumer)
		if searchOutboxConsumer != nil {
			backgroundRunners = append(backgroundRunners, searchOutboxConsumer)
		}
	} else {
		logger.Info("canal is disabled: outbox async sync pipeline will not start")
	}

	app := server.NewApp(router, cfg, logger, backgroundRunners...)
	app.AddCleanup(
		func(context.Context) error { return kafkaWriter.Close() },
		func(context.Context) error { return canalOutboxWriter.Close() },
		func(context.Context) error { return redisClient.Close() },
		func(context.Context) error { return db.Close() },
	)
	return app, nil
}

func newFreeCacheWithConfig(cfg *config.Config) *freecache.Cache {
	totalMB := cfg.Cache.L2.PublicCfg.MaxSize + cfg.Cache.L2.MineCfg.MaxSize
	if totalMB <= 0 {
		if cfg.Cache.L2.PublicCfg.FreeCacheDefaultMB > 0 {
			totalMB = cfg.Cache.L2.PublicCfg.FreeCacheDefaultMB
		} else {
			totalMB = 32
		}
	}
	return freecache.NewCache(totalMB * 1024 * 1024)
}