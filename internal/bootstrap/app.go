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
	if vErr := cfg.Validate(); vErr != nil {
		return nil, vErr
	}

	db, err := database.NewDB(&cfg.Database, logger)
	if err != nil {
		return nil, err
	}
	redisClient, err := database.NewRedisClient(&cfg.Redis, logger)
	if err != nil {
		return nil, err
	}

	if mErr := database.RunMigrations(db, logger); mErr != nil {
		return nil, fmt.Errorf("database migration: %w", mErr)
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

	kpHandler, _, feedSvc := initKnowPost(db, redisClient, sharedFreeCache, hotKeyDetector, cfg, idGen, counterSvc, logger)

	relHandler, relSvc := initRelation(db, redisClient, idGen, logger, &cfg.Relation)

	// 扩散：写路径由 canal-outbox 消费者驱动，读路径注入 Feed 服务供 /feed/home 使用。
	fanoutSvc, timelineReader, fanoutConsumer := initFanout(redisClient, relSvc, counterSvc, cfg, logger)
	feedSvc.SetHomeTimelineReader(timelineReader)
	relSvc.SetFanoutHooks(fanoutSvc)

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
		fanoutConsumer.SetFailedMessageRecorder(outbox.NewDeadLetterRepository(db))
		backgroundRunners = append(backgroundRunners, fanoutConsumer)
	}

	// outbox 表清理：标准部署下 Canal 只读 binlog、从不删行，
	// 没有清理任务时该表只增不减（每次发帖/关注都插一行）。详见 outbox.Cleaner。
	if cleaner := outbox.NewCleaner(db, outbox.CleanerConfig{}, logger); cleaner != nil {
		backgroundRunners = append(backgroundRunners, cleaner)
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
	// 健康检查在路由装配阶段就已创建，而后台任务列表要到这里才齐备，
	// 因此状态来源在 App 构造完成后回注，让 /health/runners 能反映异步链路存活。
	healthChecker.SetRunnerReporter(app)
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
