package bootstrap

import (
	"strings"

	"github.com/coocood/freecache"
	"go.uber.org/zap"

	"github.com/zhiguang/app/internal/auth"
	"github.com/zhiguang/app/internal/cache"
	"github.com/zhiguang/app/internal/canal"
	"github.com/zhiguang/app/internal/counter"
	"github.com/zhiguang/app/internal/database"
	"github.com/zhiguang/app/internal/knowpost"
	"github.com/zhiguang/app/internal/llm"
	"github.com/zhiguang/app/internal/messaging"
	"github.com/zhiguang/app/internal/outbox"
	"github.com/zhiguang/app/internal/profile"
	"github.com/zhiguang/app/internal/relation"
	"github.com/zhiguang/app/internal/search"
	"github.com/zhiguang/app/internal/server"
	"github.com/zhiguang/app/internal/storage"
	"github.com/zhiguang/app/pkg/config"
)

// InitializeApp 通过统一的 bootstrap 路径完成应用装配。
// WHY：把装配逻辑集中在一个入口，能避免多套依赖图长期漂移。
func InitializeApp(configPath string) (*server.App, error) {
	logger, err := zap.NewProduction()
	if err != nil {
		return nil, err
	}

	cfg, err := config.LoadConfig(configPath)
	if err != nil {
		return nil, err
	}

	db, err := database.NewDB(&cfg.Database)
	if err != nil {
		return nil, err
	}

	redisClient := database.NewRedisClient(&cfg.Redis)
	kafkaWriter := messaging.NewKafkaWriter(&cfg.Kafka)
	canalOutboxWriter := messaging.NewTopicWriter(&cfg.Kafka, outbox.CanalOutboxTopic, false)

	detailCache := freecache.NewCache(cfg.Cache.L2.PublicCfg.MaxSize * 1024 * 1024)
	feedPublicCache := freecache.NewCache(cfg.Cache.L2.PublicCfg.MaxSize * 1024 * 1024)
	feedMineCache := freecache.NewCache(cfg.Cache.L2.MineCfg.MaxSize * 1024 * 1024)
	hotKeyDetector := cache.NewHotKeyDetector(&cfg.Cache.HotKey)

	jwtSvc, err := auth.NewJwtService(&cfg.Auth.Jwt)
	if err != nil {
		return nil, err
	}

	verifSvc := auth.NewVerificationService(redisClient, &cfg.Auth.Verification)
	tokenStore := auth.NewRedisRefreshTokenStore(redisClient)
	authRepo := auth.NewAuthRepository(db)
	authSvc := auth.NewAuthService(authRepo, verifSvc, jwtSvc, tokenStore, &cfg.Auth)
	authHandler := auth.NewAuthHandler(authSvc, jwtSvc)

	idGen, err := knowpost.NewSnowflakeIdGenerator()
	if err != nil {
		return nil, err
	}

	kpSvc := knowpost.NewKnowPostService(db, idGen, redisClient, detailCache, hotKeyDetector, &cfg.OSS)
	feedSvc := knowpost.NewKnowPostFeedService(knowpost.NewKnowPostRepository(db), redisClient, feedPublicCache, feedMineCache, hotKeyDetector)
	kpHandler := knowpost.NewKnowPostHandler(kpSvc, feedSvc)
	kpSvc.SetFeedCacheInvalidator(feedSvc)

	counterProducer := counter.NewCounterEventProducer(kafkaWriter)
	counterSvc := counter.NewCounterService(redisClient, counterProducer)
	counterHandler := counter.NewCounterHandler(counterSvc)
	kpSvc.SetCounterClient(counterSvc)
	feedSvc.SetCounterClient(counterSvc)

	relSvc := relation.NewRelationService(db, redisClient, 10*1024*1024)
	relHandler := relation.NewRelationHandler(relSvc)

	var searchSvc *search.SearchService
	if hasElasticsearchConfig(cfg) {
		searchSvc, err = search.NewSearchService(struct {
			URIs      []string
			IndexName string
		}{URIs: cfg.Elasticsearch.URIs, IndexName: cfg.Elasticsearch.IndexName})
		if err != nil {
			logger.Warn("Failed to initialize search service (ES may be unavailable)", zap.Error(err))
			searchSvc = nil
		} else {
			searchSvc.SetCounterClient(counterSvc)
		}
	} else {
		logger.Warn("Search service disabled: elasticsearch config is incomplete")
	}
	searchHandler := search.NewSearchHandler(searchSvc)
	searchProjector := search.NewKnowPostProjector(db, searchSvc, counterSvc)
	searchOutboxConsumer := search.NewOutboxConsumer(
		messaging.NewKafkaReaderWithGroup(&cfg.Kafka, outbox.CanalOutboxTopic, outbox.SearchOutboxConsumerGroup),
		searchProjector,
		logger,
	)
	relationEventProcessor := relation.NewEventProcessor(redisClient, counterSvc)
	relationOutboxConsumer := relation.NewOutboxConsumer(
		messaging.NewKafkaReaderWithGroup(&cfg.Kafka, outbox.CanalOutboxTopic, outbox.RelationOutboxConsumerGroup),
		relationEventProcessor,
		logger,
	)
	canalBridge := canal.NewBridge(&cfg.Canal, canalOutboxWriter, logger)

	descSvc := buildDescriptionService(cfg, logger)
	ragQuerySvc := buildRagQueryService(cfg, logger)
	llmHandler := llm.NewLlmHandler(descSvc, ragQuerySvc)

	ossSvc := buildOssService(cfg, logger)
	storageHandler := storage.NewStorageHandler(ossSvc)

	profileRepo := profile.NewProfileRepository(db)
	profileSvc := profile.NewProfileService(profileRepo)
	profileHandler := profile.NewProfileHandler(profileSvc)

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

	router := server.NewRouter(handlerSet, logger, jwtSvc)
	backgroundRunners := make([]server.BackgroundRunner, 0, 3)
	if cfg.Canal.Enabled {
		backgroundRunners = append(backgroundRunners, canalBridge, relationOutboxConsumer, searchOutboxConsumer)
	} else {
		logger.Warn("canal is disabled: outbox async sync pipeline will not start")
	}

	app := server.NewApp(router, cfg, logger, backgroundRunners...)
	return app, nil
}

func hasElasticsearchConfig(cfg *config.Config) bool {
	return len(cfg.Elasticsearch.URIs) > 0 && strings.TrimSpace(cfg.Elasticsearch.URIs[0]) != "" &&
		strings.TrimSpace(cfg.Elasticsearch.IndexName) != ""
}

func buildDescriptionService(cfg *config.Config, logger *zap.Logger) *llm.KnowPostDescriptionService {
	if strings.TrimSpace(cfg.LLM.DeepSeek.APIKey) == "" ||
		strings.TrimSpace(cfg.LLM.DeepSeek.BaseURL) == "" ||
		strings.TrimSpace(cfg.LLM.DeepSeek.Model) == "" {
		logger.Warn("LLM description service disabled: DeepSeek config is incomplete")
		return nil
	}
	return llm.NewKnowPostDescriptionService(&cfg.LLM)
}

func buildRagQueryService(cfg *config.Config, logger *zap.Logger) *llm.RagQueryService {
	if !hasElasticsearchConfig(cfg) {
		logger.Warn("RAG query service disabled: elasticsearch config is incomplete")
		return nil
	}
	if strings.TrimSpace(cfg.LLM.OpenAI.APIKey) == "" || strings.TrimSpace(cfg.LLM.OpenAI.BaseURL) == "" {
		logger.Warn("RAG query service disabled: embedding config is incomplete")
		return nil
	}
	if strings.TrimSpace(cfg.LLM.DeepSeek.APIKey) == "" ||
		strings.TrimSpace(cfg.LLM.DeepSeek.BaseURL) == "" ||
		strings.TrimSpace(cfg.LLM.DeepSeek.Model) == "" {
		logger.Warn("RAG query service disabled: chat model config is incomplete")
		return nil
	}
	return llm.NewRagQueryService(&cfg.LLM, cfg.Elasticsearch.URIs[0])
}

func buildOssService(cfg *config.Config, logger *zap.Logger) *storage.OssStorageService {
	if strings.TrimSpace(cfg.OSS.Endpoint) == "" ||
		strings.TrimSpace(cfg.OSS.AccessKeyID) == "" ||
		strings.TrimSpace(cfg.OSS.AccessKeySecret) == "" ||
		strings.TrimSpace(cfg.OSS.Bucket) == "" {
		logger.Warn("Storage service disabled: OSS config is incomplete")
		return nil
	}

	ossSvc, err := storage.NewOssStorageService(&cfg.OSS)
	if err != nil {
		logger.Warn("Failed to initialize OSS service", zap.Error(err))
		return nil
	}
	return ossSvc
}
