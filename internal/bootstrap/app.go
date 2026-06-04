// Package bootstrap 提供统一的依赖装配入口 InitializeApp。
//
// 该包负责在服务启动时完成以下工作：
//  1. 加载并解析 YAML 配置（config.LoadConfig）
//  2. 创建所有基础设施连接（MySQL、Redis、Kafka）
//  3. 创建所有缓存实例（freecache、HotKeyDetector）
//  4. 通过构造函数注入创建所有业务服务
//  5. 为可选能力（搜索、LLM、OSS）检测配置完整性，
//     配置不完整时自动降级为 nil，由 handler 层返回 503
//  6. 组装路由和后台消费者
//
// WHY 使用手动装配而非 DI 框架：
// 虽然 wire 等工具可以自动生成依赖图，但手动装配提供了：
//   - 完全可读的依赖关系——开发者可以 grep 追踪任意依赖的创建和注入点
//   - 精确控制生命周期——可以决定何时创建、如何关闭
//   - 无隐式行为——所有代码显式可见
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

// InitializeApp 完成整个应用的依赖装配，返回 *server.App 实例。
//
// 参数:
//   - configPath: YAML 配置文件路径，例如 "config.yaml" 或 "/etc/zhiguang/config.yaml"
//
// 返回值:
//   - *server.App: 已装配完成的应用实例，包含 Gin Router 和后台消费者
//   - error: 任何一个强制依赖创建失败时返回（如 MySQL 连接失败、JWT 密钥加载失败等）
//
// 装配流程:
//  1. 初始化日志（zap.NewProduction）和加载配置（config.LoadConfig）
//  2. 创建基础设施连接：MySQL（sqlx）、Redis（go-redis）、Kafka Writer
//  3. 创建缓存实例：freecache 作为 L2 缓存、HotKeyDetector 热点检测
//  4. 创建鉴权服务栈：JWT 签发/校验、验证码服务、刷新令牌白名单
//  5. 创建知文服务（含 ID 生成器、详情 & Feed 缓存、写操作 outbox 注入）
//  6. 创建计数器服务（Redis SDS + Bitmap + Kafka 事件发布）
//  7. 创建关系服务（关注/取关，事务内 outbox）
//  8. 可选能力检查：ES 搜索、LLM DeepSeek、OSS 存储（配置不完善时自动降级）
//  9. 创建资料服务（用户资料查询与编辑）
//
// 10. 组装 Gin 路由和后台消费者（Canal Bridge、Outbox Consumer）
//
// WHY 使用手动装配而非 DI 框架：
// 虽然 wire 等工具可以自动生成依赖图，但手动装配提供了：
//   - 完全可读的依赖关系——开发者可以 grep 追踪任意依赖的创建和注入点
//   - 精确控制生命周期——可以决定何时创建、如何关闭
//   - 无隐式行为——所有代码显式可见
//
// 重要:
//   - 强制依赖创建失败（如 MySQL）会直接返回 error，阻止服务启动
//   - 可选依赖创建失败（如 ES、DeepSeek）仅打印 warn 日志，不影响服务启动
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
	counterAggConsumer := counter.NewAggregationConsumer(
		messaging.NewKafkaReaderWithGroup(&cfg.Kafka, cfg.Kafka.Topics.CounterEvents, cfg.Kafka.ConsumerGroup),
		redisClient,
		logger,
	)
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
	backgroundRunners = append(backgroundRunners, counterAggConsumer)
	if cfg.Canal.Enabled {
		backgroundRunners = append(backgroundRunners, canalBridge, relationOutboxConsumer, searchOutboxConsumer)
	} else {
		logger.Warn("canal is disabled: outbox async sync pipeline will not start")
	}

	app := server.NewApp(router, cfg, logger, backgroundRunners...)
	return app, nil
}

// hasElasticsearchConfig 检查 Elasticsearch 是否已正确配置。
// 要求 URIs 列表非空且至少第一个 URI 不为空，且索引名已填写。
//
// 参数:
//   - cfg: 应用配置，包含 Elasticsearch 配置段
//
// 返回值:
//   - bool: 配置完整返回 true，否则返回 false
//
// 说明:
//
//	此函数用于在 InitializeApp 中判断是否应该初始化搜索服务（ES）和 RAG 问答服务。
//	仅检查 URIs[0] 和 IndexName 两个关键字段，skip 了用户名/密码等高级配置。
func hasElasticsearchConfig(cfg *config.Config) bool {
	return len(cfg.Elasticsearch.URIs) > 0 && strings.TrimSpace(cfg.Elasticsearch.URIs[0]) != "" &&
		strings.TrimSpace(cfg.Elasticsearch.IndexName) != ""
}

// buildDescriptionService 根据配置完整性创建 AI 摘要生成服务。
// 配置不完整时返回 nil 并打印警告日志，不阻塞服务启动。
//
// 参数:
//   - cfg: 应用配置（含 LLM.DeepSeek 配置段）
//   - logger: zap 日志实例，用于输出配置缺失警告
//
// 返回值:
//   - *llm.KnowPostDescriptionService: 非 nil 表示配置完整可用
//
// 依赖的配置字段:
//   - cfg.LLM.DeepSeek.APIKey: DeepSeek API 密钥（必填）
//   - cfg.LLM.DeepSeek.BaseURL: DeepSeek API 基础地址（必填）
//   - cfg.LLM.DeepSeek.Model: 模型名称（必填）
//
// 降级策略:
//
//	任一必填字段缺失 → 返回 nil → SuggestDescription 接口返回 503
func buildDescriptionService(cfg *config.Config, logger *zap.Logger) *llm.KnowPostDescriptionService {
	if strings.TrimSpace(cfg.LLM.DeepSeek.APIKey) == "" ||
		strings.TrimSpace(cfg.LLM.DeepSeek.BaseURL) == "" ||
		strings.TrimSpace(cfg.LLM.DeepSeek.Model) == "" {
		logger.Warn("LLM description service disabled: DeepSeek config is incomplete")
		return nil
	}
	return llm.NewKnowPostDescriptionService(&cfg.LLM)
}

// buildRagQueryService 根据配置完整性创建 RAG 问答服务。
// 需要 ES 地址、OpenAI embedding 配置、DeepSeek chat 配置三类信息同时完整。
//
// 参数:
//   - cfg: 应用配置（含 Elasticsearch、LLM.OpenAI、LLM.DeepSeek 配置段）
//   - logger: zap 日志实例
//
// 返回值:
//   - *llm.RagQueryService: 非 nil 表示配置完整可用
//
// 配置检查顺序:
//  1. ES 配置（URIs 和 IndexName）
//  2. OpenAI embedding 配置（APIKey 和 BaseURL）
//  3. DeepSeek chat 配置（APIKey、BaseURL、Model）
//
// 降级策略:
//
//	任意一类配置缺失将返回 nil，并打印具体缺失原因的警告日志。
//
// 调用方（RagQuery handler）在 svc 为 nil 时返回 503。
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

// buildOssService 根据配置完整性创建 OSS 存储服务。
//
// 参数:
//   - cfg: 应用配置（含 OSS 配置段）
//   - logger: zap 日志实例
//
// 返回值:
//   - *storage.OssStorageService: 非 nil 表示配置完整且客户端创建成功
//
// 配置检查顺序:
//  1. 检查 cfg.OSS.Endpoint、AccessKeyID、AccessKeySecret、Bucket 是否非空
//  2. 如果配置完整，调用 storage.NewOssStorageService 创建客户端
//  3. 如果客户端创建失败（如网络不通），打印 warn 日志并返回 nil
//
// 降级策略:
//
//	配置不完整或客户端创建失败 → 返回 nil → Presign 接口返回 503
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
