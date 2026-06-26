// Package bootstrap 提供统一的依赖装配入口 InitializeApp。
//
// 该包负责在服务启动时完成以下工作：
//  1. 加载并解析 YAML 配置（config.LoadConfig）
//  2. 创建所有基础设施连接（MySQL、Redis、Kafka）
//  3. 创建所有缓存实例（freecache、HotKeyDetector）
//  4. 通过模块级 init* 函数创建所有业务服务
//  5. 为可选能力（搜索、LLM、OSS）检测配置完整性，
//     配置不完整时自动降级为 nil，由 handler 层返回 503
//  6. 组装路由和后台消费者
//
// 模块初始化逻辑已拆分到独立文件：
//   - init_auth.go: 鉴权模块
//   - init_knowpost.go: 知文模块 + ID 生成器
//   - init_counter.go: 计数器模块
//   - init_relation.go: 关系模块
//   - init_search.go: 搜索模块 + outbox 消费者
//   - init_optional.go: LLM + OSS 可选服务
//   - init_profile.go: 资料模块
//   - runners.go: 后台运行适配器
//
// WHY 使用手动装配而非 DI 框架：
// 虽然 wire 等工具可以自动生成依赖图，但手动装配提供了：
//   - 完全可读的依赖关系——开发者可以 grep 追踪任意依赖的创建和注入点
//   - 精确控制生命周期——可以决定何时创建、如何关闭
//   - 无隐式行为——所有代码显式可见
package bootstrap

import (
	"context"

	"github.com/coocood/freecache"
	"go.uber.org/zap"

	"github.com/zhiguang/app/internal/cache"
	"github.com/zhiguang/app/internal/canal"
	"github.com/zhiguang/app/internal/database"
	"github.com/zhiguang/app/internal/messaging"
	"github.com/zhiguang/app/internal/outbox"
	"github.com/zhiguang/app/internal/server"
	"github.com/zhiguang/app/pkg/config"
)

// InitializeApp 完成整个应用的依赖装配，返回 *server.App 实例。
//
// 装配流程：
//  1. 初始化日志和加载配置
//  2. 创建基础设施连接（MySQL、Redis、Kafka）
//  3. 创建缓存实例（freecache、HotKeyDetector）
//  4. 创建 ID 生成器 → 计数器服务 → 知文服务（解决循环依赖）
//  5. 创建其余模块（auth、relation、search、llm、storage、profile）
//  6. 组装 HandlerSet、路由、后台消费者
//  7. 注册清理回调
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
	if err := cfg.Validate(); err != nil {
		return nil, err
	}

	// ── 基础设施 ──
	db, err := database.NewDB(&cfg.Database)
	if err != nil {
		return nil, err
	}
	redisClient := database.NewRedisClient(&cfg.Redis)
	kafkaWriter := messaging.NewKafkaWriter(&cfg.Kafka)
	canalOutboxWriter := messaging.NewTopicWriter(&cfg.Kafka, outbox.CanalOutboxTopic, false)

	// ── 缓存 ──
	// 使用带前缀的单例 freecache，替代之前的 3 个独立实例。
	// 统一的内存池管理可以减少内存碎片，key 前缀隔离不同的缓存用途。
	sharedFreeCache := newFreeCacheWithConfig(cfg)
	hotKeyDetector := cache.NewHotKeyDetector(&cfg.Cache.HotKey, redisClient)

	// ── 模块初始化（按依赖拓扑序） ──
	authHandler, jwtSvc, err := initAuth(db, redisClient, cfg)
	if err != nil {
		return nil, err
	}

	// ID 生成器是 counter 和 knowpost 的共同依赖，先创建
	idGen, err := initIDGenerator(cfg, logger)
	if err != nil {
		return nil, err
	}

	// counter 先于 knowpost 创建（knowpost 需要 counter 读接口）
	counterHandler, counterSvc, counterAggConsumer, err := initCounter(db, redisClient, kafkaWriter, idGen, cfg, logger)
	if err != nil {
		return nil, err
	}

	// knowpost 构造时注入 counterSvc 和 feedSvc
	kpHandler, _, _ := initKnowPost(db, redisClient, sharedFreeCache, hotKeyDetector, cfg, idGen, counterSvc)

	relHandler, _ := initRelation(db, redisClient, idGen)

	searchHandler, searchOutboxConsumer, relationOutboxConsumer := initSearch(db, redisClient, counterSvc, cfg, logger)

	llmHandler := initLLM(cfg, logger)
	storageHandler := initStorage(cfg, logger)
	profileHandler := initProfile(db)

	// ── 路由与后台消费者 ──
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

	healthChecker := server.NewHealthChecker(db, redisClient)
	router := server.NewRouter(handlerSet, logger, jwtSvc, healthChecker)

	backgroundRunners := make([]server.BackgroundRunner, 0, 4)
	backgroundRunners = append(backgroundRunners, counterAggConsumer, &hotKeyRunner{d: hotKeyDetector})

	if cfg.Canal.Enabled {
		canalBridge := canal.NewBridge(&cfg.Canal, canalOutboxWriter, logger)
		backgroundRunners = append(backgroundRunners, canalBridge, relationOutboxConsumer)
		if searchOutboxConsumer != nil {
			backgroundRunners = append(backgroundRunners, searchOutboxConsumer)
		}
	} else {
		logger.Warn("canal is disabled: outbox async sync pipeline will not start")
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

// newFreeCacheWithConfig 根据配置创建统一的 freecache 实例。
//
// 使用单一缓存池替代之前的 3 个独立实例（detailCache、feedPublicCache、feedMineCache），
// 通过 key 前缀区分不同用途：
//   - "d:" 前缀：详情缓存（detailCache）
//   - "fp:" 前缀：公共 Feed 缓存（feedPublicCache）
//   - "fm:" 前缀：我的 Feed 缓存（feedMineCache）
//
// 总内存大小 = PublicCfg.MaxSize + MineCfg.MaxSize（单位 MB），
// 保证与之前 3 个独立实例的总容量一致。
//
// WHY 统一缓存池：
//   - 减少内存碎片：3 个独立实例各自分配内存池，合并后内存利用率更高。
//   - 弹性分配：热门缓存类型可以自然占用更多空间，不会因固定分区而浪费。
//   - 简化依赖注入：只需传递一个 cache 实例。
func newFreeCacheWithConfig(cfg *config.Config) *freecache.Cache {
	totalMB := cfg.Cache.L2.PublicCfg.MaxSize + cfg.Cache.L2.MineCfg.MaxSize
	if totalMB <= 0 {
		if cfg.Cache.L2.PublicCfg.FreeCacheDefaultMB > 0 {
			totalMB = cfg.Cache.L2.PublicCfg.FreeCacheDefaultMB
		} else {
			totalMB = 32 // 默认 32MB
		}
	}
	return freecache.NewCache(totalMB * 1024 * 1024)
}
