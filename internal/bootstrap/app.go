// Package bootstrap 负责把基础设施、领域模块和后台 runner 组装成一个可运行的应用。
//
// 它只解决“依赖如何连接”的问题，不承载业务规则，目的是把：
//   - 资源创建
//   - 模块装配
//   - 生命周期注册
//
// 统一收敛到一层，避免依赖关系分散在各个领域包内部。
package bootstrap

import (
	"go.uber.org/zap"

	"github.com/zhiguang/app/internal/canal"
	"github.com/zhiguang/app/internal/server"
	"github.com/zhiguang/app/pkg/config"
)

// InitializeApp 完成整个应用的顶层装配。
//
// app.go 只保留编排职责：
//   - 初始化 logger / config
//   - 调用 infra / wiring 构建各模块
//   - 组装 router、background runners 和 cleanup
//
// 这样做可以让 InitializeApp 保持“可读的依赖拓扑”，而不是继续膨胀成一个
// 混合了资源创建、领域装配、降级判断和生命周期控制的大函数。
func InitializeApp(configPath string) (*server.App, error) {
	logger, err := zap.NewProduction()
	if err != nil {
		return nil, err
	}

	cfg, err := config.LoadConfig(configPath)
	if err != nil {
		return nil, err
	}

	infra, cleanup, err := buildInfra(cfg, logger)
	if err != nil {
		return nil, err
	}

	authModule, err := BuildAuthModule(infra)
	if err != nil {
		return nil, err
	}

	counterModule := BuildCounterModule(infra)
	knowPostHandler := BuildKnowPostHandler(infra, counterModule.Deps)
	relationModule := BuildRelationModule(infra, counterModule.Deps)
	searchModule := BuildSearchModule(infra, counterModule.Deps)
	llmHandler := BuildLLMHandler(infra)
	storageHandler := BuildStorageHandler(infra)
	profileHandler := BuildProfileHandler(infra)

	handlerSet := &server.HandlerSet{
		Auth:     authModule.Handler,
		KnowPost: knowPostHandler,
		Counter:  counterModule.Handler,
		Relation: relationModule.Handler,
		Search:   searchModule.Handler,
		LLM:      llmHandler,
		Storage:  storageHandler,
		Profile:  profileHandler,
	}

	router := server.NewRouter(handlerSet, logger, authModule.TokenValidator)

	backgroundRunners := make([]server.BackgroundRunner, 0, 5)
	backgroundRunners = append(backgroundRunners, counterModule.Runners...)
	if cfg.Canal.Enabled {
		backgroundRunners = append(
			backgroundRunners,
			canal.NewBridge(&cfg.Canal, infra.CanalOutboxWriter, logger),
		)
		if relationModule.OutboxRunner != nil {
			backgroundRunners = append(backgroundRunners, relationModule.OutboxRunner)
		}
		if searchModule.OutboxRunner != nil {
			backgroundRunners = append(backgroundRunners, searchModule.OutboxRunner)
		}
	} else {
		logger.Warn("canal is disabled: outbox async sync pipeline will not start")
	}

	app := server.NewApp(router, cfg, logger, backgroundRunners...)
	app.AddCleanup(cleanup...)
	return app, nil
}
