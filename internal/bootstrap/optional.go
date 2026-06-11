// optional.go 统一承载“可选能力”的创建和降级逻辑。
//
// 原则是：
//   - 配置不完整时返回 nil，而不是阻止整个应用启动
//   - handler 层拿到 nil use case 后统一返回 503
//   - 降级判断集中在 bootstrap，避免业务层散落大量配置分支
package bootstrap

import (
	"strings"

	"go.uber.org/zap"

	"github.com/zhiguang/app/internal/llm"
	"github.com/zhiguang/app/internal/search"
	"github.com/zhiguang/app/internal/server"
	"github.com/zhiguang/app/internal/storage"
	"github.com/zhiguang/app/pkg/config"
)

// buildSearchService 按配置尝试创建 Elasticsearch 搜索服务。
func buildSearchService(cfg *config.Config, logger *zap.Logger, counter search.SearchCounterClient) *search.SearchService {
	if !hasElasticsearchConfig(cfg) {
		logger.Warn("search service disabled: elasticsearch config is incomplete")
		return nil
	}

	searchSvc, err := search.NewSearchService(search.ServiceConfig{
		URIs:      cfg.Elasticsearch.URIs,
		IndexName: cfg.Elasticsearch.IndexName,
		Counter:   counter,
	})
	if err != nil {
		logger.Warn("failed to initialize search service (ES may be unavailable)", zap.Error(err))
		return nil
	}
	return searchSvc
}

// BuildLLMHandler 构建 LLM 处理器，并统一处理可选能力降级。
func BuildLLMHandler(infra *InfraDeps) server.RouteRegistrar {
	descSvc := buildDescriptionService(infra.Config, infra.Logger)
	ragSvc := buildRagQueryService(infra.Config, infra.Logger)

	var desc llm.DescriptionSuggester
	if descSvc != nil {
		desc = descSvc
	}

	var rag llm.RagQueryUseCase
	if ragSvc != nil {
		rag = ragSvc
	}

	return llm.NewLLMHandler(desc, rag)
}

// BuildStorageHandler 构建 OSS 处理器，并统一处理可选能力降级。
func BuildStorageHandler(infra *InfraDeps) server.RouteRegistrar {
	ossSvc := buildOSSService(infra.Config, infra.Logger)

	var storageSvc storage.StorageUseCase
	if ossSvc != nil {
		storageSvc = ossSvc
	}

	return storage.NewStorageHandler(storageSvc)
}

// hasElasticsearchConfig 判断搜索和 RAG 所需的最小 ES 配置是否完整。
func hasElasticsearchConfig(cfg *config.Config) bool {
	return len(cfg.Elasticsearch.URIs) > 0 &&
		strings.TrimSpace(cfg.Elasticsearch.URIs[0]) != "" &&
		strings.TrimSpace(cfg.Elasticsearch.IndexName) != ""
}

// buildDescriptionService 创建基于 DeepSeek 的帖子摘要服务。
func buildDescriptionService(cfg *config.Config, logger *zap.Logger) *llm.KnowPostDescriptionService {
	if strings.TrimSpace(cfg.LLM.DeepSeek.APIKey) == "" ||
		strings.TrimSpace(cfg.LLM.DeepSeek.BaseURL) == "" ||
		strings.TrimSpace(cfg.LLM.DeepSeek.Model) == "" {
		logger.Warn("llm description service disabled: DeepSeek config is incomplete")
		return nil
	}
	return llm.NewKnowPostDescriptionService(&cfg.LLM)
}

// buildRagQueryService 创建 RAG 问答服务。
//
// 它同时依赖：
//   - Elasticsearch：负责检索上下文
//   - OpenAI 兼容 embedding 接口：负责向量化
//   - DeepSeek chat 接口：负责最终回答
func buildRagQueryService(cfg *config.Config, logger *zap.Logger) *llm.RagQueryService {
	if !hasElasticsearchConfig(cfg) {
		logger.Warn("rag query service disabled: elasticsearch config is incomplete")
		return nil
	}
	if strings.TrimSpace(cfg.LLM.OpenAI.APIKey) == "" || strings.TrimSpace(cfg.LLM.OpenAI.BaseURL) == "" {
		logger.Warn("rag query service disabled: embedding config is incomplete")
		return nil
	}
	if strings.TrimSpace(cfg.LLM.DeepSeek.APIKey) == "" ||
		strings.TrimSpace(cfg.LLM.DeepSeek.BaseURL) == "" ||
		strings.TrimSpace(cfg.LLM.DeepSeek.Model) == "" {
		logger.Warn("rag query service disabled: chat model config is incomplete")
		return nil
	}
	return llm.NewRagQueryService(&cfg.LLM, cfg.Elasticsearch.URIs[0])
}

// buildOSSService 创建对象存储服务。
func buildOSSService(cfg *config.Config, logger *zap.Logger) *storage.OSSStorageService {
	if strings.TrimSpace(cfg.OSS.Endpoint) == "" ||
		strings.TrimSpace(cfg.OSS.AccessKeyID) == "" ||
		strings.TrimSpace(cfg.OSS.AccessKeySecret) == "" ||
		strings.TrimSpace(cfg.OSS.Bucket) == "" {
		logger.Warn("storage service disabled: OSS config is incomplete")
		return nil
	}

	ossSvc, err := storage.NewOSSStorageService(&cfg.OSS)
	if err != nil {
		logger.Warn("failed to initialize OSS service", zap.Error(err))
		return nil
	}
	return ossSvc
}
