package bootstrap

import (
	"strings"

	"go.uber.org/zap"

	"github.com/zhiguang/app/internal/llm"
	"github.com/zhiguang/app/internal/storage"
	"github.com/zhiguang/app/pkg/config"
)

// initLLM 创建 LLM 模块的服务栈（可选，配置不完整时降级）。
//
// 包含两个独立服务：
//   - KnowPostDescriptionService: DeepSeek API 摘要生成
//   - RagQueryService: RAG 流式问答（需 ES + OpenAI embedding + DeepSeek chat）
//
// 返回：
//   - *llm.LlmHandler: HTTP handler（内部 svc 可能为 nil → 503）
//
// 降级策略：
//
//	任一服务配置不完整 → 对应 svc = nil → handler 返回 503
func initLLM(cfg *config.Config, logger *zap.Logger) *llm.LlmHandler {
	descSvc := buildDescriptionService(cfg, logger)
	ragQuerySvc := buildRagQueryService(cfg, logger)
	return llm.NewLlmHandler(descSvc, ragQuerySvc)
}

// initStorage 创建 OSS 存储模块的服务栈（可选，配置不完整时降级）。
//
// 返回：
//   - *storage.StorageHandler: HTTP handler（svc 可能为 nil → 503）
//
// 降级策略：
//
//	OSS 配置不完整或客户端创建失败 → ossSvc = nil → Presign 返回 503
func initStorage(cfg *config.Config, logger *zap.Logger) *storage.StorageHandler {
	ossSvc := buildOssService(cfg, logger)
	return storage.NewStorageHandler(ossSvc)
}

// buildDescriptionService 根据配置创建 AI 摘要生成服务。
//
// 降级优先级：
//  1. 显式 disabled（Enabled == false）→ 不启用
//  2. 配置缺失 → 不启用 + warn
//  3. 配置完整 → 启用
func buildDescriptionService(cfg *config.Config, logger *zap.Logger) *llm.KnowPostDescriptionService {
	if !isOptionalEnabled(cfg.LLM.Enabled) {
		logger.Warn("LLM description service disabled: explicitly disabled")
		return nil
	}
	if strings.TrimSpace(cfg.LLM.DeepSeek.APIKey) == "" ||
		strings.TrimSpace(cfg.LLM.DeepSeek.BaseURL) == "" ||
		strings.TrimSpace(cfg.LLM.DeepSeek.Model) == "" {
		logger.Warn("LLM description service disabled: DeepSeek config is incomplete")
		return nil
	}
	return llm.NewKnowPostDescriptionService(&cfg.LLM)
}

// buildRagQueryService 根据配置创建 RAG 问答服务。
func buildRagQueryService(cfg *config.Config, logger *zap.Logger) *llm.RagQueryService {
	if !isOptionalEnabled(cfg.LLM.Enabled) {
		logger.Warn("RAG query service disabled: explicitly disabled")
		return nil
	}
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
	if len(cfg.Elasticsearch.URIs) == 0 {
		logger.Warn("RAG query service disabled: elasticsearch URIs is empty")
		return nil
	}
	return llm.NewRagQueryService(&cfg.LLM, cfg.Elasticsearch.URIs[0])
}

// buildOssService 根据配置创建 OSS 存储服务。
func buildOssService(cfg *config.Config, logger *zap.Logger) storage.StorageServicer {
	if !isOptionalEnabled(cfg.OSS.Enabled) {
		logger.Warn("Storage service disabled: explicitly disabled")
		return nil
	}
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

// isOptionalEnabled 判断可选功能是否启用。
//
// 返回值：
//   - enabled == nil：未显式设置，由调用方检查配置完整性决定
//   - enabled == true：显式启用
//   - enabled == false：显式禁用
func isOptionalEnabled(enabled *bool) bool {
	return enabled == nil || *enabled
}
