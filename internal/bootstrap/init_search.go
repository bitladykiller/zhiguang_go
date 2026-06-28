package bootstrap

import (
	"context"
	"strings"

	"github.com/jmoiron/sqlx"
	"github.com/redis/go-redis/v9"
	"go.uber.org/zap"

	"github.com/zhiguang/app/internal/counter"
	"github.com/zhiguang/app/internal/messaging"
	"github.com/zhiguang/app/internal/outbox"
	"github.com/zhiguang/app/internal/relation"
	"github.com/zhiguang/app/internal/search"
	"github.com/zhiguang/app/pkg/config"
)

// initSearch 创建搜索模块的完整服务栈（可选，配置不完整时降级）。
//
// 创建顺序：
//  1. 检查 ES 配置完整性（hasElasticsearchConfig）
//  2. 如果配置完整，创建 SearchService（构造时注入 counter）
//  3. 创建 KnowPostProjector（outbox 事件 → ES 索引）
//  4. 创建 outbox.Consumer（通用消费框架，注入 searchRowHandler）
//  5. SearchHandler（HTTP 请求适配，svc 可能为 nil → 503）
//
// 返回：
//   - *search.SearchHandler: HTTP handler（svc 非 nil 表示搜索可用）
//   - *outbox.Consumer: 搜索索引消费者（需 Start）
//   - *outbox.Consumer: 关系事件消费者（需 Start）
func initSearch(
	db *sqlx.DB,
	redisClient *redis.Client,
	counterSvc *counter.CounterService,
	cfg *config.Config,
	logger *zap.Logger,
) (*search.SearchHandler, *outbox.Consumer, *outbox.Consumer) {
	var searchSvc *search.SearchService
	var searchOutboxConsumer *outbox.Consumer

	if hasElasticsearchConfig(cfg) {
		var err error
		searchSvc, err = search.NewSearchService(context.Background(), search.ESConfig{
		URIs:      cfg.Elasticsearch.URIs,
		IndexName: cfg.Elasticsearch.IndexName,
		MaxRetries: cfg.Elasticsearch.MaxRetries,
	}, counterSvc, logger)
		if err != nil {
			logger.Warn("Failed to initialize search service (ES may be unavailable)", zap.Error(err))
			searchSvc = nil
		}
	} else {
		logger.Warn("Search service disabled: elasticsearch config is incomplete")
	}

	searchHandler := search.NewSearchHandler(searchSvc)

	if searchSvc != nil {
		searchProjector := search.NewKnowPostProjector(db, searchSvc, counterSvc, logger)
		searchRowHandler := &search.SearchRowHandler{Projector: searchProjector}
		searchOutboxConsumer = outbox.NewConsumer(
			messaging.NewKafkaReaderWithGroup(&cfg.Kafka, outbox.CanalOutboxTopic, outbox.SearchOutboxConsumerGroup),
			searchRowHandler,
			logger,
		)
	}

	relationEventProcessor := relation.NewEventProcessor(redisClient, counter.NewUserCounter(counterSvc), logger)
	relationRowHandler := &relation.RelationRowHandler{Processor: relationEventProcessor}
	relationOutboxConsumer := outbox.NewConsumer(
		messaging.NewKafkaReaderWithGroup(&cfg.Kafka, outbox.CanalOutboxTopic, outbox.RelationOutboxConsumerGroup),
		relationRowHandler,
		logger,
	)

	return searchHandler, searchOutboxConsumer, relationOutboxConsumer
}

// hasElasticsearchConfig 检查 Elasticsearch 是否已正确配置。
//
// 检查优先级：
//  1. 显式 disabled（Enabled == false）→ 不启用
//  2. 配置缺失 → 不启用
//  3. 配置完整或 Enabled 为 nil → 启用
func hasElasticsearchConfig(cfg *config.Config) bool {
	if cfg.Elasticsearch.Enabled != nil && !*cfg.Elasticsearch.Enabled {
		return false
	}
	return len(cfg.Elasticsearch.URIs) > 0 && strings.TrimSpace(cfg.Elasticsearch.URIs[0]) != "" &&
		strings.TrimSpace(cfg.Elasticsearch.IndexName) != ""
}
