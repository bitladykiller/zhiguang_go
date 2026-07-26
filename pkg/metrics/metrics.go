// Package metrics 定义 Prometheus 指标（HTTP 请求量/时延、DB 查询时延），由中间件与数据层埋点。
package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	HTTPRequestTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "http_requests_total",
			Help: "Total number of HTTP requests",
		},
		[]string{"method", "path", "status"},
	)

	HTTPRequestDuration = promauto.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "http_request_duration_seconds",
			Help:    "HTTP request latency in seconds",
			Buckets: []float64{0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5},
		},
		[]string{"method", "path"},
	)

	CacheHitTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "cache_hits_total",
			Help: "Total number of cache hits",
		},
		[]string{"cache_layer"},
	)

	CacheMissTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "cache_misses_total",
			Help: "Total number of cache misses",
		},
		[]string{"cache_layer"},
	)

	DBQueryDuration = promauto.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "db_query_duration_seconds",
			Help:    "Database query latency in seconds",
			Buckets: []float64{0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1},
		},
		[]string{"operation"},
	)

	KafkaMessageTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "kafka_messages_total",
			Help: "Total number of Kafka messages",
		},
		[]string{"topic", "status"},
	)

	// FanoutPostsTotal 按处理模式统计扩散过的帖子：
	// push=推送完成 / pull=大 V 跳过推送 / promoted=边推边数中越过阈值 / guard=触发兜底上限
	FanoutPostsTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "fanout_posts_total",
			Help: "Posts processed by the fan-out pipeline, by mode",
		},
		[]string{"mode"},
	)

	// FanoutPushedFollowersTotal 是写扩散累计触达的收件箱数（写放大的直接观测值）。
	FanoutPushedFollowersTotal = promauto.NewCounter(
		prometheus.CounterOpts{
			Name: "fanout_pushed_followers_total",
			Help: "Total follower inboxes written by fan-out pushes",
		},
	)

	// HomeTimelinePulledAuthorsTotal 是读路径累计拉取的大 V 发件箱数（读成本的直接观测值）。
	HomeTimelinePulledAuthorsTotal = promauto.NewCounter(
		prometheus.CounterOpts{
			Name: "home_timeline_pulled_authors_total",
			Help: "Total celebrity author boxes pulled while serving home timelines",
		},
	)
)
