// Package canal 负责将阿里云 Canal 捕获的 MySQL binlog 变更桥接到 Kafka。
//
// 核心流程：
//
//	Canal client -> 订阅 outbox 表 binlog -> 解析 row events -> 打包 JSON -> 写入 Kafka
package canal

import (
	"context"
	"errors"

	"github.com/segmentio/kafka-go"
	"go.uber.org/zap"

	"github.com/zhiguang/app/pkg/config"
)

const (
	defaultCanalSocketTimeoutMs = 60_000
	defaultCanalIdleTimeoutMs   = 60 * 60 * 1000
)

// Bridge 负责把 Canal 中的 outbox 表变更桥接到 Kafka。
//
// 当前按职责拆分为：
//   - bridge.go: 结构体、构造函数、启动入口
//   - bridge_session.go: 单次 Canal 会话和轮询循环
//   - bridge_helpers.go: 时间/重试辅助与 Kafka 消息构造
//
// 这样做的目的是把“runner 生命周期”“一次连接会话”“工具函数”拆开，
// 避免异步桥接链路长期堆在单文件中。
type Bridge struct {
	cfg    *config.CanalConfig
	writer *kafka.Writer
	logger *zap.Logger
}

// NewBridge 创建 Canal 桥接器实例。
//
// Canal 是可选组件，因此配置未启用或 writer 缺失时直接返回 nil。
func NewBridge(cfg *config.CanalConfig, writer *kafka.Writer, logger *zap.Logger) *Bridge {
	if cfg == nil || !cfg.Enabled || writer == nil {
		return nil
	}
	return &Bridge{cfg: cfg, writer: writer, logger: logger}
}

// Start 持续连接 Canal 并将 outbox 表变更写入 Kafka。
//
// 这里保持固定间隔重试，而不是指数退避。
// 原因是 Canal 连接中断大多是短暂网络抖动或服务重启，简单固定重试已经够用。
func (b *Bridge) Start(ctx context.Context) {
	if b == nil {
		return
	}
	defer b.writer.Close()

	retryDelay := b.retryDelay()
	for {
		if ctx.Err() != nil {
			return
		}
		err := b.runOnce(ctx)
		if err != nil && !errors.Is(err, context.Canceled) && b.logger != nil {
			b.logger.Warn("canal bridge stopped unexpectedly, will retry", zap.Error(err))
		}
		if !sleepContext(ctx, retryDelay) {
			return
		}
	}
}
