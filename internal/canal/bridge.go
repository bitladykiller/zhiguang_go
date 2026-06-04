// Package canal 负责将阿里云 Canal 捕获的 MySQL binlog 变更桥接到 Kafka。
//
// 核心流程：
//
//	Canal client -> 订阅 outbox 表 binlog -> 解析 row events -> 打包 JSON -> 写入 Kafka
//
// 当 canal.enabled = true 时，该桥接器会在 background runner 中启动，
// 与 Java 版保持一致的「Canal -> Kafka -> relation/search consumers」链路。
// 当 canal.enabled = false 时，不会启动该桥接器。
package canal

import (
	"context"
	"errors"
	"time"

	"github.com/segmentio/kafka-go"
	"github.com/withlin/canal-go/client"
	"go.uber.org/zap"

	"github.com/zhiguang/app/pkg/config"
)

const (
	defaultCanalSocketTimeoutMs = 60_000
	defaultCanalIdleTimeoutMs   = 60 * 60 * 1000
)

// Bridge 负责把 Canal 中的 outbox 表变更桥接到 Kafka。
//
// 启动后会持续连接到 Canal Server，轮询订阅的 binlog 变更（filter 配置过滤规则）。
// 每次从 Canal 获取一批变更后，会按以下流程处理：
//  1. 调用 parseEntries 将 protobuf Entry 转为 JSON 格式的 CanalEnvelope
//  2. 将消息批量写入 Kafka 的 canal-outbox 主题
//  3. ACK 该 batchID，告知 Canal Server 已完成消费
//  4. 如果写入 Kafka 失败则 ROLLBACK batchID，后续重试会重新获取该批次
//
// 中断重连策略：当连接意外断开时，会以 IntervalMs 间隔无限重试，直到 ctx 被取消。
type Bridge struct {
	cfg    *config.CanalConfig
	writer *kafka.Writer
	logger *zap.Logger
}

func NewBridge(cfg *config.CanalConfig, writer *kafka.Writer, logger *zap.Logger) *Bridge {
	if cfg == nil || !cfg.Enabled || writer == nil {
		return nil
	}
	return &Bridge{cfg: cfg, writer: writer, logger: logger}
}

// Start 持续连接 Canal 并将 outbox 变更写入 Kafka。
func (b *Bridge) Start(ctx context.Context) {
	if b == nil {
		return
	}
	defer b.writer.Close()

	retryDelay := time.Duration(maxInt(b.cfg.IntervalMs, 1000)) * time.Millisecond
	for {
		if ctx.Err() != nil {
			return
		}
		if err := b.runOnce(ctx); err != nil && !errors.Is(err, context.Canceled) && b.logger != nil {
			b.logger.Warn("canal bridge stopped unexpectedly, will retry", zap.Error(err))
		}
		if !sleepContext(ctx, retryDelay) {
			return
		}
	}
}

func (b *Bridge) runOnce(ctx context.Context) error {
	connector := client.NewSimpleCanalConnector(
		b.cfg.Host,
		b.cfg.Port,
		b.cfg.Username,
		b.cfg.Password,
		b.cfg.Destination,
		defaultCanalSocketTimeoutMs,
		defaultCanalIdleTimeoutMs,
	)

	if err := connector.Connect(); err != nil {
		return err
	}
	defer connector.DisConnection()

	if err := connector.Subscribe(b.cfg.Filter); err != nil {
		return err
	}
	if err := connector.RollBack(0); err != nil {
		return err
	}

	if b.logger != nil {
		b.logger.Info("canal bridge connected",
			zap.String("host", b.cfg.Host),
			zap.Int("port", b.cfg.Port),
			zap.String("destination", b.cfg.Destination),
			zap.String("filter", b.cfg.Filter),
			zap.Int("batch_size", b.cfg.BatchSize),
		)
	}

	pollDelay := time.Duration(maxInt(b.cfg.IntervalMs, 100)) * time.Millisecond
	for {
		if ctx.Err() != nil {
			return ctx.Err()
		}

		message, err := connector.GetWithOutAck(int32(maxInt(b.cfg.BatchSize, 1)), nil, nil)
		if err != nil {
			return err
		}
		if message == nil || message.Id == -1 || len(message.Entries) == 0 {
			if !sleepContext(ctx, pollDelay) {
				return ctx.Err()
			}
			continue
		}

		batchID := message.Id
		payloads, err := parseEntries(message.Entries)
		if err != nil {
			_ = connector.RollBack(batchID)
			return err
		}
		if len(payloads) == 0 {
			if err := connector.Ack(batchID); err != nil {
				return err
			}
			continue
		}

		messages := make([]kafka.Message, 0, len(payloads))
		for _, payload := range payloads {
			messages = append(messages, kafka.Message{Value: payload})
		}
		if err := b.writer.WriteMessages(ctx, messages...); err != nil {
			_ = connector.RollBack(batchID)
			return err
		}
		if err := connector.Ack(batchID); err != nil {
			return err
		}
	}
}

func maxInt(value, fallback int) int {
	if value > 0 {
		return value
	}
	return fallback
}

func sleepContext(ctx context.Context, d time.Duration) bool {
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}
