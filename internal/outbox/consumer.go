// Package outbox 提供 Outbox 消费端的通用框架。
//
// RowHandler 是消费端处理单条 outbox 行的回调接口。
// 不同的业务模块（搜索、关系等）只需实现此接口，无需重复编写
// FetchMessage → ExtractRows → 处理 → CommitMessages 的样板代码。
package outbox

import (
	"context"
	"time"

	"github.com/segmentio/kafka-go"
	"go.uber.org/zap"

	"github.com/zhiguang/app/pkg/contextutil"
)

// Row 是从 CanalEnvelope 中提取出的单条 outbox 行，已解析关键字段。
type Row struct {
	AggregateType string
	AggregateID   string
	Type          string
	Payload       []byte // 原始 Payload JSON 字节，避免二次解析
}

// RowHandler 定义消费端处理单条 outbox 行的回调接口。
//
// 实现者只需关心"如何处理一行 outbox 事件"，无需关心：
//   - Kafka 消息的拉取和提交
//   - CanalEnvelope 的解析和行提取
//   - 重试和错误处理
type RowHandler interface {
	// HandleRow 处理单条 outbox 行。
	// 返回 nil 表示处理成功，非 nil 表示处理失败（会触发重试）。
	HandleRow(ctx context.Context, row Row) error
}

// FailedMessageRecorder 抽象死信记录能力，用于持久化处理失败的消息供排障和补偿使用。
type FailedMessageRecorder interface {
	Create(ctx context.Context, topic string, messageKey string, payload []byte, cause error) error
}

// Consumer 是 outbox 消费端的通用框架。
//
// 封装了完整的消费循环：拉取消息 → 解析 CanalEnvelope → 提取行 →
// 调用 RowHandler → 提交 offset。所有错误处理、重试和日志记录
// 都在框架内统一完成。
//
// 提供了死信记录能力：当消息处理重试耗尽后，会将失败消息持久化到数据库，
// 然后跳过该消息继续消费后续消息，避免因单条格式错误消息阻塞整个消费流程。
//
// 使用方式：
//
//	consumer := outbox.NewConsumer(reader, handler, logger)
//	consumer.SetFailedMessageRecorder(recorder)
//	go consumer.Start(ctx)
type Consumer struct {
	reader          *kafka.Reader
	handler         RowHandler
	logger          *zap.Logger
	failureRecorder FailedMessageRecorder
	maxRetries      int
	retryDelay      time.Duration
}

// NewConsumer 创建 outbox 消费者实例。
//
// 参数：
//   - reader: Kafka Reader，已配置好 topic 和 consumer group
//   - handler: 行处理器，实现 RowHandler 接口
//   - logger: 日志记录器，可为 nil
//
// 返回值：*Consumer，reader 或 handler 为 nil 时返回 nil
func NewConsumer(reader *kafka.Reader, handler RowHandler, logger *zap.Logger) *Consumer {
	if reader == nil || handler == nil {
		return nil
	}
	return &Consumer{
		reader:     reader,
		handler:    handler,
		logger:     logger,
		maxRetries: 3,
		retryDelay: time.Second,
	}
}

// SetFailedMessageRecorder 注入死信记录能力。同一个 Consumer 可被多个消费链路复用。
func (c *Consumer) SetFailedMessageRecorder(recorder FailedMessageRecorder) {
	c.failureRecorder = recorder
}

// Start 启动消费循环，阻塞直到 ctx 取消。
//
// 消费流程：
//  1. FetchMessage 拉取消息
//  2. 解析 CanalEnvelope JSON
//  3. 提取 outbox 行（过滤非 outbox 表、非 INSERT/UPDATE 的变更）
//  4. 逐行调用 handler.HandleRow
//  5. 全部行处理成功后 CommitMessages
//  6. 如果处理失败，重试最多 3 次；耗尽后将消息写入死信记录并 CommitMessages 跳过
//  7. 任何步骤失败都等待 1 秒后重试
func (c *Consumer) Start(ctx context.Context) {
	if c == nil {
		return
	}
	defer c.reader.Close()

	for {
		msg, err := c.reader.FetchMessage(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			c.logWarn("fetch outbox kafka message failed", err)
			if !contextutil.Sleep(ctx, time.Second) {
				return
			}
			continue
		}

		if err := c.handleMessageWithRetry(ctx, msg); err != nil {
			c.logWarn("process outbox kafka message exhausted retries, will skip", err)
			c.recordFailedMessage(ctx, msg.Value, err)
			if err := c.reader.CommitMessages(ctx, msg); err != nil {
				c.logWarn("commit skipped outbox kafka message failed", err)
			}
			continue
		}

		if err := c.reader.CommitMessages(ctx, msg); err != nil {
			c.logWarn("commit outbox kafka message failed", err)
		}
	}
}

// handleMessageWithRetry 对一条消息的重试包装，最多重试 c.maxRetries 次。
func (c *Consumer) handleMessageWithRetry(ctx context.Context, msg kafka.Message) error {
	for attempt := 1; attempt <= c.maxRetries; attempt++ {
		if err := c.handleMessage(ctx, msg.Value); err != nil {
			if attempt == c.maxRetries {
				return err
			}
			c.logWarn("process outbox kafka message failed, retrying", err)
			if !contextutil.Sleep(ctx, c.retryDelay) {
				return err
			}
			continue
		}
		return nil
	}
	return nil
}

// recordFailedMessage 将失败消息写入死信记录。
func (c *Consumer) recordFailedMessage(ctx context.Context, value []byte, cause error) {
	if c.failureRecorder == nil {
		return
	}
	_ = c.failureRecorder.Create(context.WithoutCancel(ctx), CanalOutboxTopic, "", value, cause)
}

// handleMessage 解析一条 Kafka 消息，提取 outbox 行并逐行处理。
func (c *Consumer) handleMessage(ctx context.Context, value []byte) error {
	rows, err := extractRows(value)
	if err != nil {
		return err
	}

	for _, row := range rows {
		if len(row.Payload) == 0 {
			continue
		}
		if err := c.handler.HandleRow(ctx, row); err != nil {
			return err
		}
	}

	return nil
}

// extractRows 从 CanalEnvelope JSON 中提取 outbox 行。
//
// 过滤规则：
//   - 表名必须是 "outbox"
//   - 变更类型必须是 INSERT 或 UPDATE
//   - Payload 为空的行会被保留（由调用方决定是否跳过）
func extractRows(value []byte) ([]Row, error) {
	canalRows, err := ExtractRows(value)
	if err != nil {
		return nil, err
	}
	if len(canalRows) == 0 {
		return nil, nil
	}

	rows := make([]Row, 0, len(canalRows))
	for _, data := range canalRows {
		rows = append(rows, Row{
			AggregateType: data.AggregateType,
			AggregateID:   data.AggregateID,
			Type:          data.Type,
			Payload:       []byte(data.Payload),
		})
	}
	return rows, nil
}

func (c *Consumer) logWarn(msg string, err error) {
	if c.logger != nil {
		c.logger.Warn(msg, zap.Error(err))
	}
}
