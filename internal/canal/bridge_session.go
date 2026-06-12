package canal

import (
	"context"

	"github.com/withlin/canal-go/client"
	"go.uber.org/zap"
)

// runOnce 建立一次 Canal 会话并持续轮询 binlog 变更，直到出现错误或 ctx 取消。
//
// 生命周期：
//   - 建立连接
//   - 订阅 filter
//   - 轮询 batch
//   - 解析 payload
//   - 写入 Kafka
//   - 成功 ACK / 失败回滚
func (b *Bridge) runOnce(ctx context.Context) error {
	connector := client.NewSimpleCanalConnector(
		b.cfg.Host,
		b.cfg.Port,
		b.cfg.Username,
		b.cfg.Password,
		b.cfg.Destination,
		int32(defaultCanalSocketTimeoutMs),
		int32(defaultCanalIdleTimeoutMs),
	)
	if err := connector.Connect(); err != nil {
		return err
	}
	defer func() {
		if err := connector.DisConnection(); err != nil && b.logger != nil {
			b.logger.Warn("disconnect canal connector failed", zap.Error(err))
		}
	}()

	if err := connector.Subscribe(b.cfg.Filter); err != nil {
		return err
	}

	pollDelay := b.pollDelay()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		message, err := connector.GetWithOutAck(int32(maxInt(b.cfg.BatchSize, 1)), nil, nil)
		if err != nil {
			return err
		}

		if message.Id == -1 || len(message.Entries) == 0 {
			_ = sleepContext(ctx, pollDelay)
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

		messages, err := buildMessages(payloads)
		if err != nil {
			_ = connector.RollBack(batchID)
			return err
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
