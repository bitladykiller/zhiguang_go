package canal

import (
	"context"
	"time"

	"github.com/segmentio/kafka-go"

	"github.com/zhiguang/app/internal/outbox"
)

func (b *Bridge) retryDelay() time.Duration {
	return time.Duration(maxInt(b.cfg.IntervalMs, 1000)) * time.Millisecond
}

func (b *Bridge) pollDelay() time.Duration {
	return time.Duration(maxInt(b.cfg.IntervalMs, 100)) * time.Millisecond
}

// buildMessages 把 CanalEnvelope JSON payload 批量转换为 Kafka 消息。
//
// 分区 key 仍然复用 outbox.MessageKey 的聚合维度规则，
// 这样同一聚合根的事件可以进入同一分区并保持顺序。
func buildMessages(payloads [][]byte) ([]kafka.Message, error) {
	messages := make([]kafka.Message, 0, len(payloads))
	for _, payload := range payloads {
		var key []byte
		rows, err := outbox.ExtractRows(payload)
		if err != nil {
			return nil, err
		}
		if len(rows) > 0 {
			key = []byte(outbox.MessageKey(rows[0]))
		}
		messages = append(messages, kafka.Message{Key: key, Value: payload})
	}
	return messages, nil
}

// maxInt 返回 value 和 fallback 中更有效的正数。
func maxInt(value, fallback int) int {
	if value > 0 {
		return value
	}
	return fallback
}

// sleepContext 是一个可被上下文取消的休眠函数。
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
