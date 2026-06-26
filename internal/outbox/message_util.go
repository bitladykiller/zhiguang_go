package outbox

import (
	"encoding/json"
	"fmt"
	"strings"
)

const (
	outboxTableName = "outbox"
	changeInsert    = "INSERT"
	changeUpdate    = "UPDATE"
)

// CanalEnvelope 表示 Canal 桥接后写入 Kafka 的标准消息结构。
//
// 这条消息是由 canal.Bridge 在 binlog 中捕获到 outbox 表的变更后，
// 按 Canal 协议的列映射转换而成的 JSON 格式。消费端（search.OutboxConsumer、
// relation.OutboxConsumer）解析该结构以获知具体的数据变更内容。
//
// 字段说明：
//   - Table：发生变更的数据库表名（此处固定为 "outbox"）
//   - Type：变更类型，当前为 "INSERT" 或 "UPDATE"
//   - Data：变更行的数组，每一行对应 outbox 表中的一条事件
type CanalEnvelope struct {
	Table string     `json:"table"`
	Type  string     `json:"type"`
	Data  []CanalRow `json:"data"`
}

// CanalRow 表示 outbox 表中的一行变更事件。
//
// 字段说明：
//   - ID：outbox 行的自增主键（当前版本仅用于排障和去重）
//   - AggregateType：聚合类型，如 "knowpost"、"following"
//   - AggregateID：聚合根 ID
//   - Type：事件类型，如 "KnowPostPublished"、"FollowCreated"
//   - Payload：业务事件的 JSON 序列化载荷，是消费端真正处理的内容
//
// 当前首版只强依赖 Payload 字段；其余字段是为后续增加幂等性校验
// 和回溯重放功能预留的扩展点。
type CanalRow struct {
	ID            string `json:"id,omitempty"`
	AggregateType string `json:"aggregate_type,omitempty"`
	AggregateID   string `json:"aggregate_id,omitempty"`
	Type          string `json:"type,omitempty"`
	Payload       string `json:"payload,omitempty"`
}

// ExtractRows 从 Canal 包装消息中提取 outbox 行数组。
//
// 调用方传入的是从 Kafka 消费到的原始消息 value（CanalEnvelope 的 JSON 表示）。
// 本函数会进行以下检查：
//   - 表名必须是 outbox（避免处理其他表的 binlog 变更）
//   - 变更类型必须是 INSERT 或 UPDATE（DELETE 变更不处理）
//   - data 数组不为空
//
// 如果任意检查不通过，则返回空切片，调用方可以直接跳过本条消息。
func ExtractRows(message []byte) ([]CanalRow, error) {
	var envelope CanalEnvelope
	if err := json.Unmarshal(message, &envelope); err != nil {
		return nil, fmt.Errorf("extract rows: unmarshal: %w", err)
	}
	if envelope.Table != outboxTableName {
		return []CanalRow{}, nil
	}
	if envelope.Type != changeInsert && envelope.Type != changeUpdate {
		return []CanalRow{}, nil
	}
	if envelope.Data == nil {
		return []CanalRow{}, nil
	}
	return envelope.Data, nil
}

// MessageKeyFunc 是生成 Kafka 分区键的函数签名。
//
// 业务模块通过注册自己的 MessageKeyFunc 来提供聚合根的分区键生成逻辑，
// 避免 outbox 包硬编码业务类型。
//
// 参数：
//   - row: CanalRow，包含 AggregateType、AggregateID、Payload 等字段
//
// 返回值：
//   - string: 分区键，用于路由到同一 Kafka 分区
//   - bool:   true=该函数能处理此聚合类型；false=不能处理，应尝试下一个函数
type MessageKeyFunc func(row CanalRow) (string, bool)

var messageKeyFuncs []MessageKeyFunc

// RegisterMessageKeyFunc 注册一个业务模块的分区键生成函数。
//
// 调用时机：在应用初始化时（如 bootstrap 中）注册。
// 线程安全：应在单线程初始化阶段调用，无需并发保护。
//
// 示例：
//
//	outbox.RegisterMessageKeyFunc(knowpost.MessageKey)
//	outbox.RegisterMessageKeyFunc(relation.MessageKey)
func RegisterMessageKeyFunc(fn MessageKeyFunc) {
	messageKeyFuncs = append(messageKeyFuncs, fn)
}

// MessageKey 为一条 outbox 行生成稳定的 Kafka 分区键。
//
// 调用顺序：
//  1. 先遍历所有已注册的 MessageKeyFunc，按注册顺序调用
//  2. 如果某个函数返回 ok=true，使用其返回的 key
//  3. 如果所有函数都返回 ok=false，使用默认兜底策略
//
// 默认兜底策略（按优先级）：
//   - aggregate_type:aggregate_id
//   - aggregate_type:event_type
//   - outbox:row_id
//   - event_type
func MessageKey(row CanalRow) string {
	for _, fn := range messageKeyFuncs {
		if key, ok := fn(row); ok {
			return key
		}
	}

	// 默认兜底策略
	aggType := strings.TrimSpace(row.AggregateType)
	aggID := strings.TrimSpace(row.AggregateID)
	if aggType != "" && aggID != "" {
		return aggType + ":" + aggID
	}
	if aggType != "" && row.Type != "" {
		return aggType + ":" + row.Type
	}
	if row.ID != "" {
		return "outbox:" + row.ID
	}
	return row.Type
}
