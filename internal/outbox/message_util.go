package outbox

import "encoding/json"

const (
	outboxTableName = "outbox"
	changeInsert    = "INSERT"
	changeUpdate    = "UPDATE"
)

// CanalEnvelope 表示 Canal 桥接后写入 Kafka 的标准消息结构。
type CanalEnvelope struct {
	Table string     `json:"table"`
	Type  string     `json:"type"`
	Data  []CanalRow `json:"data"`
}

// CanalRow 表示 outbox 表中的一行变更。
// 当前首版只强依赖 payload；其余字段是为后续排障与扩展保留。
type CanalRow struct {
	ID            string `json:"id,omitempty"`
	AggregateType string `json:"aggregate_type,omitempty"`
	AggregateID   string `json:"aggregate_id,omitempty"`
	Type          string `json:"type,omitempty"`
	Payload       string `json:"payload,omitempty"`
}

// ExtractRows 从 Canal 包装消息中提取 outbox 行数组。
// 如果消息不是 outbox 的 INSERT/UPDATE 变更，则返回空切片。
func ExtractRows(message []byte) ([]CanalRow, error) {
	var envelope CanalEnvelope
	if err := json.Unmarshal(message, &envelope); err != nil {
		return nil, err
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
