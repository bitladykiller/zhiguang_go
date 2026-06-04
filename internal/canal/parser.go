// Package canal - parseEntries 将 Canal 的 protobuf Entry 解析为业务可读的 JSON 消息。
package canal

import (
	"encoding/json"
	"strings"

	pbe "github.com/withlin/canal-go/protocol/entry"
	"google.golang.org/protobuf/proto"

	"github.com/zhiguang/app/internal/outbox"
)

// parseEntries 把 Canal row events 转成写入 Kafka 的 JSON 消息。
//
// 输入：Canal 协议中的 Entry 数组（protobuf 格式），每个 Entry 代表一条 binlog 变更。
// 输出：JSON 序列化的 CanalEnvelope 字节数组，每条对应一个表变更事件。
//
// 解析流程：
//  1. 遍历 Entry 数组，过滤出 ROWDATA 类型的条目。
//  2. 用 protobuf 反序列化获取 RowChange 对象。
//  3. 过滤出 INSERT 和 UPDATE 类型的变更（DELETE 不处理）。
//  4. 遍历 RowData 的 AfterColumns，将列名映射为 CanalRow 的字段。
//  5. 将 CanalEnvelope 序列化为 JSON。z
//
// WHY：需要区分事件类型是因为 outbox 消费端只关心「数据有变化」的情况，
// 而 DELETE 的 outbox 行已经在原事务结束时被标记，不必再消费一次。
func parseEntries(entries []pbe.Entry) ([][]byte, error) {
	payloads := make([][]byte, 0, len(entries))
	for i := range entries {
		entry := &entries[i]
		if entry.GetEntryType() != pbe.EntryType_ROWDATA {
			continue
		}

		rowChange := new(pbe.RowChange)
		if err := proto.Unmarshal(entry.GetStoreValue(), rowChange); err != nil {
			return nil, err
		}

		eventType := rowChange.GetEventType()
		if eventType != pbe.EventType_INSERT && eventType != pbe.EventType_UPDATE {
			continue
		}

		envelope := outbox.CanalEnvelope{
			Table: entry.GetHeader().GetTableName(),
			Type:  eventTypeName(eventType),
			Data:  make([]outbox.CanalRow, 0, len(rowChange.GetRowDatas())),
		}

		for _, rowData := range rowChange.GetRowDatas() {
			row := outbox.CanalRow{}
			for _, col := range rowData.GetAfterColumns() {
				assignColumn(&row, col)
			}
			envelope.Data = append(envelope.Data, row)
		}

		body, err := json.Marshal(envelope)
		if err != nil {
			return nil, err
		}
		payloads = append(payloads, body)
	}

	return payloads, nil
}

func assignColumn(row *outbox.CanalRow, col *pbe.Column) {
	switch strings.ToLower(col.GetName()) {
	case "id":
		row.ID = col.GetValue()
	case "aggregate_type":
		row.AggregateType = col.GetValue()
	case "aggregate_id":
		row.AggregateID = col.GetValue()
	case "type":
		row.Type = col.GetValue()
	case "payload":
		row.Payload = col.GetValue()
	}
}

func eventTypeName(eventType pbe.EventType) string {
	switch eventType {
	case pbe.EventType_INSERT:
		return "INSERT"
	case pbe.EventType_UPDATE:
		return "UPDATE"
	default:
		return ""
	}
}
