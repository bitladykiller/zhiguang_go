package canal

import (
	"encoding/json"
	"strings"

	pbe "github.com/withlin/canal-go/protocol/entry"
	"google.golang.org/protobuf/proto"

	"github.com/zhiguang/app/internal/outbox"
)

// parseEntries 把 Canal row events 转成写入 Kafka 的 JSON 消息。
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
