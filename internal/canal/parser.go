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

// assignColumn 将 Canal 协议中的 Column 值映射到业务 CanalRow 的对应字段。
//
// 功能：
//   根据列名（小写比较）将 protobuf Column 对象的 Value 赋值到 CanalRow 结构体。
//   只关注 outbox 表的必要列：id、aggregate_type、aggregate_id、type、payload。
//   其他列会被忽略（不处理）。
//
// 参数：
//   - row: 目标 CanalRow 结构体指针
//   - col: Canal 协议中的 Column 对象，包含列名和值
//
// 函数调用说明：
//   - col.GetName(): 获取 protobuf 中 Column 的 name 字段值（列名）
//   - col.GetValue(): 获取 protobuf 中 Column 的 value 字段值（该行的列值）
//   - strings.ToLower(name): 标准库字符串函数，将列名转为小写以不区分大小写比较
//
// 设计决策：
//   - 只处理 AfterColumns（变更后的值），不处理 BeforeColumns。
//   - 列名大小写不敏感，是因为 MySQL 的 lower_case_table_names 配置不同
//     可能导致列名字段大小写不一致。
//   - 未识别的列被静默忽略（不报错），因为 outbox 表可能新增非关键列。
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

// eventTypeName 将 protobuf 中的事件类型枚举转为字符串表示。
//
// 功能：
//   将 Canal 协议中的 EventType 枚举（INSERT/UPDATE/DELETE 等）
//   映射为 Go 字符串，用于 JSON 序列化中的 "type" 字段。
//
// 参数：
//   - eventType: protobuf 枚举值（pbe.EventType）
//
// 返回值：
//   - string: "INSERT"、"UPDATE" 或空字符串（非 INSERT/UPDATE 类型）
//
// 设计决策：
//   只处理 INSERT 和 UPDATE 而不处理 DELETE，是因为 parseEntries
//   入口已过滤了 DELETE 事件，因此此处永远不需要返回 "DELETE"。
//   但 default 分支返回空字符串以防万一下游调用扩展了 eventType 处理。
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
