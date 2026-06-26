package canal

import (
	"encoding/json"
	"testing"

	pbe "github.com/withlin/canal-go/protocol/entry"
	"google.golang.org/protobuf/proto"

	"github.com/zhiguang/app/internal/outbox"
)

// --- eventTypeName ---

func TestEventTypeName_INSERT(t *testing.T) {
	if got := eventTypeName(pbe.EventType_INSERT); got != "INSERT" {
		t.Errorf("got %q, want %q", got, "INSERT")
	}
}

func TestEventTypeName_UPDATE(t *testing.T) {
	if got := eventTypeName(pbe.EventType_UPDATE); got != "UPDATE" {
		t.Errorf("got %q, want %q", got, "UPDATE")
	}
}

func TestEventTypeName_DELETE(t *testing.T) {
	if got := eventTypeName(pbe.EventType_DELETE); got != "" {
		t.Errorf("expected empty for DELETE, got %q", got)
	}
}

func TestEventTypeName_Unknown(t *testing.T) {
	if got := eventTypeName(pbe.EventType_CREATE); got != "" {
		t.Errorf("expected empty for CREATE, got %q", got)
	}
}

// --- assignColumn ---

func makeColumn(name, value string) *pbe.Column {
	return &pbe.Column{
		Name:  name,
		Value: value,
	}
}

func TestAssignColumn_ID(t *testing.T) {
	row := &outbox.CanalRow{}
	assignColumn(row, makeColumn("id", "100"))
	if row.ID != "100" {
		t.Errorf("ID = %q, want %q", row.ID, "100")
	}
}

func TestAssignColumn_CaseInsensitive(t *testing.T) {
	row := &outbox.CanalRow{}
	assignColumn(row, makeColumn("AGGREGATE_TYPE", "knowpost"))
	if row.AggregateType != "knowpost" {
		t.Errorf("AggregateType = %q, want %q", row.AggregateType, "knowpost")
	}
}

func TestAssignColumn_AllFields(t *testing.T) {
	row := &outbox.CanalRow{}
	assignColumn(row, makeColumn("id", "1"))
	assignColumn(row, makeColumn("aggregate_type", "knowpost"))
	assignColumn(row, makeColumn("aggregate_id", "42"))
	assignColumn(row, makeColumn("type", "KnowPostPublished"))
	assignColumn(row, makeColumn("payload", `{"id":42}`))

	if row.ID != "1" || row.AggregateType != "knowpost" || row.AggregateID != "42" || row.Type != "KnowPostPublished" || string(row.Payload) != `{"id":42}` {
		t.Errorf("unexpected row: %+v", row)
	}
}

func TestAssignColumn_UnknownColumnIgnored(t *testing.T) {
	row := &outbox.CanalRow{}
	assignColumn(row, makeColumn("unknown_column", "whatever"))
}

func TestAssignColumn_NilColumn(t *testing.T) {
	row := &outbox.CanalRow{}
	assignColumn(row, nil)
}

// --- maxInt ---

func TestMaxInt_Positive(t *testing.T) {
	if got := maxInt(5, 1); got != 5 {
		t.Errorf("maxInt(5,1) = %d, want 5", got)
	}
}

func TestMaxInt_Zero(t *testing.T) {
	if got := maxInt(0, 3); got != 3 {
		t.Errorf("maxInt(0,3) = %d, want 3", got)
	}
}

func TestMaxInt_Negative(t *testing.T) {
	if got := maxInt(-5, 10); got != 10 {
		t.Errorf("maxInt(-5,10) = %d, want 10", got)
	}
}

// --- parseEntries (protobuf integration) ---

func newTestEntry(tableName string, eventType pbe.EventType, columns []*pbe.Column) pbe.Entry {
	rowData := &pbe.RowData{AfterColumns: columns}
	rowChange := &pbe.RowChange{
		EventTypePresent: &pbe.RowChange_EventType{EventType: eventType},
		RowDatas:         []*pbe.RowData{rowData},
	}
	storeValue, _ := proto.Marshal(rowChange)

	return pbe.Entry{
		EntryTypePresent: &pbe.Entry_EntryType{EntryType: pbe.EntryType_ROWDATA},
		Header:           &pbe.Header{TableName: tableName},
		StoreValue:       storeValue,
	}
}

func TestParseEntries_EmptyInput(t *testing.T) {
	payloads, err := parseEntries(nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(payloads) != 0 {
		t.Errorf("expected 0 payloads, got %d", len(payloads))
	}
}

func TestParseEntries_NonRowDataEntrySkipped(t *testing.T) {
	entries := []pbe.Entry{{
		EntryTypePresent: &pbe.Entry_EntryType{EntryType: pbe.EntryType_TRANSACTIONBEGIN},
	}}
	payloads, err := parseEntries(entries)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(payloads) != 0 {
		t.Errorf("expected 0 payloads for non-ROWDATA entry, got %d", len(payloads))
	}
}

func TestParseEntries_DELETEEventSkipped(t *testing.T) {
	entries := []pbe.Entry{newTestEntry("outbox", pbe.EventType_DELETE, nil)}
	payloads, err := parseEntries(entries)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(payloads) != 0 {
		t.Errorf("expected 0 payloads for DELETE, got %d", len(payloads))
	}
}

func TestParseEntries_INSERT(t *testing.T) {
	columns := []*pbe.Column{
		makeColumn("id", "1"),
		makeColumn("aggregate_type", "knowpost"),
		makeColumn("aggregate_id", "42"),
		makeColumn("type", "KnowPostPublished"),
		makeColumn("payload", `{"id":42}`),
	}
	entries := []pbe.Entry{newTestEntry("outbox", pbe.EventType_INSERT, columns)}

	payloads, err := parseEntries(entries)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(payloads) != 1 {
		t.Fatalf("expected 1 payload, got %d", len(payloads))
	}

	var envelope outbox.CanalEnvelope
	if err := json.Unmarshal(payloads[0], &envelope); err != nil {
		t.Fatalf("unmarshal error: %v", err)
	}
	if envelope.Table != "outbox" || envelope.Type != "INSERT" || len(envelope.Data) != 1 {
		t.Errorf("unexpected envelope: %+v", envelope)
	}
	if envelope.Data[0].AggregateType != "knowpost" || envelope.Data[0].AggregateID != "42" {
		t.Errorf("unexpected row data: %+v", envelope.Data[0])
	}
}

func TestParseEntries_UPDATE(t *testing.T) {
	columns := []*pbe.Column{
		makeColumn("id", "5"),
		makeColumn("aggregate_type", "following"),
		makeColumn("aggregate_id", "99"),
		makeColumn("type", "FollowCreated"),
		makeColumn("payload", `{}`),
	}
	entries := []pbe.Entry{newTestEntry("outbox", pbe.EventType_UPDATE, columns)}

	payloads, err := parseEntries(entries)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(payloads) != 1 {
		t.Fatalf("expected 1 payload, got %d", len(payloads))
	}

	var envelope outbox.CanalEnvelope
	json.Unmarshal(payloads[0], &envelope)
	if envelope.Type != "UPDATE" {
		t.Errorf("type = %q, want %q", envelope.Type, "UPDATE")
	}
}

func TestParseEntries_MultipleRowsData(t *testing.T) {
	rowData1 := &pbe.RowData{AfterColumns: []*pbe.Column{makeColumn("id", "1"), makeColumn("payload", `{"a":1}`)}}
	rowData2 := &pbe.RowData{AfterColumns: []*pbe.Column{makeColumn("id", "2"), makeColumn("payload", `{"a":2}`)}}
	rowChange := &pbe.RowChange{
		EventTypePresent: &pbe.RowChange_EventType{EventType: pbe.EventType_INSERT},
		RowDatas:         []*pbe.RowData{rowData1, rowData2},
	}
	storeValue, _ := proto.Marshal(rowChange)
	entries := []pbe.Entry{{
		EntryTypePresent: &pbe.Entry_EntryType{EntryType: pbe.EntryType_ROWDATA},
		Header:           &pbe.Header{TableName: "outbox"},
		StoreValue:       storeValue,
	}}

	payloads, err := parseEntries(entries)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(payloads) != 2 {
		t.Fatalf("expected 2 payloads, got %d", len(payloads))
	}
}

func TestParseEntries_InvalidProtobuf(t *testing.T) {
	entries := []pbe.Entry{{
		EntryTypePresent: &pbe.Entry_EntryType{EntryType: pbe.EntryType_ROWDATA},
		StoreValue:       []byte("invalid protobuf"),
	}}
	_, err := parseEntries(entries)
	if err == nil {
		t.Fatal("expected error for invalid protobuf")
	}
}

func TestParseEntries_MultipleEntryMixed(t *testing.T) {
	entries := []pbe.Entry{
		newTestEntry("outbox", pbe.EventType_INSERT, []*pbe.Column{makeColumn("id", "1")}),
		{EntryTypePresent: &pbe.Entry_EntryType{EntryType: pbe.EntryType_TRANSACTIONEND}},
		newTestEntry("other_table", pbe.EventType_INSERT, []*pbe.Column{makeColumn("id", "2")}),
	}

	payloads, err := parseEntries(entries)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(payloads) != 2 {
		t.Fatalf("expected 2 payloads (one per ROWDATA entry), got %d", len(payloads))
	}
}

func TestParseEntries_OnlyNonRowData(t *testing.T) {
	entries := []pbe.Entry{{
		EntryTypePresent: &pbe.Entry_EntryType{EntryType: pbe.EntryType_TRANSACTIONEND},
	}}
	payloads, err := parseEntries(entries)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(payloads) != 0 {
		t.Errorf("expected 0 payloads, got %d", len(payloads))
	}
}
