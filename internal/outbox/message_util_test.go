package outbox

import (
	"encoding/json"
	"fmt"
	"testing"
)

func TestMessageUtilExtractRows_Success(t *testing.T) {
	payload := []byte(`{"table":"outbox","type":"INSERT","data":[{"id":"1","aggregate_type":"knowpost","aggregate_id":"42","type":"KnowPostPublished","payload":"{\"entity\":\"knowpost\",\"id\":42}"}]}`)
	rows, err := ExtractRows(payload)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(rows) != 1 {
		t.Fatalf("expected 1 row, got %d", len(rows))
	}
	if rows[0].AggregateType != "knowpost" || rows[0].AggregateID != "42" {
		t.Errorf("unexpected row: %+v", rows[0])
	}
}

func TestMessageUtilExtractRows_InvalidJSON(t *testing.T) {
	_, err := ExtractRows([]byte(`not json`))
	if err == nil {
		t.Fatal("expected error for invalid JSON")
	}
}

func TestMessageUtilExtractRows_NonOutboxTable(t *testing.T) {
	payload := []byte(`{"table":"users","type":"INSERT","data":[{"id":"1"}]}`)
	rows, err := ExtractRows(payload)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(rows) != 0 {
		t.Errorf("expected 0 rows for non-outbox table, got %d", len(rows))
	}
}

func TestMessageUtilExtractRows_NotInsertOrUpdate(t *testing.T) {
	tests := []string{"DELETE", "QUERY", "CREATE"}
	for _, typ := range tests {
		payload := []byte(`{"table":"outbox","type":"` + typ + `","data":[{"id":"1"}]}`)
		rows, err := ExtractRows(payload)
		if err != nil {
			t.Fatalf("type=%q unexpected error: %v", typ, err)
		}
		if len(rows) != 0 {
			t.Errorf("type=%q expected 0 rows, got %d", typ, len(rows))
		}
	}
}

func TestMessageUtilExtractRows_NilData(t *testing.T) {
	payload := []byte(`{"table":"outbox","type":"INSERT"}`)
	rows, err := ExtractRows(payload)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(rows) != 0 {
		t.Errorf("expected 0 rows for nil data, got %d", len(rows))
	}
}

func TestMessageUtilExtractRows_EmptyData(t *testing.T) {
	payload := []byte(`{"table":"outbox","type":"INSERT","data":[]}`)
	rows, err := ExtractRows(payload)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(rows) != 0 {
		t.Errorf("expected 0 rows, got %d", len(rows))
	}
}

func TestMessageUtilExtractRows_UPDATE(t *testing.T) {
	payload := []byte(`{"table":"outbox","type":"UPDATE","data":[{"id":"2","aggregate_type":"following","aggregate_id":"100","type":"FollowCreated","payload":"{}"}]}`)
	rows, err := ExtractRows(payload)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(rows) != 1 {
		t.Fatalf("expected 1 row, got %d", len(rows))
	}
}

// --- MessageKey with registered handlers ---

func TestMessageKey_KnowPostByAggID(t *testing.T) {
	RegisterMessageKeyFunc(knowpostMessageKey)
	row := CanalRow{AggregateType: "knowpost", AggregateID: "42"}
	key := MessageKey(row)
	if key != "knowpost:42" {
		t.Errorf("got %q, want %q", key, "knowpost:42")
	}
}

func TestMessageKey_KnowPostByPayload(t *testing.T) {
	RegisterMessageKeyFunc(knowpostMessageKey)
	row := CanalRow{AggregateType: "knowpost", Payload: `{"entity":"knowpost","id":99}`}
	key := MessageKey(row)
	if key != "knowpost:99" {
		t.Errorf("got %q, want %q", key, "knowpost:99")
	}
}

func TestMessageKey_KnowPostPayloadMalformed(t *testing.T) {
	RegisterMessageKeyFunc(knowpostMessageKey)
	row := CanalRow{AggregateType: "knowpost", Payload: `{bad}`}
	key := MessageKey(row)
	if key != "" {
		t.Errorf("expected empty key for malformed payload, got %q", key)
	}
}

func TestMessageKey_KnowPostNoAggIDNoPayload(t *testing.T) {
	RegisterMessageKeyFunc(knowpostMessageKey)
	row := CanalRow{AggregateType: "knowpost"}
	key := MessageKey(row)
	if key != "" {
		t.Errorf("expected empty, got %q", key)
	}
}

func TestMessageKey_FollowingByPayload(t *testing.T) {
	RegisterMessageKeyFunc(followingMessageKey)
	row := CanalRow{AggregateType: "following", Payload: `{"from_user_id":1,"to_user_id":2}`}
	key := MessageKey(row)
	if key != "following:1:2" {
		t.Errorf("got %q, want %q", key, "following:1:2")
	}
}

func TestMessageKey_FollowingByAggID(t *testing.T) {
	RegisterMessageKeyFunc(followingMessageKey)
	row := CanalRow{AggregateType: "following", AggregateID: "55"}
	key := MessageKey(row)
	if key != "following:55" {
		t.Errorf("got %q, want %q", key, "following:55")
	}
}

func TestMessageKey_FollowingMalformedPayloadNoAggID(t *testing.T) {
	RegisterMessageKeyFunc(followingMessageKey)
	row := CanalRow{AggregateType: "following", Payload: `{bad}`}
	key := MessageKey(row)
	if key != "" {
		t.Errorf("expected empty string, got %q", key)
	}
}

// --- MessageKey fallback (no registered handlers) ---

func TestMessageKey_FallbackByTypeID(t *testing.T) {
	row := CanalRow{AggregateType: "comment", AggregateID: "10"}
	key := MessageKey(row)
	if key != "comment:10" {
		t.Errorf("got %q, want %q", key, "comment:10")
	}
}

func TestMessageKey_FallbackByTypeEvent(t *testing.T) {
	row := CanalRow{AggregateType: "notification", Type: "Notify"}
	key := MessageKey(row)
	if key != "notification:Notify" {
		t.Errorf("got %q, want %q", key, "notification:Notify")
	}
}

func TestMessageKey_FallbackByID(t *testing.T) {
	row := CanalRow{ID: "123"}
	key := MessageKey(row)
	if key != "outbox:123" {
		t.Errorf("got %q, want %q", key, "outbox:123")
	}
}

func TestMessageKey_FallbackByType(t *testing.T) {
	row := CanalRow{Type: "GenericEvent"}
	key := MessageKey(row)
	if key != "GenericEvent" {
		t.Errorf("got %q, want %q", key, "GenericEvent")
	}
}

func TestMessageKey_TrimSpaces(t *testing.T) {
	RegisterMessageKeyFunc(knowpostMessageKey)
	row := CanalRow{AggregateType: "  knowpost  ", AggregateID: "  42  "}
	key := MessageKey(row)
	if key != "knowpost:42" {
		t.Errorf("got %q, want %q", key, "knowpost:42")
	}
}

func TestMessageKey_EmptyRow(t *testing.T) {
	row := CanalRow{}
	key := MessageKey(row)
	if key != "" {
		t.Errorf("expected empty string for empty row, got %q", key)
	}
}

// --- Stub message key functions for testing ---

func knowpostMessageKey(row CanalRow) (string, bool) {
	if row.AggregateType != "knowpost" {
		return "", false
	}
	if row.AggregateID != "" {
		return "knowpost:" + row.AggregateID, true
	}
	var payload struct {
		Entity string `json:"entity"`
		ID     uint64 `json:"id"`
	}
	if err := json.Unmarshal([]byte(row.Payload), &payload); err == nil && payload.Entity == "knowpost" && payload.ID != 0 {
		return fmt.Sprintf("knowpost:%d", payload.ID), true
	}
	return "", false
}

func followingMessageKey(row CanalRow) (string, bool) {
	if row.AggregateType != "following" {
		return "", false
	}
	var evt struct {
		FromUserID uint64 `json:"from_user_id"`
		ToUserID   uint64 `json:"to_user_id"`
	}
	if err := json.Unmarshal([]byte(row.Payload), &evt); err == nil && evt.FromUserID != 0 && evt.ToUserID != 0 {
		return fmt.Sprintf("following:%d:%d", evt.FromUserID, evt.ToUserID), true
	}
	if row.AggregateID != "" {
		return "following:" + row.AggregateID, true
	}
	return "", false
}

// --- CanalEnvelope JSON round-trip ---

func TestCanalEnvelope_JSONRoundTrip(t *testing.T) {
	orig := CanalEnvelope{
		Table: "outbox",
		Type:  "INSERT",
		Data: []CanalRow{
			{ID: "1", AggregateType: "knowpost", AggregateID: "42", Type: "Published", Payload: `{"id":42}`},
		},
	}
	data, err := json.Marshal(orig)
	if err != nil {
		t.Fatalf("marshal error: %v", err)
	}
	var decoded CanalEnvelope
	if err := json.Unmarshal(data, &decoded); err != nil {
		t.Fatalf("unmarshal error: %v", err)
	}
	if decoded.Table != orig.Table || decoded.Type != orig.Type || len(decoded.Data) != 1 {
		t.Errorf("round-trip mismatch: %+v", decoded)
	}
}

func TestCanalEnvelope_OmitEmpty(t *testing.T) {
	env := CanalEnvelope{Table: "outbox", Type: "INSERT"}
	data, err := json.Marshal(env)
	if err != nil {
		t.Fatalf("marshal error: %v", err)
	}
	if len(data) == 0 {
		t.Fatal("empty JSON output")
	}
}

func TestCanalRow_OmitEmptyFields(t *testing.T) {
	row := CanalRow{ID: "1"}
	data, err := json.Marshal(row)
	if err != nil {
		t.Fatalf("marshal error: %v", err)
	}
	var decoded CanalRow
	json.Unmarshal(data, &decoded)
	if decoded.ID != "1" {
		t.Errorf("expected ID=1, got %q", decoded.ID)
	}
	if decoded.AggregateType != "" {
		t.Errorf("expected empty AggregateType, got %q", decoded.AggregateType)
	}
}
