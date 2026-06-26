package outbox

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/segmentio/kafka-go"
	"go.uber.org/zap"
)

// --- mocks ---

type mockHandler struct {
	err error
}

func (h *mockHandler) HandleRow(_ context.Context, row Row) error {
	return h.err
}

type mockRecorder struct {
	created bool
	topic   string
	key     string
	cause   error
}

func (r *mockRecorder) Create(_ context.Context, topic, key string, payload []byte, cause error) error {
	r.created = true
	r.topic = topic
	r.key = key
	r.cause = cause
	return nil
}

// --- NewConsumer ---

func TestNewConsumer_NilArgs(t *testing.T) {
	if got := NewConsumer(nil, &mockHandler{}, nil); got != nil {
		t.Error("expected nil when reader is nil")
	}
	if got := NewConsumer(&kafka.Reader{}, nil, nil); got != nil {
		t.Error("expected nil when handler is nil")
	}
}

func TestNewConsumer_Defaults(t *testing.T) {
	c := NewConsumer(&kafka.Reader{}, &mockHandler{}, nil)
	if c == nil {
		t.Fatal("expected non-nil consumer")
	}
	if c.maxRetries != 3 {
		t.Errorf("maxRetries = %d, want 3", c.maxRetries)
	}
	if c.retryDelay != time.Second {
		t.Errorf("retryDelay = %v, want 1s", c.retryDelay)
	}
}

func TestNewConsumer_Logger(t *testing.T) {
	logger := zap.NewNop()
	c := NewConsumer(&kafka.Reader{}, &mockHandler{}, logger)
	if c.logger != logger {
		t.Error("logger not set")
	}
}

// --- SetFailedMessageRecorder ---

func TestSetFailedMessageRecorder(t *testing.T) {
	c := NewConsumer(&kafka.Reader{}, &mockHandler{}, nil)
	rec := &mockRecorder{}
	c.SetFailedMessageRecorder(rec)
	if c.failureRecorder != rec {
		t.Error("recorder not set")
	}
}

// --- Start (nil receiver) ---

func TestStart_NilReceiver(t *testing.T) {
	var c *Consumer
	c.Start(context.Background())
}

// --- extractRows ---

func TestExtractRows_InvalidJSON(t *testing.T) {
	_, err := extractRows([]byte(`{invalid`))
	if err == nil {
		t.Fatal("expected error for invalid JSON")
	}
}

func TestExtractRows_WrongTable(t *testing.T) {
	payload := `{"table":"other","type":"INSERT","data":[]}`
	rows, err := extractRows([]byte(payload))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(rows) != 0 {
		t.Errorf("expected 0 rows for wrong table, got %d", len(rows))
	}
}

func TestExtractRows_WrongEventType(t *testing.T) {
	tests := []string{"DELETE", "QUERY", ""}
	for _, typ := range tests {
		payload := `{"table":"outbox","type":"` + typ + `","data":[]}`
		rows, err := extractRows([]byte(payload))
		if err != nil {
			t.Fatalf("type=%q unexpected error: %v", typ, err)
		}
		if len(rows) != 0 {
			t.Errorf("type=%q expected 0 rows, got %d", typ, len(rows))
		}
	}
}

func TestExtractRows_INSERT(t *testing.T) {
	payload := `{"table":"outbox","type":"INSERT","data":[{"aggregate_type":"knowpost","aggregate_id":"42","type":"KnowPostPublished","payload":"{}"}]}`
	rows, err := extractRows([]byte(payload))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(rows) != 1 {
		t.Fatalf("expected 1 row, got %d", len(rows))
	}
	if rows[0].AggregateType != "knowpost" || rows[0].AggregateID != "42" {
		t.Errorf("unexpected row fields: %+v", rows[0])
	}
}

func TestExtractRows_UPDATE(t *testing.T) {
	payload := `{"table":"outbox","type":"UPDATE","data":[{"aggregate_type":"knowpost","aggregate_id":"1","type":"KnowPostUpdated","payload":"{}"}]}`
	rows, err := extractRows([]byte(payload))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(rows) != 1 {
		t.Fatalf("expected 1 row, got %d", len(rows))
	}
}

func TestExtractRows_MultipleRows(t *testing.T) {
	payload := `{"table":"outbox","type":"INSERT","data":[{"aggregate_type":"a","aggregate_id":"1","type":"A","payload":"{}"},{"aggregate_type":"b","aggregate_id":"2","type":"B","payload":"{}"}]}`
	rows, err := extractRows([]byte(payload))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(rows) != 2 {
		t.Fatalf("expected 2 rows, got %d", len(rows))
	}
}

func TestExtractRows_EmptyData(t *testing.T) {
	payload := `{"table":"outbox","type":"INSERT","data":[]}`
	rows, err := extractRows([]byte(payload))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(rows) != 0 {
		t.Errorf("expected 0 rows, got %d", len(rows))
	}
}

func TestExtractRows_EmptyPayloadString(t *testing.T) {
	payload := `{"table":"outbox","type":"INSERT","data":[{"aggregate_type":"knowpost","aggregate_id":"1","type":"KnowPostPublished","payload":""}]}`
	rows, err := extractRows([]byte(payload))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(rows) != 1 {
		t.Fatalf("expected 1 row, got %d", len(rows))
	}
	if len(rows[0].Payload) != 0 {
		t.Errorf("expected empty payload bytes, got %d bytes", len(rows[0].Payload))
	}
}

// --- handleMessage ---

func TestHandleMessage_EmptyPayloadRowSkipped(t *testing.T) {
	payload := `{"table":"outbox","type":"INSERT","data":[{"aggregate_type":"knowpost","aggregate_id":"1","type":"KnowPostPublished","payload":""}]}`
	handler := &mockHandler{}
	c := NewConsumer(&kafka.Reader{}, handler, nil)
	err := c.handleMessage(context.Background(), []byte(payload))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestHandleMessage_HandlerError(t *testing.T) {
	payload := `{"table":"outbox","type":"INSERT","data":[{"aggregate_type":"knowpost","aggregate_id":"1","type":"KnowPostPublished","payload":"{}"}]}`
	handler := &mockHandler{err: errors.New("handler failed")}
	c := NewConsumer(&kafka.Reader{}, handler, nil)
	err := c.handleMessage(context.Background(), []byte(payload))
	if err == nil {
		t.Fatal("expected error from handler")
	}
}

func TestHandleMessage_HandlerErrorSecondRow(t *testing.T) {
	payload := `{"table":"outbox","type":"INSERT","data":[{"aggregate_type":"a","aggregate_id":"1","type":"A","payload":"{}"},{"aggregate_type":"b","aggregate_id":"2","type":"B","payload":"{}"}]}`
	handler := &mockHandler{err: errors.New("fail")}
	c := NewConsumer(&kafka.Reader{}, handler, nil)
	err := c.handleMessage(context.Background(), []byte(payload))
	if err == nil {
		t.Fatal("expected error")
	}
}

func TestHandleMessage_InvalidEnvelope(t *testing.T) {
	c := NewConsumer(&kafka.Reader{}, &mockHandler{}, nil)
	err := c.handleMessage(context.Background(), []byte(`{bad`))
	if err == nil {
		t.Fatal("expected error for invalid envelope")
	}
}

// --- recordFailedMessage ---

func TestRecordFailedMessage_NilRecorder(t *testing.T) {
	c := NewConsumer(&kafka.Reader{}, &mockHandler{}, nil)
	c.recordFailedMessage(context.Background(), []byte("data"), errors.New("cause"))
}

func TestRecordFailedMessage_WithRecorder(t *testing.T) {
	rec := &mockRecorder{}
	c := NewConsumer(&kafka.Reader{}, &mockHandler{}, nil)
	c.SetFailedMessageRecorder(rec)
	c.recordFailedMessage(context.Background(), []byte("data"), errors.New("cause"))
	if !rec.created {
		t.Error("expected recorder to be called")
	}
	if rec.topic != CanalOutboxTopic {
		t.Errorf("topic = %q, want %q", rec.topic, CanalOutboxTopic)
	}
}

// --- handleMessageWithRetry ---

func TestHandleMessageWithRetry_Exhausted(t *testing.T) {
	payload := `{"table":"outbox","type":"INSERT","data":[{"aggregate_type":"knowpost","aggregate_id":"1","type":"KnowPostPublished","payload":"{}"}]}`
	handler := &mockHandler{err: errors.New("always fail")}
	c := NewConsumer(&kafka.Reader{}, handler, zap.NewNop())
	c.maxRetries = 2
	c.retryDelay = time.Millisecond

	err := c.handleMessageWithRetry(context.Background(), kafka.Message{Value: []byte(payload)})
	if err == nil {
		t.Fatal("expected error after exhausting retries")
	}
}

func TestHandleMessageWithRetry_Success(t *testing.T) {
	payload := `{"table":"outbox","type":"INSERT","data":[{"aggregate_type":"knowpost","aggregate_id":"1","type":"KnowPostPublished","payload":"{}"}]}`
	handler := &mockHandler{}
	c := NewConsumer(&kafka.Reader{}, handler, zap.NewNop())
	err := c.handleMessageWithRetry(context.Background(), kafka.Message{Value: []byte(payload)})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestHandleMessageWithRetry_InvalidPayload(t *testing.T) {
	handler := &mockHandler{}
	c := NewConsumer(&kafka.Reader{}, handler, zap.NewNop())
	err := c.handleMessageWithRetry(context.Background(), kafka.Message{Value: []byte(`bad`)})
	if err == nil {
		t.Fatal("expected error for invalid payload")
	}
}

// --- logWarn ---

func TestLogWarn_NilLogger(t *testing.T) {
	c := NewConsumer(&kafka.Reader{}, &mockHandler{}, nil)
	c.logWarn("test", errors.New("err"))
}

func TestLogWarn_WithLogger(t *testing.T) {
	logger := zap.NewNop()
	c := NewConsumer(&kafka.Reader{}, &mockHandler{}, logger)
	c.logWarn("test", errors.New("err"))
}
