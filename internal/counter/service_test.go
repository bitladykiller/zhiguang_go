package counter

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net"
	"os/exec"
	"strconv"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/segmentio/kafka-go"
)

func TestTogglePublishFailureMarksDirty(t *testing.T) {
	t.Helper()

	rdb, shutdown := startTestRedis(t)
	defer shutdown()

	svc := NewCounterService(rdb, &stubCounterPublisher{err: errors.New("kafka down")}, nil)
	ctx := context.Background()
	entityType := "knowpost"
	entityID := "42"

	changed, err := svc.Like(ctx, 1001, entityType, entityID)
	if err != nil {
		t.Fatalf("like: %v", err)
	}
	if !changed {
		t.Fatalf("expected toggle to change bitmap state")
	}

	waitForCondition(t, 2*time.Second, func() bool {
		ok, err := rdb.SIsMember(ctx, DirtySetKey(), DirtyMember(entityType, entityID)).Result()
		return err == nil && ok
	})
}

func TestApplyBatchWritesDeltaIntoSds(t *testing.T) {
	t.Helper()

	rdb, shutdown := startTestRedis(t)
	defer shutdown()

	ctx := context.Background()
	entityType := "knowpost"
	entityID := "99"
	cntKey := SdsKey(entityType, entityID)

	raw := make([]byte, SchemaLen*FieldSize)
	writeInt32BE(raw, IdxLike*FieldSize, 5)
	writeInt32BE(raw, IdxFav*FieldSize, 1)
	if err := rdb.Set(ctx, cntKey, raw, 0).Err(); err != nil {
		t.Fatalf("seed sds: %v", err)
	}

	svc := NewCounterService(rdb, nil, nil)
	consumer := &AggregationConsumer{service: svc}
	batch := newCounterBatch(2)
	if err := batch.add(mustCounterMessage(t, CounterEvent{
		EntityType: entityType,
		EntityID:   entityID,
		Metric:     "like",
		Index:      IdxLike,
		Delta:      2,
	})); err != nil {
		t.Fatalf("add like event: %v", err)
	}
	if err := batch.add(mustCounterMessage(t, CounterEvent{
		EntityType: entityType,
		EntityID:   entityID,
		Metric:     "fav",
		Index:      IdxFav,
		Delta:      -1,
	})); err != nil {
		t.Fatalf("add fav event: %v", err)
	}

	if err := svc.markDirtyMembers(ctx, batch.dirtyMembers()); err != nil {
		t.Fatalf("mark dirty: %v", err)
	}
	if err := consumer.applyBatch(ctx, batch); err != nil {
		t.Fatalf("apply batch: %v", err)
	}

	gotRaw, err := rdb.Get(ctx, cntKey).Bytes()
	if err != nil {
		t.Fatalf("get sds after apply: %v", err)
	}
	if got := readInt32BE(gotRaw, IdxLike*FieldSize); got != 7 {
		t.Fatalf("unexpected like count after apply: got=%d want=7", got)
	}
	if got := readInt32BE(gotRaw, IdxFav*FieldSize); got != 0 {
		t.Fatalf("unexpected fav count after apply: got=%d want=0", got)
	}

	ok, err := rdb.SIsMember(ctx, DirtySetKey(), DirtyMember(entityType, entityID)).Result()
	if err != nil {
		t.Fatalf("check dirty set: %v", err)
	}
	if !ok {
		t.Fatalf("expected dirty marker to be present before commit cleanup")
	}
}

func TestRepairDirtyMemberOverwritesSnapshotFromBitmap(t *testing.T) {
	t.Helper()

	rdb, shutdown := startTestRedis(t)
	defer shutdown()

	ctx := context.Background()
	entityType := "knowpost"
	entityID := "77"

	svc := NewCounterService(rdb, nil, nil)
	if _, err := svc.Like(ctx, 1001, entityType, entityID); err != nil {
		t.Fatalf("like first user: %v", err)
	}
	if _, err := svc.Like(ctx, 1002, entityType, entityID); err != nil {
		t.Fatalf("like second user: %v", err)
	}

	raw := make([]byte, SchemaLen*FieldSize)
	writeInt32BE(raw, IdxLike*FieldSize, 9)
	if err := rdb.Set(ctx, SdsKey(entityType, entityID), raw, 0).Err(); err != nil {
		t.Fatalf("seed wrong sds: %v", err)
	}
	if err := svc.markDirty(ctx, entityType, entityID); err != nil {
		t.Fatalf("mark dirty: %v", err)
	}

	consumer := &AggregationConsumer{service: svc}
	if err := consumer.repairDirtyMember(ctx, DirtyMember(entityType, entityID)); err != nil {
		t.Fatalf("repair dirty member: %v", err)
	}

	gotRaw, err := rdb.Get(ctx, SdsKey(entityType, entityID)).Bytes()
	if err != nil {
		t.Fatalf("get repaired sds: %v", err)
	}
	if got := readInt32BE(gotRaw, IdxLike*FieldSize); got != 2 {
		t.Fatalf("unexpected like count after repair: got=%d want=2", got)
	}

	ok, err := rdb.SIsMember(ctx, DirtySetKey(), DirtyMember(entityType, entityID)).Result()
	if err != nil {
		t.Fatalf("check dirty set: %v", err)
	}
	if ok {
		t.Fatalf("expected dirty marker to be removed after repair")
	}
}

type stubCounterPublisher struct {
	err error
}

func (p *stubCounterPublisher) Publish(event *CounterEvent) error {
	return p.err
}

func mustCounterMessage(t *testing.T, event CounterEvent) kafka.Message {
	t.Helper()

	data, err := json.Marshal(event)
	if err != nil {
		t.Fatalf("marshal event: %v", err)
	}
	return kafka.Message{
		Key:   []byte(event.EntityType + ":" + event.EntityID),
		Value: data,
	}
}

func waitForCondition(t *testing.T, timeout time.Duration, fn func() bool) {
	t.Helper()

	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if fn() {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("condition was not met within %s", timeout)
}

func startTestRedis(t *testing.T) (*redis.Client, func()) {
	t.Helper()

	port := reservePort(t)
	dataDir := t.TempDir()
	cmd := exec.Command(
		"redis-server",
		"--save", "",
		"--appendonly", "no",
		"--bind", "127.0.0.1",
		"--port", strconv.Itoa(port),
		"--dir", dataDir,
	)

	var output bytes.Buffer
	cmd.Stdout = &output
	cmd.Stderr = &output

	if err := cmd.Start(); err != nil {
		t.Fatalf("start redis-server: %v", err)
	}

	addr := "127.0.0.1:" + strconv.Itoa(port)
	client := redis.NewClient(&redis.Options{Addr: addr})

	deadline := time.Now().Add(5 * time.Second)
	for {
		if err := client.Ping(context.Background()).Err(); err == nil {
			break
		}
		if time.Now().After(deadline) {
			_ = cmd.Process.Kill()
			_ = cmd.Wait()
			_ = client.Close()
			t.Fatalf("redis-server did not become ready: %s", output.String())
		}
		time.Sleep(50 * time.Millisecond)
	}

	return client, func() {
		_ = client.Close()
		if cmd.Process != nil {
			_ = cmd.Process.Kill()
		}
		_ = cmd.Wait()
	}
}

func reservePort(t *testing.T) int {
	t.Helper()

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("reserve tcp port: %v", err)
	}
	defer ln.Close()

	addr, ok := ln.Addr().(*net.TCPAddr)
	if !ok {
		t.Fatalf("unexpected addr type: %T", ln.Addr())
	}
	return addr.Port
}
