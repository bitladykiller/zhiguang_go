package counter

import (
	"bytes"
	"context"
	"net"
	"os/exec"
	"strconv"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
)

func TestToggleKeepsSnapshotAndAggregationBucket(t *testing.T) {
	t.Helper()

	rdb, shutdown := startTestRedis(t)
	defer shutdown()

	svc := NewCounterService(rdb, nil, nil)
	ctx := context.Background()

	entityType := "knowpost"
	entityID := "42"

	raw := make([]byte, SchemaLen*FieldSize)
	writeInt32BE(raw, IdxLike*FieldSize, 7)
	if err := rdb.Set(ctx, SdsKey(entityType, entityID), raw, 0).Err(); err != nil {
		t.Fatalf("seed sds: %v", err)
	}
	if err := rdb.HSet(ctx, AggKey(entityType, entityID), "0", 3).Err(); err != nil {
		t.Fatalf("seed agg: %v", err)
	}

	changed, err := svc.Like(ctx, 1001, entityType, entityID)
	if err != nil {
		t.Fatalf("like: %v", err)
	}
	if !changed {
		t.Fatalf("expected toggle to change bitmap state")
	}

	gotRaw, err := rdb.Get(ctx, SdsKey(entityType, entityID)).Bytes()
	if err != nil {
		t.Fatalf("get sds: %v", err)
	}
	if len(gotRaw) != len(raw) {
		t.Fatalf("unexpected sds length: got=%d want=%d", len(gotRaw), len(raw))
	}
	if got := readInt32BE(gotRaw, IdxLike*FieldSize); got != 7 {
		t.Fatalf("unexpected like count in sds: got=%d want=7", got)
	}

	agg, err := rdb.HGet(ctx, AggKey(entityType, entityID), "0").Int64()
	if err != nil {
		t.Fatalf("get agg field: %v", err)
	}
	if agg != 3 {
		t.Fatalf("unexpected agg delta: got=%d want=3", agg)
	}
}

func TestFlushAggregationWritesDeltaIntoSds(t *testing.T) {
	t.Helper()

	rdb, shutdown := startTestRedis(t)
	defer shutdown()

	ctx := context.Background()
	entityType := "knowpost"
	entityID := "99"
	aggKey := AggKey(entityType, entityID)
	cntKey := SdsKey(entityType, entityID)

	raw := make([]byte, SchemaLen*FieldSize)
	writeInt32BE(raw, IdxLike*FieldSize, 5)
	writeInt32BE(raw, IdxFav*FieldSize, 1)
	if err := rdb.Set(ctx, cntKey, raw, 0).Err(); err != nil {
		t.Fatalf("seed sds: %v", err)
	}
	if err := rdb.HSet(ctx, aggKey, "0", 2, "1", -1).Err(); err != nil {
		t.Fatalf("seed agg: %v", err)
	}

	consumer := &AggregationConsumer{redis: rdb}
	if err := consumer.flushAggregation(ctx, aggKey); err != nil {
		t.Fatalf("flush aggregation: %v", err)
	}

	gotRaw, err := rdb.Get(ctx, cntKey).Bytes()
	if err != nil {
		t.Fatalf("get sds after flush: %v", err)
	}
	if got := readInt32BE(gotRaw, IdxLike*FieldSize); got != 7 {
		t.Fatalf("unexpected like count after flush: got=%d want=7", got)
	}
	if got := readInt32BE(gotRaw, IdxFav*FieldSize); got != 0 {
		t.Fatalf("unexpected fav count after flush: got=%d want=0", got)
	}

	exists, err := rdb.Exists(ctx, aggKey).Result()
	if err != nil {
		t.Fatalf("check agg exists: %v", err)
	}
	if exists != 0 {
		t.Fatalf("expected agg key to be deleted after flush, exists=%d", exists)
	}
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
