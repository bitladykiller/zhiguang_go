package cache

import (
	"context"
	"os"
	"strings"
	"testing"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
	"go.uber.org/zap"
)

// newTestBloom 连接带 RedisBloom 的 Redis；不可用则 Skip。
// 环境变量 REDIS_BLOOM_ADDR 默认 127.0.0.1:6379。
func newTestBloom(t *testing.T) *RedisBloom {
	t.Helper()
	rdb := requireRedisBloom(t)
	cfg := BloomConfig{
		Enabled:           true,
		ExpectedItems:     1000,
		FalsePositiveRate: 0.01,
		Key:               "cf:test:" + t.Name(),
		BucketSize:        2,
		MaxKicks:          20,
		Expansion:         1,
	}
	b := NewRedisBloom(rdb, cfg, zap.NewNop())
	if b == nil {
		t.Fatal("bloom nil")
	}
	t.Cleanup(func() {
		_ = rdb.Del(context.Background(), b.key).Err()
	})
	return b
}

func requireRedisBloom(t *testing.T) *redis.Client {
	t.Helper()
	addr := os.Getenv("REDIS_BLOOM_ADDR")
	if addr == "" {
		addr = "127.0.0.1:6379"
	}
	rdb := redis.NewClient(&redis.Options{Addr: addr})
	t.Cleanup(func() { _ = rdb.Close() })

	ctx := context.Background()
	if err := rdb.Ping(ctx).Err(); err != nil {
		t.Skipf("redis not reachable at %s: %v", addr, err)
	}
	probe := "cf:probe:" + t.Name()
	err := rdb.Do(ctx, "CF.RESERVE", probe, 100).Err()
	if err != nil {
		if isUnknownCommandErr(err) {
			t.Skipf("RedisBloom CF.* unavailable at %s: %v (use redis/redis-stack-server)", addr, err)
		}
		// probe key 可能已存在
		if !isAlreadyExistsErr(err) {
			// 其它错误：仍尝试 EXISTS 探测
			if e2 := rdb.Do(ctx, "CF.ADD", probe, "x").Err(); e2 != nil && isUnknownCommandErr(e2) {
				t.Skipf("RedisBloom CF.* unavailable: %v", e2)
			}
		}
	}
	_ = rdb.Del(ctx, probe).Err()
	return rdb
}

func TestRedisBloom_NilSafe(t *testing.T) {
	var b *RedisBloom
	b.AddUint64(context.Background(), 1)
	b.DeleteUint64(context.Background(), 1)
	ok, err := b.MightContainUint64(context.Background(), 1)
	if err != nil || !ok {
		t.Fatalf("nil bloom should fail-open: ok=%v err=%v", ok, err)
	}
}

func TestRedisBloom_DisabledReturnsNil(t *testing.T) {
	rdb := redis.NewClient(&redis.Options{Addr: "127.0.0.1:0"})
	if got := NewRedisBloom(rdb, BloomConfig{Enabled: false}, nil); got != nil {
		t.Fatal("disabled should return nil")
	}
	if got := NewRedisBloom(nil, DefaultBloomConfig(), nil); got != nil {
		t.Fatal("nil rdb should return nil")
	}
}

func TestRedisBloom_ColdStartFailOpen(t *testing.T) {
	b := newTestBloom(t)
	ctx := context.Background()
	// 未 Add、未 RESERVE 成功前键可能不存在
	// ensure 前直接查：若键不存在则 fail-open
	ok, err := b.MightContainUint64(ctx, 42)
	if err != nil {
		t.Fatal(err)
	}
	if !ok {
		t.Fatal("cold filter must MightContain=true (fail-open)")
	}
}

func TestRedisBloom_AddAndMightContain(t *testing.T) {
	b := newTestBloom(t)
	ctx := context.Background()

	b.AddUint64(ctx, 42)
	if !b.IsWarm(ctx) {
		t.Fatal("after Add should be warm")
	}
	ok, err := b.MightContainUint64(ctx, 42)
	if err != nil {
		t.Fatal(err)
	}
	if !ok {
		t.Fatal("added id must MightContain=true")
	}

	ok, err = b.MightContainUint64(ctx, 999999)
	if err != nil {
		t.Fatal(err)
	}
	if ok {
		t.Fatal("unrelated id should MightContain=false after warm")
	}
}

func TestRedisBloom_DeleteRemovesMember(t *testing.T) {
	b := newTestBloom(t)
	ctx := context.Background()

	b.AddUint64(ctx, 42)
	b.AddUint64(ctx, 43)
	ok, err := b.MightContainUint64(ctx, 42)
	if err != nil || !ok {
		t.Fatalf("pre-delete: ok=%v err=%v", ok, err)
	}

	b.DeleteUint64(ctx, 42)
	ok, err = b.MightContainUint64(ctx, 42)
	if err != nil {
		t.Fatal(err)
	}
	if ok {
		t.Fatal("deleted id should MightContain=false")
	}

	ok, err = b.MightContainUint64(ctx, 43)
	if err != nil || !ok {
		t.Fatalf("sibling must remain: ok=%v err=%v", ok, err)
	}
}

func TestRedisBloom_DeleteIdempotent(t *testing.T) {
	b := newTestBloom(t)
	ctx := context.Background()
	b.DeleteUint64(ctx, 1)
	b.AddUint64(ctx, 7)
	b.DeleteUint64(ctx, 7)
	b.DeleteUint64(ctx, 7)
	ok, err := b.MightContainUint64(ctx, 7)
	if err != nil {
		t.Fatal(err)
	}
	if ok {
		t.Fatal("after double delete should be absent")
	}
}

func TestRedisBloom_NoFalseNegative(t *testing.T) {
	b := newTestBloom(t)
	ctx := context.Background()
	for i := uint64(1); i <= 200; i++ {
		b.AddUint64(ctx, i)
	}
	for i := uint64(1); i <= 200; i++ {
		ok, err := b.MightContainUint64(ctx, i)
		if err != nil {
			t.Fatal(err)
		}
		if !ok {
			t.Fatalf("false negative on id=%d", i)
		}
	}
}

func TestRedisBloom_AddDeleteCycleNoFalseNegative(t *testing.T) {
	b := newTestBloom(t)
	ctx := context.Background()
	for i := uint64(1); i <= 100; i++ {
		b.AddUint64(ctx, i)
	}
	for i := uint64(1); i <= 50; i++ {
		b.DeleteUint64(ctx, i)
	}
	for i := uint64(51); i <= 100; i++ {
		ok, err := b.MightContainUint64(ctx, i)
		if err != nil {
			t.Fatal(err)
		}
		if !ok {
			t.Fatalf("false negative after partial delete id=%d", i)
		}
	}
	for i := uint64(1); i <= 50; i++ {
		ok, err := b.MightContainUint64(ctx, i)
		if err != nil {
			t.Fatal(err)
		}
		if ok {
			t.Fatalf("deleted id still present id=%d", i)
		}
	}
}

func TestDefaultBloomConfig(t *testing.T) {
	cfg := DefaultBloomConfig()
	if !cfg.Enabled || cfg.ExpectedItems == 0 || cfg.Key == "" {
		t.Fatalf("bad defaults: %+v", cfg)
	}
	if !strings.HasPrefix(cfg.Key, "cf:") {
		t.Fatalf("default key should use cf: prefix: %s", cfg.Key)
	}
}

func TestRedisBloom_Stats(t *testing.T) {
	b := newTestBloom(t)
	key, m, k := b.Stats()
	if key == "" || m == 0 || k == 0 {
		t.Fatalf("stats key=%s m=%d k=%d", key, m, k)
	}
	var nilB *RedisBloom
	if k2, m2, h2 := nilB.Stats(); k2 != "" || m2 != 0 || h2 != 0 {
		t.Fatal("nil stats should be zero")
	}
}

func TestRedisBloom_Info(t *testing.T) {
	b := newTestBloom(t)
	ctx := context.Background()
	b.AddUint64(ctx, 1)
	info, err := b.Info(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(info) == 0 {
		t.Fatal("expected CF.INFO fields")
	}
}

func TestRedisBloom_EmptyMemberNoop(t *testing.T) {
	b := newTestBloom(t)
	ctx := context.Background()
	b.Add(ctx, "")
	b.Delete(ctx, "")
	ok, err := b.MightContain(ctx, "")
	if err != nil || !ok {
		t.Fatalf("empty member fail-open: ok=%v err=%v", ok, err)
	}
}

func TestRedisBloom_ReAddAfterDelete(t *testing.T) {
	b := newTestBloom(t)
	ctx := context.Background()
	b.AddUint64(ctx, 9)
	b.DeleteUint64(ctx, 9)
	b.AddUint64(ctx, 9)
	ok, err := b.MightContainUint64(ctx, 9)
	if err != nil || !ok {
		t.Fatalf("re-add: ok=%v err=%v", ok, err)
	}
}

func TestRedisBloom_DuplicateAddIdempotent(t *testing.T) {
	b := newTestBloom(t)
	ctx := context.Background()
	b.AddUint64(ctx, 55)
	b.AddUint64(ctx, 55)
	ok, err := b.MightContainUint64(ctx, 55)
	if err != nil || !ok {
		t.Fatalf("dup add: ok=%v err=%v", ok, err)
	}
}

func TestNewRedisBloom_ConfigDefaults(t *testing.T) {
	// 不连 Redis，只测构造
	rdb := redis.NewClient(&redis.Options{Addr: "127.0.0.1:0"})
	b := NewRedisBloom(rdb, BloomConfig{Enabled: true}, nil)
	if b == nil {
		t.Fatal("expected non-nil")
	}
	if b.key != "cf:knowpost:ids" {
		t.Fatalf("default key=%s", b.key)
	}
	if b.bucketSize != 2 {
		t.Fatalf("bucketSize=%d", b.bucketSize)
	}
	b2 := NewRedisBloom(rdb, BloomConfig{
		Enabled: true, ExpectedItems: 100, FalsePositiveRate: 0.0005, Key: "cf:x",
	}, zap.NewNop())
	if b2.bucketSize != 4 {
		t.Fatalf("strict fpr bucketSize=%d want 4", b2.bucketSize)
	}
}

func TestParseCFInfoAndHelpers(t *testing.T) {
	m := parseCFInfo([]interface{}{"Number of items inserted", int64(3), "Size", int64(100)})
	if m["Number of items inserted"] != 3 || m["Size"] != 100 {
		t.Fatalf("%v", m)
	}
	m2 := parseCFInfo(map[string]interface{}{"Size": int64(9)})
	if m2["Size"] != 9 {
		t.Fatalf("%v", m2)
	}
	if toInt64(int64(1)) != 1 || toInt64(2) != 2 || toInt64("3") != 3 || toInt64([]byte("4")) != 4 {
		t.Fatal("toInt64")
	}
	if !isUnknownCommandErr(errorsNew("ERR unknown command `CF.ADD`")) {
		t.Fatal("unknown command detect")
	}
	if !isAlreadyExistsErr(errorsNew("ERR item exists")) {
		t.Fatal("exists detect")
	}
	if !isNotExistErr(errorsNew("not found")) || !isNotExistErr(redis.Nil) {
		t.Fatal("not exist detect")
	}
	if !redisTruthy(true) || !redisTruthy(int64(1)) || redisTruthy(int64(0)) || !redisTruthy("1") {
		t.Fatal("redisTruthy")
	}
}

func TestRedisBloom_ModuleFailFailOpen(t *testing.T) {
	// 模拟 miniredis / 无模块：CF.* unknown → 之后 fail-open
	srv, err := miniredis.Run()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(srv.Close)
	rdb := redis.NewClient(&redis.Options{Addr: srv.Addr()})
	t.Cleanup(func() { _ = rdb.Close() })
	b := NewRedisBloom(rdb, BloomConfig{
		Enabled: true, ExpectedItems: 100, Key: "cf:nomin",
	}, zap.NewNop())
	ctx := context.Background()
	b.AddUint64(ctx, 1) // 触发 ensure → unknown command
	ok, err := b.MightContainUint64(ctx, 1)
	if err != nil {
		// fail-open 可能带 err，也可能 moduleFail 后无 err
		_ = err
	}
	if !ok {
		t.Fatal("module missing must fail-open MightContain=true")
	}
	if b.IsWarm(ctx) {
		t.Fatal("module fail should not report warm")
	}
	b.DeleteUint64(ctx, 1) // no-op path
	if _, err := b.Info(ctx); err == nil {
		t.Fatal("Info should fail without module")
	}
}

func TestRedisBloom_ReserveIdempotentAndDeleteMissing(t *testing.T) {
	b := newTestBloom(t)
	ctx := context.Background()
	// 第二次 ensure：键已存在走 Exists 短路
	b.AddUint64(ctx, 1)
	b.AddUint64(ctx, 2)
	// 删除不存在的 member
	b.DeleteUint64(ctx, 999001)
	ok, err := b.MightContainUint64(ctx, 1)
	if err != nil || !ok {
		t.Fatalf("ok=%v err=%v", ok, err)
	}
	// 并发 RESERVE 路径：对同一 key 再建一个客户端配置
	addr := os.Getenv("REDIS_BLOOM_ADDR")
	if addr == "" {
		addr = "127.0.0.1:6379"
	}
	rdb := redis.NewClient(&redis.Options{Addr: addr})
	t.Cleanup(func() { _ = rdb.Close() })
	b2 := NewRedisBloom(rdb, BloomConfig{
		Enabled: true, ExpectedItems: 1000, Key: b.key,
	}, zap.NewNop())
	b2.AddUint64(ctx, 3)
	ok, err = b2.MightContainUint64(ctx, 3)
	if err != nil || !ok {
		t.Fatalf("second client: ok=%v err=%v", ok, err)
	}
}

func TestRedisTruthyMore(t *testing.T) {
	if !redisTruthy([]byte("1")) || redisTruthy([]byte("0")) {
		t.Fatal("bytes")
	}
	if !redisTruthy(int(1)) || redisTruthy(int(0)) {
		t.Fatal("int")
	}
	if redisTruthy("false") {
		t.Fatal("false string")
	}
	m := parseCFInfo(map[interface{}]interface{}{"A": int64(1)})
	if m["A"] != 1 {
		t.Fatalf("%v", m)
	}
}

// 避免再引 errors 包名冲突的小封装
func errorsNew(s string) error {
	return &simpleErr{s}
}

type simpleErr struct{ s string }

func (e *simpleErr) Error() string { return e.s }
