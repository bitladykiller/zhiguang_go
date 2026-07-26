package cache

import (
	"context"
	"encoding/json"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/coocood/freecache"
	"github.com/redis/go-redis/v9"
)

type payload struct {
	ID   int    `json:"id"`
	Name string `json:"name"`
	// Liked 模拟用户维度字段：按不变量它绝不该出现在缓存里。
	Liked bool `json:"liked,omitempty"`
}

func newTiered(t *testing.T, srv *miniredis.Miniredis, l1 *freecache.Cache) *Tiered[*payload] {
	t.Helper()
	var rdb *redis.Client
	if srv != nil {
		rdb = redis.NewClient(&redis.Options{Addr: srv.Addr()})
		t.Cleanup(func() { _ = rdb.Close() })
	}
	var bc ByteCache
	if l1 != nil {
		bc = &PrefixCache{Cache: l1, Prefix: "t:"}
	}
	return &Tiered[*payload]{
		L1:           bc,
		Redis:        rdb,
		Encode:       func(p *payload) ([]byte, error) { return json.Marshal(p) },
		Decode:       func(b []byte) (*payload, error) { var p payload; err := json.Unmarshal(b, &p); return &p, err },
		L1TTLSeconds: 60,
		L2TTL:        func() time.Duration { return time.Minute },
		NullSentinel: "NULL",
		NullTTL:      func() time.Duration { return 30 * time.Second },
	}
}

func loaderOf(p *payload, calls *atomic.Int32) Loader[*payload] {
	return func(context.Context) (*payload, bool, error) {
		calls.Add(1)
		if p == nil {
			return nil, false, nil
		}
		return p, true, nil
	}
}

// TestTiered_LoadThenL1ThenL2 验证三级命中顺序与回填。
func TestTiered_LoadThenL1ThenL2(t *testing.T) {
	srv := miniredis.RunT(t)
	l1 := freecache.NewCache(256 * 1024)
	tc := newTiered(t, srv, l1)

	var calls atomic.Int32
	load := loaderOf(&payload{ID: 1, Name: "a"}, &calls)

	// 首次：Loader 回源
	v, lvl, err := tc.Get(context.Background(), "k1", load)
	if err != nil || lvl != HitLoad || v.ID != 1 {
		t.Fatalf("first get: v=%+v lvl=%v err=%v", v, lvl, err)
	}
	// 第二次：L1 命中，Loader 不再被调
	if _, lvl, _ = tc.Get(context.Background(), "k1", load); lvl != HitL1 {
		t.Fatalf("second get level = %v, want HitL1", lvl)
	}
	if calls.Load() != 1 {
		t.Fatalf("loader calls = %d, want 1", calls.Load())
	}

	// 清掉 L1：应命中 L2 并回填 L1
	l1.Clear()
	if _, lvl, _ = tc.Get(context.Background(), "k1", load); lvl != HitL2 {
		t.Fatalf("after L1 clear level = %v, want HitL2", lvl)
	}
	if _, lvl, _ = tc.Get(context.Background(), "k1", load); lvl != HitL1 {
		t.Fatalf("L1 backfill missing: level = %v, want HitL1", lvl)
	}
}

// TestTiered_CacheStoresLoaderOutputOnly 验证结构性防串号不变量：
// 调用方在拿到返回值后修改（模拟叠加用户态），缓存内容不受影响。
func TestTiered_CacheStoresLoaderOutputOnly(t *testing.T) {
	srv := miniredis.RunT(t)
	tc := newTiered(t, srv, freecache.NewCache(256*1024))

	var calls atomic.Int32
	v, _, err := tc.Get(context.Background(), "k1", loaderOf(&payload{ID: 1, Name: "a"}, &calls))
	if err != nil {
		t.Fatal(err)
	}
	v.Liked = true // 调用方叠加用户态——发生在缓存写入之后

	// L2 中的字节必须仍是共享视图
	raw, _ := srv.Get("k1")
	var cached payload
	if err := json.Unmarshal([]byte(raw), &cached); err != nil {
		t.Fatalf("unmarshal cached: %v", err)
	}
	if cached.Liked {
		t.Fatal("user-scoped field leaked into the shared cache")
	}

	// 下一个读者拿到的也必须是干净的共享视图
	v2, _, _ := tc.Get(context.Background(), "k1", loaderOf(nil, &calls))
	if v2.Liked {
		t.Fatal("next reader observed another user's state")
	}
}

// TestTiered_NullSentinel 验证空值缓存：确认不存在后写哨兵，后续命中不再回源。
func TestTiered_NullSentinel(t *testing.T) {
	srv := miniredis.RunT(t)
	tc := newTiered(t, srv, freecache.NewCache(256*1024))

	var calls atomic.Int32
	if _, _, err := tc.Get(context.Background(), "kx", loaderOf(nil, &calls)); !errors.Is(err, ErrNullCached) {
		t.Fatalf("want ErrNullCached, got %v", err)
	}
	if got, _ := srv.Get("kx"); got != "NULL" {
		t.Fatalf("sentinel = %q, want NULL", got)
	}
	if _, _, err := tc.Get(context.Background(), "kx", loaderOf(nil, &calls)); !errors.Is(err, ErrNullCached) {
		t.Fatalf("second: want ErrNullCached, got %v", err)
	}
	if calls.Load() != 1 {
		t.Fatalf("loader calls = %d, want 1 (sentinel must absorb the second read)", calls.Load())
	}
}

// TestTiered_PreLoadGate 验证 PreLoad 在缓存未命中后、回源前生效。
func TestTiered_PreLoadGate(t *testing.T) {
	srv := miniredis.RunT(t)
	tc := newTiered(t, srv, freecache.NewCache(256*1024))
	gateErr := errors.New("bloom says absent")
	tc.PreLoad = func(context.Context) error { return gateErr }

	var calls atomic.Int32
	if _, _, err := tc.Get(context.Background(), "kg", loaderOf(&payload{ID: 9}, &calls)); !errors.Is(err, gateErr) {
		t.Fatalf("want gate error, got %v", err)
	}
	if calls.Load() != 0 {
		t.Fatal("loader must not run when PreLoad rejects")
	}

	// 命中缓存时 PreLoad 不应被调用
	srv.Set("kg", `{"id":9,"name":"x"}`)
	if _, lvl, err := tc.Get(context.Background(), "kg", loaderOf(nil, &calls)); err != nil || lvl != HitL2 {
		t.Fatalf("cached read should bypass PreLoad: lvl=%v err=%v", lvl, err)
	}
}

// TestTiered_LoaderErrorWritesNothing 验证加载失败不产生任何缓存写入。
func TestTiered_LoaderErrorWritesNothing(t *testing.T) {
	srv := miniredis.RunT(t)
	tc := newTiered(t, srv, freecache.NewCache(256*1024))

	boom := errors.New("db down")
	_, _, err := tc.Get(context.Background(), "ke", func(context.Context) (*payload, bool, error) {
		return nil, false, boom
	})
	if !errors.Is(err, boom) {
		t.Fatalf("want loader error, got %v", err)
	}
	if srv.Exists("ke") {
		t.Fatal("failed load must not write L2")
	}
}

// TestTiered_CorruptL2FallsThrough 验证 L2 解码失败按 miss 回源重建。
func TestTiered_CorruptL2FallsThrough(t *testing.T) {
	srv := miniredis.RunT(t)
	tc := newTiered(t, srv, freecache.NewCache(256*1024))
	srv.Set("kc", "{corrupt")

	var calls atomic.Int32
	v, lvl, err := tc.Get(context.Background(), "kc", loaderOf(&payload{ID: 5, Name: "ok"}, &calls))
	if err != nil || lvl != HitLoad || v.ID != 5 {
		t.Fatalf("corrupt L2 should fall through to loader: v=%+v lvl=%v err=%v", v, lvl, err)
	}
}

// TestTiered_WithLock 验证带锁路径正常返回（锁行为本身由 cacheutil 的测试覆盖）。
func TestTiered_WithLock(t *testing.T) {
	srv := miniredis.RunT(t)
	tc := newTiered(t, srv, freecache.NewCache(256*1024))
	tc.LockKey = "lock:kl"
	tc.LockRetry = 10 * time.Millisecond

	var calls atomic.Int32
	v, lvl, err := tc.Get(context.Background(), "kl", loaderOf(&payload{ID: 7}, &calls))
	if err != nil || lvl != HitLoad || v.ID != 7 {
		t.Fatalf("locked load: v=%+v lvl=%v err=%v", v, lvl, err)
	}
	// 回填后再次读取走 L1
	if _, lvl, _ := tc.Get(context.Background(), "kl", loaderOf(nil, &calls)); lvl != HitL1 {
		t.Fatalf("post-load level = %v, want HitL1", lvl)
	}
}

// TestVersions_LocalCacheAndDrop 验证版本号本地短缓存与写侧作废。
func TestVersions_LocalCacheAndDrop(t *testing.T) {
	srv := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: srv.Addr()})
	defer rdb.Close()

	v := &Versions{Redis: rdb, Local: freecache.NewCache(256 * 1024), LocalTTLSeconds: 30, LocalPrefix: "v:", Default: 1}

	if got := v.Get(context.Background(), "ver:k"); got != 1 {
		t.Fatalf("missing key = %d, want default 1", got)
	}
	srv.Set("ver:k", "5")
	v.Drop("ver:k") // 清掉刚缓存的默认值
	if got := v.Get(context.Background(), "ver:k"); got != 5 {
		t.Fatalf("after set = %d, want 5", got)
	}
	// Redis 变更但本地未失效：短窗口内仍读旧值
	srv.Set("ver:k", "6")
	if got := v.Get(context.Background(), "ver:k"); got != 5 {
		t.Fatalf("local cache window = %d, want 5", got)
	}
	// 写侧 Drop 后立即可见
	v.Drop("ver:k")
	if got := v.Get(context.Background(), "ver:k"); got != 6 {
		t.Fatalf("after drop = %d, want 6", got)
	}
}

// TestVersions_Disabled 验证 Local 为 nil 或 TTL<=0 时直读 Redis。
func TestVersions_Disabled(t *testing.T) {
	srv := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: srv.Addr()})
	defer rdb.Close()

	v := &Versions{Redis: rdb, Default: 1}
	srv.Set("ver:k", "3")
	if got := v.Get(context.Background(), "ver:k"); got != 3 {
		t.Fatalf("got %d, want 3", got)
	}
	srv.Set("ver:k", "4")
	if got := v.Get(context.Background(), "ver:k"); got != 4 {
		t.Fatalf("no local cache means always fresh: got %d, want 4", got)
	}
}
