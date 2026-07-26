package knowpost

import (
	"bytes"
	"sync"
	"testing"

	"github.com/coocood/freecache"
)

func newPrefixCachePair(t testing.TB) (*PrefixCache, *PrefixCache) {
	t.Helper()
	shared := freecache.NewCache(1024 * 1024)
	return &PrefixCache{Cache: shared, Prefix: "d:"}, &PrefixCache{Cache: shared, Prefix: "f:"}
}

// TestPrefixCache_PrefixIsolatesNamespaces 验证同一底层缓存下不同前缀互不串扰。
func TestPrefixCache_PrefixIsolatesNamespaces(t *testing.T) {
	detail, feed := newPrefixCachePair(t)

	key := []byte("knowpost:1")
	if err := detail.Set(key, []byte("detail-value"), 60); err != nil {
		t.Fatalf("detail.Set: %v", err)
	}
	if err := feed.Set(key, []byte("feed-value"), 60); err != nil {
		t.Fatalf("feed.Set: %v", err)
	}

	got, err := detail.Get(key)
	if err != nil {
		t.Fatalf("detail.Get: %v", err)
	}
	if !bytes.Equal(got, []byte("detail-value")) {
		t.Errorf("detail.Get = %q, want %q", got, "detail-value")
	}

	got, err = feed.Get(key)
	if err != nil {
		t.Fatalf("feed.Get: %v", err)
	}
	if !bytes.Equal(got, []byte("feed-value")) {
		t.Errorf("feed.Get = %q, want %q", got, "feed-value")
	}

	// 删除一个命名空间不应影响另一个。
	if !detail.Del(key) {
		t.Error("detail.Del should report success")
	}
	if _, err := detail.Get(key); err == nil {
		t.Error("detail entry should be gone")
	}
	if _, err := feed.Get(key); err != nil {
		t.Errorf("feed entry must survive detail.Del: %v", err)
	}
}

// TestPrefixCache_ConcurrentAccess 验证并发读写下 key 拼接互不污染。
//
// 这是拼接缓冲区实现的核心不变量：prefixed 返回的切片必须为本次调用独有。
// 此前的实现从一个包级 sync.Pool 取缓冲区却从不归还，一旦有人「修好」归还逻辑
// 而未同步收紧生命周期，就会出现多个 goroutine 共享同一底层数组、
// key 相互覆盖的数据竞争——本用例配合 -race 可以捕获该退化。
func TestPrefixCache_ConcurrentAccess(t *testing.T) {
	detail, feed := newPrefixCachePair(t)

	keys := [][]byte{
		[]byte("knowpost:detail:1"),
		[]byte("knowpost:detail:22"),
		[]byte("knowpost:detail:333"),
		[]byte("a"),
		[]byte("a-considerably-longer-cache-key-than-the-others-to-force-regrowth"),
	}
	for i, k := range keys {
		if err := detail.Set(k, []byte{byte(i)}, 60); err != nil {
			t.Fatalf("seed detail: %v", err)
		}
	}

	var wg sync.WaitGroup
	for g := 0; g < 8; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 200; i++ {
				k := keys[i%len(keys)]
				if got, err := detail.Get(k); err == nil {
					if len(got) != 1 || got[0] != byte(i%len(keys)) {
						t.Errorf("key %q returned %v", k, got)
						return
					}
				}
				_ = feed.Set(k, []byte("f"), 60)
			}
		}()
	}
	wg.Wait()
}

// BenchmarkPrefixCache_Get 度量 L1 读取路径的分配次数。
//
// 用于佐证移除「只 Get 不 Put」的 sync.Pool 是收益为正的改动：
// 原实现每次调用都要做一次 Pool 查找，随后仍由 Pool.New 分配新切片，
// 分配次数不降反升。
func BenchmarkPrefixCache_Get(b *testing.B) {
	detail, _ := newPrefixCachePair(b)
	key := []byte("knowpost:detail:1234567890:v1:ver3")
	if err := detail.Set(key, []byte("payload"), 600); err != nil {
		b.Fatalf("seed: %v", err)
	}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := detail.Get(key); err != nil {
			b.Fatalf("Get: %v", err)
		}
	}
}
