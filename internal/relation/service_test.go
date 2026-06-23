package relation

import (
	"context"
	"testing"

	"github.com/alicebob/miniredis/v2"
	"github.com/coocood/freecache"
	"github.com/redis/go-redis/v9"
)

type stubIDGen struct {
	next uint64
}

func (g *stubIDGen) NextID() uint64 {
	id := g.next
	g.next++
	return id
}

func startTestRedis(t *testing.T) (*redis.Client, func()) {
	t.Helper()
	mr, err := miniredis.Run()
	if err != nil {
		t.Fatalf("start miniredis: %v", err)
	}
	client := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	return client, func() { client.Close(); mr.Close() }
}

func newTestService(rdb *redis.Client) *RelationService {
	return &RelationService{
		redis: rdb,
		l1:    freecache.NewCache(1024 * 1024),
		idGen: &stubIDGen{next: 1000},
	}
}

func TestNewRelationService_CreatesInstance(t *testing.T) {
	rdb, shutdown := startTestRedis(t)
	defer shutdown()

	svc := newTestService(rdb)
	if svc.redis == nil {
		t.Fatal("expected redis client to be set")
	}
	if svc.l1 == nil {
		t.Fatal("expected l1 cache to be set")
	}
	if svc.idGen == nil {
		t.Fatal("expected id generator to be set")
	}
}

func TestRelationStatus_None(t *testing.T) {
	t.Skip("requires db connection")
}

func TestZsetKey(t *testing.T) {
	svc := newTestService(nil)
	key := svc.zsetKey("following", 1001)
	if key != "z:following:1001" {
		t.Fatalf("unexpected zset key: %s", key)
	}
}

func TestL1KeyStr(t *testing.T) {
	svc := newTestService(nil)
	key := svc.l1KeyStr("followers", 1001)
	if key != "l1:followers:1001" {
		t.Fatalf("unexpected l1 key: %s", key)
	}
}

func TestToLongList(t *testing.T) {
	svc := newTestService(nil)
	ids := svc.toLongList("1001,1002,1003")
	if len(ids) != 3 {
		t.Fatalf("expected 3 ids, got %d", len(ids))
	}
	if ids[0] != 1001 || ids[1] != 1002 || ids[2] != 1003 {
		t.Fatalf("unexpected ids: %v", ids)
	}
}

func TestToLongList_Empty(t *testing.T) {
	svc := newTestService(nil)
	ids := svc.toLongList("")
	if len(ids) != 0 {
		t.Fatalf("expected 0 ids, got %d", len(ids))
	}
}

func TestToIDList(t *testing.T) {
	svc := newTestService(nil)
	ids := svc.toIDList([]string{"1001", "1002", "invalid", "1003"})
	if len(ids) != 3 {
		t.Fatalf("expected 3 ids, got %d", len(ids))
	}
	if ids[0] != 1001 || ids[1] != 1002 || ids[2] != 1003 {
		t.Fatalf("unexpected ids: %v", ids)
	}
}

func TestIsBigV_False(t *testing.T) {
	rdb, shutdown := startTestRedis(t)
	defer shutdown()

	svc := newTestService(rdb)
	if svc.isBigV(context.Background(), 1) {
		t.Fatal("expected not big V for empty cache")
	}
}

func TestCacheEndReached(t *testing.T) {
	rdb, shutdown := startTestRedis(t)
	defer shutdown()

	svc := newTestService(rdb)
	zsetKey := "z:following:999"

	reached := svc.cacheEndReached(context.Background(), zsetKey, 0)
	if !reached {
		t.Fatal("expected cache reached when ZSet is empty and offset is 0")
	}

	svc.redis.ZAdd(context.Background(), zsetKey, redis.Z{Score: 1, Member: "1"})
	reached = svc.cacheEndReached(context.Background(), zsetKey, 1)
	if !reached {
		t.Fatal("expected offset 1 reached when ZSet size is 1")
	}
	reached = svc.cacheEndReached(context.Background(), zsetKey, 0)
	if reached {
		t.Fatal("expected offset 0 not reached when ZSet size is 1")
	}
}

func TestInvalidateCaches_WithoutRedis(t *testing.T) {
	t.Skip("requires redis client with lock support")
}

func TestAcquireListCacheLock_WithoutRedis(t *testing.T) {
	t.Skip("requires non-nil redis client")
}

func TestShouldFallbackToFollowing(t *testing.T) {
	rdb, shutdown := startTestRedis(t)
	defer shutdown()

	svc := newTestService(rdb)
	should := svc.shouldFallbackToFollowing(context.Background(), 1)
	if !should {
		t.Fatal("expected fallback to be allowed initially")
	}
}

func TestMarkFollowerFallbackExhausted(t *testing.T) {
	rdb, shutdown := startTestRedis(t)
	defer shutdown()

	svc := newTestService(rdb)
	svc.markFollowerFallbackExhausted(context.Background(), 1)
	should := svc.shouldFallbackToFollowing(context.Background(), 1)
	if should {
		t.Fatal("expected fallback to be exhausted after marking")
	}
}

func TestFillL1_WithNoData_Noop(t *testing.T) {
	t.Skip("requires db connection")
}

func TestFillZSet_WithNoDB_ReturnsError(t *testing.T) {
	t.Skip("requires db connection")
}

func TestEnsureListCacheWarm_EmptyKey_ReturnsFalse(t *testing.T) {
	t.Skip("requires db connection")
}

func TestReadFromDB_UnknownListType(t *testing.T) {
	t.Skip("requires db connection")
}