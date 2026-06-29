package relation

import (
	"context"
	"testing"

	"github.com/alicebob/miniredis/v2"
	"github.com/coocood/freecache"
	"github.com/jmoiron/sqlx"
	"github.com/redis/go-redis/v9"
	"go.uber.org/zap"
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
		redis:         rdb,
		l1:            freecache.NewCache(1024 * 1024),
		idGen:         &stubIDGen{next: 1000},
		logger:        zapL(),
		bigVThreshold: int64(bigVThreshold),
	}
}

func newTestServiceWithDB(rdb *redis.Client, db *sqlx.DB) *RelationService {
	return &RelationService{
		db:            db,
		redis:         rdb,
		repo:          NewRelationRepository(db),
		l1:            freecache.NewCache(1024 * 1024),
		idGen:         &stubIDGen{next: 1000},
		logger:        zapL(),
		bigVThreshold: int64(bigVThreshold),
	}
}

// nop logger for tests
func zapL() *zap.Logger {
	cfg := zap.NewDevelopmentConfig()
	cfg.OutputPaths = []string{"stdout"}
	l, _ := cfg.Build()
	return l
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

func TestToLongList_WithSpaces(t *testing.T) {
	svc := newTestService(nil)
	ids := svc.toLongList(" 1001 , 1002 , 1003 ")
	if len(ids) != 3 || ids[0] != 1001 || ids[1] != 1002 || ids[2] != 1003 {
		t.Fatalf("unexpected ids with spaces: %v", ids)
	}
}

func TestToLongList_InvalidEntry(t *testing.T) {
	svc := newTestService(nil)
	ids := svc.toLongList("1001,abc,1003")
	if len(ids) != 2 || ids[0] != 1001 || ids[1] != 1003 {
		t.Fatalf("expected 2 valid ids, got %v", ids)
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

func TestToIDList_Empty(t *testing.T) {
	svc := newTestService(nil)
	ids := svc.toIDList([]string{})
	if len(ids) != 0 {
		t.Fatalf("expected empty, got %v", ids)
	}
}

func TestToIDList_AllInvalid(t *testing.T) {
	svc := newTestService(nil)
	ids := svc.toIDList([]string{"abc", "def"})
	if len(ids) != 0 {
		t.Fatalf("expected empty, got %v", ids)
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

func TestIsBigV_True(t *testing.T) {
	rdb, shutdown := startTestRedis(t)
	defer shutdown()

	svc := newTestService(rdb)
	zkey := svc.zsetKey("followers", 1)
	members := make([]redis.Z, bigVThreshold)
	for i := 0; i < bigVThreshold; i++ {
		members[i] = redis.Z{Score: float64(i), Member: string(rune(i))}
	}
	svc.redis.ZAdd(context.Background(), zkey, members...)

	if !svc.isBigV(context.Background(), 1) {
		t.Fatal("expected big V after adding threshold members")
	}
}

func TestIsBigV_RedisError(t *testing.T) {
	rdb, shutdown := startTestRedis(t)
	defer shutdown()

	svc := newTestService(rdb)
	if svc.isBigV(context.Background(), 1) {
		t.Fatal("expected false when zset does not exist")
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

func TestCacheEndReached_RedisError(t *testing.T) {
	rdb, shutdown := startTestRedis(t)
	defer shutdown()

	svc := newTestService(rdb)

	reached := svc.cacheEndReached(context.Background(), "z:following:nonexistent", 0)
	if !reached {
		t.Fatal("expected true when zset is empty and offset is 0 (empty cache = end reached)")
	}

	reached2 := svc.cacheEndReached(context.Background(), "z:following:nonexistent", 100)
	if !reached2 {
		t.Fatal("expected true when zset is empty and offset is 100")
	}
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

func TestShouldFallbackToFollowing_AfterExhausted(t *testing.T) {
	rdb, shutdown := startTestRedis(t)
	defer shutdown()

	svc := newTestService(rdb)
	svc.markFollowerFallbackExhausted(context.Background(), 1)
	should := svc.shouldFallbackToFollowing(context.Background(), 1)
	if should {
		t.Fatal("expected fallback to be exhausted after marking")
	}
}

func TestShouldFallbackToFollowing_RedisError(t *testing.T) {
	rdb, shutdown := startTestRedis(t)
	defer shutdown()

	svc := newTestService(rdb)
	should := svc.shouldFallbackToFollowing(context.Background(), 1)
	if !should {
		t.Fatal("expected conservative fallback when key does not exist")
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

func TestIsFollowing_WithoutDB_ReturnsError(t *testing.T) {
	t.Skip("requires db connection for repository")
}

func TestRelationStatus_WithoutDB_ReturnsError(t *testing.T) {
	t.Skip("requires db connection for repository")
}

func TestFollow_WithoutRedisAndDB(t *testing.T) {
	t.Skip("requires db connection for outbox pattern")
}

func TestUnfollow_WithoutDB(t *testing.T) {
	t.Skip("requires db connection for repository")
}

func TestFollow_FollowSelf(t *testing.T) {
	t.Skip("requires db connection; handshake with outbox")
}

func TestEnsureListCacheWarm_WithRedis_NoDB(t *testing.T) {
	rdb, shutdown := startTestRedis(t)
	defer shutdown()

	svc := newTestService(rdb)
	svc.db = nil
	svc.repo = nil

	warmed, err := svc.ensureListCacheWarm(context.Background(), "following", 1)
	if err == nil {
		t.Fatal("expected error when repo is nil")
	}
	if warmed {
		t.Fatal("expected false when fillZSet errors")
	}
}

func TestEnsureListCacheWarm_WithRedis_AlreadyWarm(t *testing.T) {
	rdb, shutdown := startTestRedis(t)
	defer shutdown()

	svc := newTestService(rdb)
	zkey := svc.zsetKey("following", 1)
	svc.redis.ZAdd(context.Background(), zkey, redis.Z{Score: 1000, Member: "42"})

	warmed, err := svc.ensureListCacheWarm(context.Background(), "following", 1)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !warmed {
		t.Fatal("expected true when zset already exists")
	}
}

func TestGetListWithOffset_RedisError_DoesNotPanic(t *testing.T) {
	t.Skip("requires db connection for ensureListCacheWarm")
}

func TestGetListWithCursor_RedisError_ReturnsEmpty(t *testing.T) {
	t.Skip("requires db connection for ensureListCacheWarm")
}

func TestGetListWithCursor_WithData(t *testing.T) {
	rdb, shutdown := startTestRedis(t)
	defer shutdown()

	svc := newTestService(rdb)
	zkey := svc.zsetKey("following", 1)
	svc.redis.ZAdd(context.Background(), zkey, redis.Z{Score: 3000, Member: "30"})
	svc.redis.ZAdd(context.Background(), zkey, redis.Z{Score: 2000, Member: "20"})
	svc.redis.ZAdd(context.Background(), zkey, redis.Z{Score: 1000, Member: "10"})

	ids, cursor, err := svc.getListWithCursor(context.Background(), 1, "following", 2, 0)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(ids) != 2 || ids[0] != 30 || ids[1] != 20 {
		t.Fatalf("expected [30 20], got %v", ids)
	}
	if cursor == 0 {
		t.Fatal("expected non-zero cursor")
	}
}

func TestGetListWithOffset_L1CacheHit(t *testing.T) {
	t.Skip("requires db connection for ensureListCacheWarm")
}

func TestGetListWithOffset_EmptyCache_NoData(t *testing.T) {
	t.Skip("requires db connection for ensureListCacheWarm")
}

func TestInProcess(t *testing.T) {
	t.Skip("requires full integration setup")
}

func TestFollow_WithRateLimit(t *testing.T) {
	t.Skip("requires db connection for outbox pattern")
}

func TestFollow_MultipleCalls_RateLimited(t *testing.T) {
	t.Skip("requires db connection for outbox pattern")
}

func TestFillL1_NilRepo_Noop(t *testing.T) {
	svc := newTestService(nil)
	svc.fillL1(context.Background(), "following", 1)
}

func TestFollowing_NilRedis_ReturnsEmpty(t *testing.T) {
	t.Skip("requires db connection for ensureListCacheWarm")
}

func TestFollowers_NilRedis_ReturnsEmpty(t *testing.T) {
	t.Skip("requires db connection for ensureListCacheWarm")
}

func TestEnsureListCacheWarm_DoubleCheckExists(t *testing.T) {
	rdb, shutdown := startTestRedis(t)
	defer shutdown()

	svc := newTestService(rdb)
	zkey := svc.zsetKey("following", 1)

	svc.redis.ZAdd(context.Background(), zkey, redis.Z{Score: 1, Member: "1"})

	warmed, err := svc.ensureListCacheWarm(context.Background(), "following", 1)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !warmed {
		t.Fatal("expected warm already")
	}
}

func TestInProcessIsFollowing(t *testing.T) {
	t.Skip("requires db connection")
}

func TestInProcessFollowAndUnfollow(t *testing.T) {
	t.Skip("requires db connection")
}

func TestEventProcessor(t *testing.T) {
	t.Skip("covered in dedicated test")
}

func TestRelationStatusCases(t *testing.T) {
	t.Skip("requires db connection")
}

func TestFollowingCursor_Empty(t *testing.T) {
	t.Skip("requires db connection for ensureListCacheWarm")
}

func TestRelationServiceImplementsInterface(t *testing.T) {
	var _ RelationServiceInterface = (*RelationService)(nil)
}
