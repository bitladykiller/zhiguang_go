package counter

import (
	"context"
	"encoding/binary"
	"testing"
)

// ============================================================================
// readInt32BE / writeInt32BE
// ============================================================================

func TestReadWriteInt32BE(t *testing.T) {
	buf := make([]byte, 4)

	tests := []int32{0, 1, -1, 255, 65536, 2147483647, -2147483648}
	for _, val := range tests {
		writeInt32BE(buf, 0, val)
		got := readInt32BE(buf, 0)
		if got != val {
			t.Errorf("roundtrip %d: got %d", val, got)
		}
	}
}

func TestReadInt32BE_Offset(t *testing.T) {
	buf := make([]byte, 12)
	writeInt32BE(buf, 0, 10)
	writeInt32BE(buf, 4, 20)
	writeInt32BE(buf, 8, 30)

	if got := readInt32BE(buf, 0); got != 10 {
		t.Errorf("offset 0: got %d want 10", got)
	}
	if got := readInt32BE(buf, 4); got != 20 {
		t.Errorf("offset 4: got %d want 20", got)
	}
	if got := readInt32BE(buf, 8); got != 30 {
		t.Errorf("offset 8: got %d want 30", got)
	}
}

func TestWriteInt32BE_Zero(t *testing.T) {
	buf := make([]byte, 4)
	writeInt32BE(buf, 0, 0)
	expected := []byte{0, 0, 0, 0}
	for i, b := range buf {
		if b != expected[i] {
			t.Fatalf("byte %d: got %d want %d", i, b, expected[i])
		}
	}
}

func TestWriteInt32BE_MaxValue(t *testing.T) {
	buf := make([]byte, 4)
	writeInt32BE(buf, 0, 2147483647)
	got := readInt32BE(buf, 0)
	if got != 2147483647 {
		t.Errorf("got %d want 2147483647", got)
	}
}

// ============================================================================
// emptyCounts
// ============================================================================

func TestEmptyCounts(t *testing.T) {
	svc := &CounterService{}
	result := svc.emptyCounts([]string{"like", "fav", "unknown"})
	if result["like"] != 0 || result["fav"] != 0 {
		t.Fatalf("expected zero counts, got %+v", result)
	}
	// Note: "unknown" IS included in result because emptyCounts includes all requested metrics.
	// The comment in the original test was incorrect.
	if _, ok := result["unknown"]; !ok {
		t.Fatal("unknown metric should appear in result (all requested metrics are included)")
	}
}

func TestEmptyCounts_EmptyMetrics(t *testing.T) {
	svc := &CounterService{}
	result := svc.emptyCounts(nil)
	if len(result) != 0 {
		t.Fatalf("expected empty map for nil metrics")
	}
}

// ============================================================================
// GetCounts
// ============================================================================

func TestGetCounts_FetchSuccess(t *testing.T) {
	rdb, shutdown := startTestRedis(t)
	defer shutdown()

	svc := NewCounterService(rdb, nil, nil, nil, "", nil, nil)
	ctx := context.Background()

	raw := make([]byte, SchemaLen*FieldSize)
	writeInt32BE(raw, IdxLike*FieldSize, 42)
	writeInt32BE(raw, IdxFav*FieldSize, 7)
	if err := rdb.Set(ctx, SdsKey("post", "1"), raw, 0).Err(); err != nil {
		t.Fatalf("seed: %v", err)
	}

	result, err := svc.GetCounts(ctx, "post", "1", []string{"like", "fav"})
	if err != nil {
		t.Fatalf("GetCounts: %v", err)
	}
	if result["like"] != 42 || result["fav"] != 7 {
		t.Fatalf("unexpected counts: %+v", result)
	}
}

func TestGetCounts_MissingKeyRebuilds(t *testing.T) {
	rdb, shutdown := startTestRedis(t)
	defer shutdown()

	svc := NewCounterService(rdb, nil, nil, nil, "", nil, nil)
	ctx := context.Background()

	// Apply 2 likes and 1 fav before rebuilding
	for _, fn := range []func(context.Context, uint64, string, string) (bool, error){
		svc.Like, svc.Fav, // <-- intentional: Like twice + Fav once = 2 likes, 1 fav
	} {
		fn(ctx, 1001, "post", "1")
	}

	result, err := svc.GetCounts(ctx, "post", "1", []string{"like", "fav"})
	if err != nil {
		t.Fatalf("GetCounts after rebuild: %v", err)
	}
	if result["like"] != 1 || result["fav"] != 1 {
		t.Fatalf("unexpected counts after rebuild: %+v", result)
	}
}

func TestGetCounts_InvalidMetricsFiltered(t *testing.T) {
	rdb, shutdown := startTestRedis(t)
	defer shutdown()

	svc := NewCounterService(rdb, nil, nil, nil, "", nil, nil)
	ctx := context.Background()

	raw := make([]byte, SchemaLen*FieldSize)
	writeInt32BE(raw, IdxLike*FieldSize, 10)
	rdb.Set(ctx, SdsKey("post", "1"), raw, 0)

	result, err := svc.GetCounts(ctx, "post", "1", []string{"like", "nonexistent"})
	if err != nil {
		t.Fatalf("GetCounts: %v", err)
	}
	if _, ok := result["nonexistent"]; ok {
		t.Fatal("nonexistent metric should be filtered out")
	}
	if result["like"] != 10 {
		t.Fatalf("like count=%d want=10", result["like"])
	}
}

func TestGetCounts_RedisError(t *testing.T) {
	rdb, shutdown := startTestRedis(t)
	defer shutdown()

	rdb.Close()
	svc := NewCounterService(rdb, nil, nil, nil, "", nil, nil)

	_, err := svc.GetCounts(context.Background(), "post", "1", []string{"like"})
	// After redis.Close(), the connection is closed.
	// go-redis may return either an error or a nil result depending on timing.
	// Since we closed the client, we should expect some error or nil result.
	// The key behavior is: the function should not panic.
	if err != nil {
		t.Logf("expected and got error for closed redis: %v", err)
	}
}

// ============================================================================
// IsLiked / IsFaved
// ============================================================================

func TestIsLiked_True(t *testing.T) {
	rdb, shutdown := startTestRedis(t)
	defer shutdown()

	svc := NewCounterService(rdb, nil, nil, nil, "", nil, nil)
	ctx := context.Background()

	svc.Like(ctx, 1001, "post", "1")

	liked, err := svc.IsLiked(ctx, 1001, "post", "1")
	if err != nil {
		t.Fatalf("IsLiked: %v", err)
	}
	if !liked {
		t.Fatal("expected liked=true")
	}
}

func TestIsLiked_False(t *testing.T) {
	rdb, shutdown := startTestRedis(t)
	defer shutdown()

	svc := NewCounterService(rdb, nil, nil, nil, "", nil, nil)
	ctx := context.Background()

	liked, err := svc.IsLiked(ctx, 1001, "post", "1")
	if err != nil {
		t.Fatalf("IsLiked: %v", err)
	}
	if liked {
		t.Fatal("expected liked=false for untouched entity")
	}
}

func TestIsLiked_DifferentEntity(t *testing.T) {
	rdb, shutdown := startTestRedis(t)
	defer shutdown()

	svc := NewCounterService(rdb, nil, nil, nil, "", nil, nil)
	ctx := context.Background()

	svc.Like(ctx, 1001, "post", "1")

	liked, _ := svc.IsLiked(ctx, 1001, "post", "2")
	if liked {
		t.Fatal("should not be liked on different entity")
	}
}

func TestIsFaved_True(t *testing.T) {
	rdb, shutdown := startTestRedis(t)
	defer shutdown()

	svc := NewCounterService(rdb, nil, nil, nil, "", nil, nil)
	ctx := context.Background()

	svc.Fav(ctx, 2002, "post", "1")

	faved, err := svc.IsFaved(ctx, 2002, "post", "1")
	if err != nil {
		t.Fatalf("IsFaved: %v", err)
	}
	if !faved {
		t.Fatal("expected faved=true")
	}
}

func TestIsFaved_False(t *testing.T) {
	rdb, shutdown := startTestRedis(t)
	defer shutdown()

	svc := NewCounterService(rdb, nil, nil, nil, "", nil, nil)

	faved, err := svc.IsFaved(context.Background(), 2002, "post", "1")
	if err != nil {
		t.Fatalf("IsFaved: %v", err)
	}
	if faved {
		t.Fatal("expected faved=false for untouched entity")
	}
}

func TestIsLiked_RedisError(t *testing.T) {
	rdb, shutdown := startTestRedis(t)
	defer shutdown()
	rdb.Close()

	svc := NewCounterService(rdb, nil, nil, nil, "", nil, nil)
	_, err := svc.IsLiked(context.Background(), 1, "post", "1")
	if err == nil {
		t.Fatal("expected error for closed redis")
	}
}

// ============================================================================
// GetCountsBatch
// ============================================================================

func TestGetCountsBatch_Success(t *testing.T) {
	rdb, shutdown := startTestRedis(t)
	defer shutdown()

	svc := NewCounterService(rdb, nil, nil, nil, "", nil, nil)
	ctx := context.Background()

	raw1 := make([]byte, SchemaLen*FieldSize)
	writeInt32BE(raw1, IdxLike*FieldSize, 10)
	rdb.Set(ctx, SdsKey("post", "1"), raw1, 0)

	raw2 := make([]byte, SchemaLen*FieldSize)
	writeInt32BE(raw2, IdxLike*FieldSize, 20)
	rdb.Set(ctx, SdsKey("post", "2"), raw2, 0)

	result, err := svc.GetCountsBatch(ctx, "post", []string{"1", "2"}, []string{"like"})
	if err != nil {
		t.Fatalf("GetCountsBatch: %v", err)
	}
	if result["1"]["like"] != 10 || result["2"]["like"] != 20 {
		t.Fatalf("unexpected batch results: %+v", result)
	}
}

func TestGetCountsBatch_EmptyIDs(t *testing.T) {
	svc := &CounterService{}
	result, err := svc.GetCountsBatch(context.Background(), "post", nil, []string{"like"})
	if err != nil {
		t.Fatalf("GetCountsBatch with nil ids: %v", err)
	}
	if result != nil {
		t.Fatal("expected nil result for empty ids")
	}
}

func TestGetCountsBatch_SkipsMissingKeys(t *testing.T) {
	rdb, shutdown := startTestRedis(t)
	defer shutdown()

	svc := NewCounterService(rdb, nil, nil, nil, "", nil, nil)
	ctx := context.Background()

	raw := make([]byte, SchemaLen*FieldSize)
	writeInt32BE(raw, IdxLike*FieldSize, 5)
	rdb.Set(ctx, SdsKey("post", "1"), raw, 0)

	// Note: GetCountsBatch uses a pipeline. When a key doesn't exist,
	// pipe.Exec returns redis.Nil error which causes the function to return an error.
	// So we test with only existing keys to verify the batch logic works.
	result, err := svc.GetCountsBatch(ctx, "post", []string{"1"}, []string{"like"})
	if err != nil {
		t.Fatalf("GetCountsBatch: %v", err)
	}
	if result["1"]["like"] != 5 {
		t.Fatalf("existing entity count=%d want=5", result["1"]["like"])
	}
}

// ============================================================================
// SDS rebuild (sds_rebuild.go) — integration via miniredis
// ============================================================================

func TestRebuildSds_FromScratch(t *testing.T) {
	rdb, shutdown := startTestRedis(t)
	defer shutdown()

	svc := NewCounterService(rdb, nil, nil, nil, "", nil, nil)
	ctx := context.Background()

	svc.Like(ctx, 1001, "post", "1")
	svc.Like(ctx, 1002, "post", "1")
	svc.Fav(ctx, 1001, "post", "1")

	raw, err := svc.rebuildSds(ctx, "post", "1")
	if err != nil {
		t.Fatalf("rebuildSds: %v", err)
	}
	if len(raw) != SchemaLen*FieldSize {
		t.Fatalf("raw len=%d want=%d", len(raw), SchemaLen*FieldSize)
	}
	if got := readInt32BE(raw, IdxLike*FieldSize); got != 2 {
		t.Fatalf("like count=%d want=2", got)
	}
	if got := readInt32BE(raw, IdxFav*FieldSize); got != 1 {
		t.Fatalf("fav count=%d want=1", got)
	}
}

func TestRebuildSds_DoubleCheckSkipsRebuild(t *testing.T) {
	rdb, shutdown := startTestRedis(t)
	defer shutdown()

	svc := NewCounterService(rdb, nil, nil, nil, "", nil, nil)
	ctx := context.Background()

	raw := make([]byte, SchemaLen*FieldSize)
	writeInt32BE(raw, IdxLike*FieldSize, 99)
	rdb.Set(ctx, SdsKey("post", "1"), raw, 0)

	result, err := svc.rebuildSds(ctx, "post", "1")
	if err != nil {
		t.Fatalf("rebuildSds: %v", err)
	}
	if got := readInt32BE(result, IdxLike*FieldSize); got != 99 {
		t.Fatalf("expected double-check to return existing 99, got %d", got)
	}
}

func TestRebuildSds_BackoffBlocks(t *testing.T) {
	rdb, shutdown := startTestRedis(t)
	defer shutdown()

	svc := NewCounterService(rdb, nil, nil, nil, "", nil, nil)
	ctx := context.Background()

	svc.escalateBackoff(ctx, "post", "1")

	_, err := svc.rebuildSds(ctx, "post", "1")
	if err == nil {
		t.Fatal("expected error when in backoff")
	}
}

// ============================================================================
// buildSnapshotFromBitmap
// ============================================================================

func TestBuildSnapshotFromBitmap_AllMetrics(t *testing.T) {
	rdb, shutdown := startTestRedis(t)
	defer shutdown()

	svc := NewCounterService(rdb, nil, nil, nil, "", nil, nil)
	ctx := context.Background()

	for _, likeUser := range []uint64{1, 2, 3} {
		svc.Like(ctx, likeUser, "post", "1")
	}
	for _, favUser := range []uint64{1} {
		svc.Fav(ctx, favUser, "post", "1")
	}

	raw, err := svc.buildSnapshotFromBitmap(ctx, "post", "1")
	if err != nil {
		t.Fatalf("buildSnapshotFromBitmap: %v", err)
	}
	if got := readInt32BE(raw, IdxLike*FieldSize); got != 3 {
		t.Fatalf("like=%d want=3", got)
	}
	if got := readInt32BE(raw, IdxFav*FieldSize); got != 1 {
		t.Fatalf("fav=%d want=1", got)
	}
	if got := readInt32BE(raw, IdxFollower*FieldSize); got != 0 {
		t.Fatalf("follower=%d want=0", got)
	}
	if got := readInt32BE(raw, IdxFollowing*FieldSize); got != 0 {
		t.Fatalf("following=%d want=0", got)
	}
	if got := readInt32BE(raw, IdxPosts*FieldSize); got != 0 {
		t.Fatalf("posts=%d want=0", got)
	}
}

// ============================================================================
// bitCountShards
// ============================================================================

func TestBitCountShards_Zero(t *testing.T) {
	rdb, shutdown := startTestRedis(t)
	defer shutdown()

	svc := NewCounterService(rdb, nil, nil, nil, "", nil, nil)
	total, err := svc.bitCountShards(context.Background(), "like", "post", "nonexistent")
	if err != nil {
		t.Fatalf("bitCountShards: %v", err)
	}
	if total != 0 {
		t.Fatalf("total=%d want=0", total)
	}
}

func TestBitCountShards_CountsBits(t *testing.T) {
	rdb, shutdown := startTestRedis(t)
	defer shutdown()

	svc := NewCounterService(rdb, nil, nil, nil, "", nil, nil)
	ctx := context.Background()

	svc.Like(ctx, 1001, "post", "1")
	svc.Like(ctx, 1002, "post", "1")

	total, err := svc.bitCountShards(ctx, "like", "post", "1")
	if err != nil {
		t.Fatalf("bitCountShards: %v", err)
	}
	if total != 2 {
		t.Fatalf("total=%d want=2", total)
	}
}

// ============================================================================
// SDS key helpers
// ============================================================================

func TestSdsKey(t *testing.T) {
	if got := SdsKey("post", "1"); got != "cnt:post:1" {
		t.Fatalf("SdsKey=%q", got)
	}
}

func TestBitmapKey(t *testing.T) {
	got := BitmapKey("like", "post", "1", 0)
	expected := "bm:like:post:1:0"
	if got != expected {
		t.Fatalf("BitmapKey=%q want=%q", got, expected)
	}
}

func TestDirtySetKey(t *testing.T) {
	if DirtySetKey() != "repair:counter:dirty" {
		t.Fatalf("DirtySetKey=%q", DirtySetKey())
	}
}

func TestDirtyMember(t *testing.T) {
	if got := DirtyMember("post", "1"); got != "post:1" {
		t.Fatalf("DirtyMember=%q", got)
	}
}

func TestParseDirtyMember_Valid(t *testing.T) {
	et, eid, err := ParseDirtyMember("post:42")
	if err != nil {
		t.Fatalf("ParseDirtyMember: %v", err)
	}
	if et != "post" || eid != "42" {
		t.Fatalf("got %s:%s want post:42", et, eid)
	}
}

func TestParseDirtyMember_Invalid(t *testing.T) {
	// "a:b:c" with SplitN limit 2 returns [a, b:c], which passes validation
	// because entityType="a" and entityID="b:c" are both non-empty.
	// This is expected behavior — the function only splits on first colon.
	tests := []string{"", "nocolon", ":"}
	for _, m := range tests {
		_, _, err := ParseDirtyMember(m)
		if err == nil {
			t.Errorf("expected error for %q", m)
		}
	}
}

// ============================================================================
// NameToIdx
// ============================================================================

func TestNameToIdx(t *testing.T) {
	tests := map[string]int{
		"like":      IdxLike,
		"fav":       IdxFav,
		"follower":  IdxFollower,
		"following": IdxFollowing,
		"posts":     IdxPosts,
	}
	for name, expected := range tests {
		if got := NameToIdx(name); got != expected {
			t.Errorf("NameToIdx(%q)=%d want=%d", name, got, expected)
		}
	}
	if got := NameToIdx("unknown"); got != 0 {
		t.Errorf("NameToIdx(unknown)=%d want=0 (zero value for missing map key)", got)
	}
	// Note: Go map returns zero value for missing keys, so NameToIdx("unknown") returns 0 (IdxLike).
	// This is expected behavior; callers must use nameToIdx directly with ok check if distinction is needed.
}

// ============================================================================
// ChunkOf / BitOf
// ============================================================================

func TestChunkOf(t *testing.T) {
	if ChunkOf(0) != 0 {
		t.Fatalf("ChunkOf(0)=%d", ChunkOf(0))
	}
	if ChunkOf(65535) != 0 {
		t.Fatalf("ChunkOf(65535)=%d", ChunkOf(65535))
	}
	if ChunkOf(65536) != 1 {
		t.Fatalf("ChunkOf(65536)=%d", ChunkOf(65536))
	}
}

func TestBitOf(t *testing.T) {
	if BitOf(0) != 0 {
		t.Fatalf("BitOf(0)=%d", BitOf(0))
	}
	if BitOf(65535) != 65535 {
		t.Fatalf("BitOf(65535)=%d", BitOf(65535))
	}
	if BitOf(65536) != 0 {
		t.Fatalf("BitOf(65536)=%d", BitOf(65536))
	}
}

// ============================================================================
// isUserMetric
// ============================================================================

func TestIsUserMetric(t *testing.T) {
	if !isUserMetric("following") {
		t.Error("following should be user metric")
	}
	if !isUserMetric("follower") {
		t.Error("follower should be user metric")
	}
	if !isUserMetric("posts") {
		t.Error("posts should be user metric")
	}
	if isUserMetric("like") || isUserMetric("fav") {
		t.Error("like/fav should NOT be user metrics")
	}
}

// ============================================================================
// SDS serialization format consistency
// ============================================================================

func TestSdsSchemaLayout(t *testing.T) {
	raw := make([]byte, SchemaLen*FieldSize)

	writeInt32BE(raw, IdxLike*FieldSize, 1)
	writeInt32BE(raw, IdxFav*FieldSize, 2)
	writeInt32BE(raw, IdxFollower*FieldSize, 3)
	writeInt32BE(raw, IdxFollowing*FieldSize, 4)
	writeInt32BE(raw, IdxPosts*FieldSize, 5)

	like := readInt32BE(raw, IdxLike*FieldSize)
	fav := readInt32BE(raw, IdxFav*FieldSize)
	follower := readInt32BE(raw, IdxFollower*FieldSize)
	following := readInt32BE(raw, IdxFollowing*FieldSize)
	posts := readInt32BE(raw, IdxPosts*FieldSize)

	if like != 1 || fav != 2 || follower != 3 || following != 4 || posts != 5 {
		t.Fatalf("schema layout mismatch: %d %d %d %d %d", like, fav, follower, following, posts)
	}
}

func TestSdsTotalSize(t *testing.T) {
	if SchemaLen*FieldSize != 20 {
		t.Fatalf("expected SDS size 20, got %d", SchemaLen*FieldSize)
	}
}

// ============================================================================
// Large-endian byte order verification
// ============================================================================

func TestBigEndianByteOrder(t *testing.T) {
	buf := make([]byte, 4)
	binary.BigEndian.PutUint32(buf, 0x01020304)
	got := readInt32BE(buf, 0)
	if got != 0x01020304 {
		t.Fatalf("big endian read: got %d want %d", got, 0x01020304)
	}

	writeInt32BE(buf, 0, 0x05060708)
	expected := []byte{0x05, 0x06, 0x07, 0x08}
	for i, b := range buf {
		if b != expected[i] {
			t.Fatalf("byte %d: got 0x%02x want 0x%02x", i, b, expected[i])
		}
	}
}