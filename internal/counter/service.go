package counter

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/redis/go-redis/v9"
	"go.uber.org/zap"

	"github.com/zhiguang/app/pkg/config"
	"github.com/zhiguang/app/pkg/redislock"
)

const defaultMaxChunk = uint64(128)

// CounterService 提供原子化的计数开关操作。
type CounterService struct {
	redis              *redis.Client
	producer           CounterEventPublisher
	rebuildLockOptions redislock.Options
	failureRecorder    CounterFailureRecorder
	failureTopic       string
	messageIDGenerator MessageIDGenerator
	logger             *zap.Logger
	publishTimeout     time.Duration
	backoffCfg         *config.BackoffConfig
	rebuildRateCfg     *config.RebuildRateConfig
	auditLog           AuditLogger
	// likersMaxChunk 是点赞者位图扫描的分片上限，决定可枚举的最大用户 ID。
	likersMaxChunk uint64
}

// AuditLogger 定义审计日志接口。
type AuditLogger interface {
	LogAction(ctx context.Context, action string, userID int64, resourceType, resourceID, detail string)
}

func NewCounterService(
	rdb *redis.Client,
	producer CounterEventPublisher,
	cfg *config.CounterConfig,
	failureRecorder CounterFailureRecorder,
	failureTopic string,
	messageIDGenerator MessageIDGenerator,
	logger *zap.Logger,
	auditLog AuditLogger,
) *CounterService {
	publishTimeout := config.CounterConfig{}.PublishTimeout()
	if cfg != nil {
		publishTimeout = cfg.PublishTimeout()
	}
	return &CounterService{
		redis:              rdb,
		producer:           producer,
		rebuildLockOptions: rebuildLockOptions(cfg),
		publishTimeout:     publishTimeout,
		failureRecorder:    failureRecorder,
		failureTopic:       failureTopic,
		messageIDGenerator: messageIDGenerator,
		logger:             logger,
		backoffCfg:         backoffConfig(cfg),
		rebuildRateCfg:     rebuildRateConfig(cfg),
		auditLog:           auditLog,
		likersMaxChunk:     likersMaxChunk(cfg),
	}
}

func backoffConfig(cfg *config.CounterConfig) *config.BackoffConfig {
	if cfg != nil {
		return &cfg.Rebuild.Backoff
	}
	return &config.BackoffConfig{BaseMs: 500, MaxMs: 30000}
}

func rebuildRateConfig(cfg *config.CounterConfig) *config.RebuildRateConfig {
	if cfg != nil {
		return &cfg.Rebuild.Rate
	}
	return &config.RebuildRateConfig{Permits: 3, WindowSeconds: 10}
}

// GetLikers 返回指定实体的点赞/收藏用户列表（分页）。
func (s *CounterService) GetLikers(ctx context.Context, entityType string, entityID uint64, metric string, cursor uint64, limit int) (*LikersResponse, error) {
	if limit <= 0 {
		limit = 20
	}
	if limit > 50 {
		limit = 50
	}

	prefix := "like"
	if metric == "favorite" {
		prefix = "fav"
	}

	cacheKey := fmt.Sprintf("likers_cache:%s:%d:%s", entityType, entityID, metric)
	results, err := s.redis.ZRangeByScore(ctx, cacheKey, &redis.ZRangeBy{
		Min:   "(" + strconv.FormatUint(cursor, 10),
		Max:   "+inf",
		Count: int64(limit + 1),
	}).Result()
	if err == nil && len(results) > 0 {
		return s.buildLikersFromCache(ctx, entityType, entityID, results, limit, cacheKey)
	}

	return s.scanBitmapForLikers(ctx, entityType, entityID, prefix, cursor, limit, cacheKey)
}

func (s *CounterService) buildLikersFromCache(ctx context.Context, entityType string, entityID uint64, results []string, limit int, cacheKey string) (*LikersResponse, error) {
	items := make([]LikerItem, 0, len(results))
	if len(results) > 0 {
		pipe := s.redis.Pipeline()
		type likerSlot struct {
			uidStr string
			cmd    *redis.StringCmd
		}
		cmds := make([]likerSlot, len(results))
		for i, uidStr := range results {
			timeKey := fmt.Sprintf("liker_time:%s:%d:%s", entityType, entityID, uidStr)
			cmds[i] = likerSlot{uidStr: uidStr, cmd: pipe.Get(ctx, timeKey)}
		}
		if _, err := pipe.Exec(ctx); err != nil {
			s.logger.Warn("liker time pipeline exec failed", zap.String("entityType", entityType), zap.Uint64("entityID", entityID), zap.Error(err))
		}
		for _, slot := range cmds {
			uid, err := strconv.ParseUint(slot.uidStr, 10, 64)
			if err != nil {
				continue
			}
			likedAt, _ := slot.cmd.Int64()
			items = append(items, LikerItem{UserID: uid, LikedAt: likedAt})
		}
	}

	hasMore := len(items) > limit
	if hasMore {
		items = items[:limit]
	}

	var nextCursor uint64
	if len(items) > 0 {
		nextCursor = items[len(items)-1].UserID
	}

	return &LikersResponse{Items: items, Cursor: nextCursor, HasMore: hasMore}, nil
}

// chunkFetchBatch 是一次 pipeline 中并发获取的位图分片数。
//
// 分片 GET 原本是逐个串行发出的，最坏情况下单次请求要打 defaultMaxChunk 次往返。
// 批量取回后往返数降为 ceil(扫描分片数 / chunkFetchBatch)。
const chunkFetchBatch = 16

// scanBitmapForLikers 从点赞位图中扫描出一页点赞者。
//
// 位图布局：用户 ID 是自增整数，按 ChunkSize 切成多个分片，
// 分片 c 的第 offset 位表示用户 `c*ChunkSize + offset` 是否点赞。
//
// 扫描策略（三条关键优化，均针对原实现的缺陷）：
//
//  1. **从游标所在分片起扫**。原实现每次都从分片 0 开始，把游标过滤放在解出 bit 之后
//     （`if userID <= cursor { continue }`），于是翻到第 N 页仍要重扫前面所有位，
//     深翻页复杂度是 O(已翻过的全部用户)。现在直接用游标定位起始分片与起始位。
//
//  2. **整字节跳过**。原实现逐位判断，单次请求最坏遍历
//     defaultMaxChunk × ChunkSize 个 bit（默认 128 × 65536 ≈ 838 万次循环）。
//     点赞位图通常极稀疏，绝大多数字节是 0，整字节跳过可一次略过 8 个用户。
//
//  3. **分片批量取回**。见 chunkFetchBatch。
//
// 已知边界：扫描上限为 maxChunk × ChunkSize 个用户 ID。超出该范围的用户不会出现在
// 列表中。原实现对此完全静默，现在会在触顶且可能仍有数据时打一条 Warn，
// 便于在用户规模增长到临界点前发现。
func (s *CounterService) scanBitmapForLikers(ctx context.Context, entityType string, entityID uint64, prefix string, cursor uint64, limit int, cacheKey string) (*LikersResponse, error) {
	items := make([]LikerItem, 0, limit+1)
	maxChunk := s.likersMaxChunk
	need := limit + 1

	startChunk := cursor / ChunkSize
	truncated := true

	for chunk := startChunk; chunk < maxChunk && len(items) < need; chunk += chunkFetchBatch {
		end := min(chunk+chunkFetchBatch, maxChunk)

		chunks, err := s.fetchLikerBitmapChunks(ctx, entityType, entityID, prefix, chunk, end)
		if err != nil {
			// 与 redis.Nil（分片不存在）不同，这里是真实故障。
			// 原实现把两者一并 continue，Redis 异常时会静默返回「没有点赞者」，
			// 调用方无从分辨「确实没人点赞」和「Redis 挂了」。
			return nil, fmt.Errorf("scan likers bitmap: %w", err)
		}

		for i, bmStr := range chunks {
			if bmStr == "" {
				continue
			}
			curChunk := chunk + uint64(i)
			items = appendLikersFromChunk(items, bmStr, curChunk, cursor, need)
			if len(items) >= need {
				break
			}
		}
		if len(items) >= need {
			truncated = false
			break
		}
		if end >= maxChunk {
			// 扫到上限仍未填满：说明确实没有更多数据，而不是被截断。
			truncated = false
		}
	}

	if truncated && len(items) < need {
		s.logger.Warn("likers bitmap scan hit the chunk ceiling; users beyond the scan range are not listed",
			zap.String("entityType", entityType),
			zap.Uint64("entityID", entityID),
			zap.Uint64("maxUserID", maxChunk*ChunkSize),
		)
	}

	if err := s.fillLikedAt(ctx, entityType, entityID, items); err != nil {
		s.logger.Warn("liker time pipeline exec failed for scan",
			zap.String("entityType", entityType), zap.Uint64("entityID", entityID), zap.Error(err))
	}

	hasMore := len(items) > limit
	if hasMore {
		items = items[:limit]
	}

	var nextCursor uint64
	if len(items) > 0 {
		nextCursor = items[len(items)-1].UserID
	}

	if len(items) > 0 {
		pipe := s.redis.Pipeline()
		for _, item := range items {
			pipe.ZAdd(ctx, cacheKey, redis.Z{Score: float64(item.UserID), Member: strconv.FormatUint(item.UserID, 10)})
		}
		pipe.Expire(ctx, cacheKey, time.Duration(config.DefaultLikersCacheTTLMinutes)*time.Minute)
		pipe.ZRemRangeByRank(ctx, cacheKey, 0, int64(-config.DefaultLikersCacheMaxSize-1))
		if _, err := pipe.Exec(ctx); err != nil {
			s.logger.Warn("likers cache pipeline exec failed", zap.String("entityType", entityType), zap.Uint64("entityID", entityID), zap.Error(err))
		}
	}

	return &LikersResponse{Items: items, Cursor: nextCursor, HasMore: hasMore}, nil
}

// likersMaxChunk 解析位图扫描的分片上限。
//
// 上限 × ChunkSize 即可枚举的最大用户 ID。默认 128 × 65536 ≈ 838 万，
// 用户规模接近该值时需要调大 counter.likers_max_chunk，否则新用户的点赞不会出现在列表里。
func likersMaxChunk(cfg *config.CounterConfig) uint64 {
	if cfg != nil && cfg.LikersMaxChunk > 0 {
		return uint64(cfg.LikersMaxChunk)
	}
	return defaultMaxChunk
}

// fetchLikerBitmapChunks 用一次 pipeline 取回 [from, to) 区间的位图分片。
//
// 返回切片下标与 from 对齐；分片不存在时对应元素为空串（非错误）。
// 只有真实的 Redis 故障才返回 error——调用方据此区分「没有点赞者」与「查询失败」。
func (s *CounterService) fetchLikerBitmapChunks(ctx context.Context, entityType string, entityID uint64, prefix string, from, to uint64) ([]string, error) {
	pipe := s.redis.Pipeline()
	cmds := make([]*redis.StringCmd, 0, to-from)
	for chunk := from; chunk < to; chunk++ {
		cmds = append(cmds, pipe.Get(ctx, fmt.Sprintf("bm:%s:%s:%d:%d", prefix, entityType, entityID, chunk)))
	}
	// Exec 在任一命令返回 redis.Nil 时也会返回该错误，这里按命令逐个判定，
	// 因此忽略聚合错误中的 redis.Nil。
	if _, err := pipe.Exec(ctx); err != nil && !errors.Is(err, redis.Nil) {
		return nil, err
	}

	out := make([]string, len(cmds))
	for i, cmd := range cmds {
		val, err := cmd.Result()
		if err != nil {
			if errors.Is(err, redis.Nil) {
				continue // 该分片没有任何点赞者
			}
			return nil, err
		}
		out[i] = val
	}
	return out, nil
}

// appendLikersFromChunk 从单个位图分片中解出点赞者，追加到 items 直到达到 need。
//
// cursor 语义为「严格大于」：只收集 userID > cursor 的用户。
//
// **位序必须与 Redis SETBIT 一致（大端）**：
//
//	Redis 的 SETBIT/GETBIT 把 offset 0 定义为每个字节的**最高位**（0x80），
//	offset 7 才是最低位（0x01）。位图由 TOGGLE_LUA 中的 SETBIT 写入，
//	因此解码时必须用 `1 << (7-bit)` 取掩码。
//
//	原实现写的是 `bmStr[byteIdx] & (1 << bitIdx)`，即把 offset 0 当成最低位（小端），
//	与写入方的位序正好相反。后果是每个字节内的用户位置被镜像翻转：
//	SETBIT 写入的 offset k 会被解码成 offset 7-k，
//	于是 GetLikers 一直在返回**错误的用户 ID**——
//	例如给用户 100 点赞，列表里出现的是用户 99；给用户 7 点赞，解码成用户 0 后被丢弃。
//	该缺陷由本次新增的 TestScanBitmapForLikers_BasicOrder 捕获。
func appendLikersFromChunk(items []LikerItem, bmStr string, chunk, cursor uint64, need int) []LikerItem {
	// 本分片内的起始位：仅游标所在分片需要跳过前缀部分。
	var firstOffset uint64
	if chunk == cursor/ChunkSize {
		firstOffset = cursor%ChunkSize + 1
		if firstOffset >= ChunkSize {
			return items
		}
	}

	for byteIdx := firstOffset / 8; byteIdx < uint64(len(bmStr)); byteIdx++ {
		b := bmStr[byteIdx]
		if b == 0 {
			continue // 整字节为空：一次跳过 8 个用户
		}
		for bit := uint64(0); bit < 8; bit++ {
			// 大端取位：offset 0 对应 0x80，offset 7 对应 0x01，与 SETBIT 一致。
			if b&(1<<(7-bit)) == 0 {
				continue
			}
			offset := byteIdx*8 + bit
			if offset < firstOffset {
				continue
			}
			userID := chunk*ChunkSize + offset
			if userID == 0 {
				continue // 用户 ID 自增从 1 开始，0 号位无意义
			}
			items = append(items, LikerItem{UserID: userID})
			if len(items) >= need {
				return items
			}
		}
	}
	return items
}

// fillLikedAt 批量补齐每个点赞者的点赞时间。
//
// 直接由 items 派生键，不再维护与 items 平行的键数组——
// 平行数组一旦在某个分支上追加不同步，就会出现「时间戳串到别人身上」的错位。
func (s *CounterService) fillLikedAt(ctx context.Context, entityType string, entityID uint64, items []LikerItem) error {
	if len(items) == 0 {
		return nil
	}
	pipe := s.redis.Pipeline()
	cmds := make([]*redis.StringCmd, len(items))
	for i, item := range items {
		cmds[i] = pipe.Get(ctx, fmt.Sprintf("liker_time:%s:%d:%d", entityType, entityID, item.UserID))
	}
	_, err := pipe.Exec(ctx)
	for i, cmd := range cmds {
		likedAt, _ := cmd.Int64() // 缺失时保持 0，不影响列表本身
		items[i].LikedAt = likedAt
	}
	if err != nil && !errors.Is(err, redis.Nil) {
		return err
	}
	return nil
}
