// Package counter — 点赞/收藏用户列表（likers）子系统。
//
// 本文件集中 likers 的键 schema、游标编解码与两条读取路径；
// 与 toggle（bitmap_toggle.go）、快照读（sds_read.go）职责分离。
package counter

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"
	"go.uber.org/zap"

	"github.com/zhiguang/app/pkg/config"
)

// 指标标识：API 层 metric 与存储层前缀。
const (
	metricLike     = "like"
	metricFavorite = "favorite"
	prefixLike     = "like"
	prefixFav      = "fav"
)

// ── 键 schema ────────────────────────────────────────────────────────────────
//
//	likers:{metric}:{entityType}:{entityID}   ZSet   点赞者按时间索引（score=点赞秒级时间戳）
//	likers_cache:{entityType}:{entityID}:{m}  ZSet   位图扫描结果缓存（仅回退路径使用）
//
// 点赞时间的**唯一真源**是 ZSet 的 score。曾存在平行键 liker_time:{...}——
// 但全仓库只有读取方、从无写入方：回退路径为它逐人发 GET，每次都 miss，
// LikedAt 恒为 0 还白付一轮 pipeline。一份数据不该有第二个（何况从未生效的）存放处，
// 该键空间已整体移除；回退路径的 LikedAt 诚实置 0（历史数据的点赞时间本就不可考）。
//
// WHY 引入按时间的 ZSet 索引：
//
//	点赞状态的真值是 Bitmap（O(1) 判定、内存极省），但 Bitmap 只能按**用户 ID**
//	枚举成员——于是「谁赞了我」列表曾经按 userID 排序，而产品语义几乎总是
//	「最近点赞的人在前」。这是**存储选型反向决定 API 语义**的典型倒置。
//	正确做法是让每种查询各有胜任的索引：Bitmap 继续当真值管判定与计数，
//	ZSet（score=时间）专门服务时间序列表。两者由 toggle 的同一段 Lua 原子维护。

func likersZSetKey(metric, entityType, entityID string) string {
	return fmt.Sprintf("likers:%s:%s:%s", metric, entityType, entityID)
}

func likersScanCacheKey(entityType string, entityID uint64, metric string) string {
	return fmt.Sprintf("likers_cache:%s:%d:%s", entityType, entityID, metric)
}

// ── 游标 ────────────────────────────────────────────────────────────────────
//
// 游标是自描述的复合串，客户端视作不透明值原样回传：
//
//	"t:{unixTs}:{userID}"  时间序路径（ZSet），按 (score, member) 双键定位
//	"u:{userID}"           用户 ID 序路径（位图回退），按 userID 定位
//
// WHY 必须携带 member 而不能只用 score：
//
//	同一秒内可能有多人点赞（score 并列）。若游标只有 score 且用排除式区间
//	`(score`，页边界上与其并列的成员会被整体跳过——这正是 relation 列表
//	曾经的隐性丢条目缺陷。带上 member 后，取到并列 score 的成员时在应用侧
//	按 member 精确跳过「游标之前」的部分，翻页不重不漏。

type likersCursor struct {
	byTime bool
	ts     int64
	userID uint64
}

func encodeTimeCursor(ts int64, userID uint64) string {
	return "t:" + strconv.FormatInt(ts, 10) + ":" + strconv.FormatUint(userID, 10)
}

func encodeUIDCursor(userID uint64) string {
	return "u:" + strconv.FormatUint(userID, 10)
}

// parseLikersCursor 解析游标；空串表示第一页。兼容历史纯数字游标（按 uid 语义）。
func parseLikersCursor(raw string) (likersCursor, error) {
	if raw == "" || raw == "0" {
		return likersCursor{}, nil
	}
	if ts, uidStr, ok := strings.Cut(strings.TrimPrefix(raw, "t:"), ":"); ok && strings.HasPrefix(raw, "t:") {
		t, err1 := strconv.ParseInt(ts, 10, 64)
		u, err2 := strconv.ParseUint(uidStr, 10, 64)
		if err1 != nil || err2 != nil {
			return likersCursor{}, errors.New("invalid cursor")
		}
		return likersCursor{byTime: true, ts: t, userID: u}, nil
	}
	if uidStr, found := strings.CutPrefix(raw, "u:"); found {
		u, err := strconv.ParseUint(uidStr, 10, 64)
		if err != nil {
			return likersCursor{}, errors.New("invalid cursor")
		}
		return likersCursor{userID: u}, nil
	}
	// 历史客户端传纯数字（旧 uid 游标）
	if u, err := strconv.ParseUint(raw, 10, 64); err == nil {
		return likersCursor{userID: u}, nil
	}
	return likersCursor{}, errors.New("invalid cursor")
}

// ── 读取入口 ─────────────────────────────────────────────────────────────────

// GetLikers 返回指定实体的点赞/收藏用户列表（游标分页）。
//
// 主路径：likers ZSet 按点赞时间倒序（产品期望的「最近在前」）。
// 回退路径：ZSet 不存在（该实体的互动早于本索引上线、尚未重建）时，
// 退回位图扫描——顺序退化为按 userID，游标带 "u:" 前缀保持路径内自洽。
func (s *CounterService) GetLikers(ctx context.Context, entityType string, entityID uint64, metric string, cursor string, limit int) (*LikersResponse, error) {
	limit = clampLikersLimit(limit)
	cur, err := parseLikersCursor(cursor)
	if err != nil {
		return nil, err
	}

	prefix := prefixLike
	if metric == metricFavorite {
		prefix = prefixFav
	}

	// 位图路径的游标只能续位图路径
	if !cur.byTime && cur.userID > 0 {
		return s.scanBitmapForLikers(ctx, entityType, entityID, prefix, cur.userID, limit)
	}

	entityIDStr := strconv.FormatUint(entityID, 10)
	zkey := likersZSetKey(prefix, entityType, entityIDStr)

	items, hasMore, err := s.readLikersByTime(ctx, zkey, cur, limit)
	if err != nil {
		return nil, err
	}
	if items == nil {
		// ZSet 不存在：历史数据回退到位图扫描
		return s.scanBitmapForLikers(ctx, entityType, entityID, prefix, 0, limit)
	}

	resp := &LikersResponse{Items: items, HasMore: hasMore}
	if len(items) > 0 {
		last := items[len(items)-1]
		resp.Cursor = encodeTimeCursor(last.LikedAt, last.UserID)
	}
	return resp, nil
}

func clampLikersLimit(limit int) int {
	if limit <= 0 {
		return 20
	}
	if limit > 50 {
		return 50
	}
	return limit
}

// readLikersByTime 从时间序 ZSet 读一页。返回 items==nil 表示索引不存在（调用方回退）。
//
// 续页定位分两级：
//
//  1. **rank 快路**：游标成员仍在 ZSet 时，ZREVRANK 拿全局排名后按排名区间取下一页——
//     与并列规模完全无关。这是必须的：score 区间 + 固定并列冗余的方案在
//     「海量同 score」下会失效——第 k 页要跳过 k×limit 个并列成员，而固定冗余
//     只能覆盖十几个，翻几页后整页都是已见成员、被误判为翻尽提前终止。
//     离线重建的历史数据 score 统一为 0，正是海量并列的现实来源。
//  2. **score 慢路**：游标成员已被删（unlike）时 rank 不可得，退回
//     包含式上界 + (score, member) 应用侧跳过；此时并列冗余仅需覆盖
//     自然场景（同秒点赞十几人），足够。
func (s *CounterService) readLikersByTime(ctx context.Context, zkey string, cur likersCursor, limit int) ([]LikerItem, bool, error) {
	if cur.byTime {
		if items, hasMore, handled, err := s.readLikersByRank(ctx, zkey, cur, limit); handled {
			return items, hasMore, err
		}
		// rank 不可得（游标成员已被 unlike）→ score 慢路
	}
	return s.readLikersByScore(ctx, zkey, cur, limit)
}

// readLikersByRank 是续页快路：游标成员仍在时按全局排名取下一页，与并列规模无关。
// handled=false 表示成员已不在（redis.Nil），调用方转 score 慢路。
func (s *CounterService) readLikersByRank(ctx context.Context, zkey string, cur likersCursor, limit int) (items []LikerItem, hasMore, handled bool, err error) {
	member := strconv.FormatUint(cur.userID, 10)
	rank, rankErr := s.redis.ZRevRank(ctx, zkey, member).Result()
	if rankErr != nil {
		if errors.Is(rankErr, redis.Nil) {
			return nil, false, false, nil
		}
		return nil, false, true, fmt.Errorf("rank likers cursor: %w", rankErr)
	}
	zs, rErr := s.redis.ZRevRangeWithScores(ctx, zkey, rank+1, rank+int64(limit)+1).Result()
	if rErr != nil {
		return nil, false, true, fmt.Errorf("read likers zset by rank: %w", rErr)
	}
	items, hasMore, err = packLikerPage(zs, limit)
	return items, hasMore, true, err
}

// readLikersByScore 是首页/慢路：包含式上界 + (score, member) 应用侧跳过。
func (s *CounterService) readLikersByScore(ctx context.Context, zkey string, cur likersCursor, limit int) ([]LikerItem, bool, error) {
	maxArg := "+inf"
	if cur.byTime {
		maxArg = strconv.FormatInt(cur.ts, 10)
	}

	fetch := limit + 1
	if cur.byTime {
		fetch += 16 // 慢路并列冗余：仅需覆盖自然同秒点赞规模
	}

	zs, err := s.redis.ZRevRangeByScoreWithScores(ctx, zkey, &redis.ZRangeBy{
		Min: "-inf", Max: maxArg, Offset: 0, Count: int64(fetch),
	}).Result()
	if err != nil {
		return nil, false, fmt.Errorf("read likers zset: %w", err)
	}
	if len(zs) == 0 {
		if cur.byTime {
			return []LikerItem{}, false, nil // 翻页翻尽
		}
		// 第一页为空：区分「无人点赞」与「索引未建」
		exists, err := s.redis.Exists(ctx, zkey).Result()
		if err != nil {
			return nil, false, fmt.Errorf("check likers zset: %w", err)
		}
		if exists == 0 {
			return nil, false, nil // 触发位图回退
		}
		return []LikerItem{}, false, nil
	}

	items, hasMore := filterLikerPage(zs, cur, limit)
	return items, hasMore, nil
}

// filterLikerPage 把 ZSet 原始条目转为一页点赞者：解析成员、执行游标的
// (score, member) 应用侧跳过、截断到 limit 并判定 hasMore。
//
// 跳过规则：游标本身及同 score 下「排在游标之前」的成员都要略过。
// ZREVRANGEBYSCORE 对并列 score 按 member 逆字典序输出，
// 与游标 member 比较即可确定相对位置。
func filterLikerPage(zs []redis.Z, cur likersCursor, limit int) ([]LikerItem, bool) {
	items := make([]LikerItem, 0, limit+1)
	for _, z := range zs {
		member, ok := z.Member.(string)
		if !ok {
			continue
		}
		uid, parseErr := strconv.ParseUint(member, 10, 64)
		if parseErr != nil {
			continue
		}
		ts := int64(z.Score)
		if cur.byTime && ts == cur.ts {
			curMember := strconv.FormatUint(cur.userID, 10)
			if member >= curMember {
				continue
			}
		}
		items = append(items, LikerItem{UserID: uid, LikedAt: ts})
		if len(items) == limit+1 {
			break
		}
	}

	hasMore := len(items) > limit
	if hasMore {
		items = items[:limit]
	}
	return items, hasMore
}

// ── 位图回退路径（历史数据） ──────────────────────────────────────────────────

// chunkFetchBatch 是一次 pipeline 中并发获取的位图分片数。
const chunkFetchBatch = 16

// scanBitmapForLikers 从点赞位图扫描一页点赞者（按 userID 升序）。
//
// 仅作为时间序 ZSet 缺失时的回退：位图无法按时间枚举，顺序退化为 userID。
// 扫描要点（游标定位起始分片、整字节跳过、分片批量取回、故障不伪装成空结果）
// 与位序约定（SETBIT 大端）见各辅助函数注释。
func (s *CounterService) scanBitmapForLikers(ctx context.Context, entityType string, entityID uint64, prefix string, cursorUID uint64, limit int) (*LikersResponse, error) {
	cacheKey := likersScanCacheKey(entityType, entityID, prefix)

	if resp, hit := s.tryLikersScanCache(ctx, entityType, entityID, cacheKey, cursorUID, limit); hit {
		return resp, nil
	}

	items, err := s.scanLikerChunks(ctx, entityType, entityID, prefix, cursorUID, limit)
	if err != nil {
		return nil, err
	}

	hasMore := len(items) > limit
	if hasMore {
		items = items[:limit]
	}

	resp := &LikersResponse{Items: items, HasMore: hasMore}
	if len(items) > 0 {
		resp.Cursor = encodeUIDCursor(items[len(items)-1].UserID)
		s.fillLikersScanCache(ctx, cacheKey, items, entityType, entityID)
	}

	return resp, nil
}

// scanLikerChunks 从游标所在分片开始批量取回位图并解出点赞者，最多取 limit+1 条。
// 扫描触顶（maxChunk 之外可能还有用户）时记 Warn——宁可显式提示也不静默截断。
func (s *CounterService) scanLikerChunks(ctx context.Context, entityType string, entityID uint64, prefix string, cursorUID uint64, limit int) ([]LikerItem, error) {
	items := make([]LikerItem, 0, limit+1)
	maxChunk := s.likersMaxChunk
	need := limit + 1

	startChunk := cursorUID / ChunkSize
	truncated := true

	for chunk := startChunk; chunk < maxChunk && len(items) < need; chunk += chunkFetchBatch {
		end := min(chunk+chunkFetchBatch, maxChunk)

		chunks, err := s.fetchLikerBitmapChunks(ctx, entityType, entityID, prefix, chunk, end)
		if err != nil {
			// 与 redis.Nil（分片不存在）不同，这里是真实故障：
			// 不能伪装成「没有点赞者」，调用方需要能分辨故障与空结果。
			return nil, fmt.Errorf("scan likers bitmap: %w", err)
		}

		for i, bmStr := range chunks {
			if bmStr == "" {
				continue
			}
			items = appendLikersFromChunk(items, bmStr, chunk+uint64(i), cursorUID, need)
			if len(items) >= need {
				break
			}
		}
		if len(items) >= need {
			truncated = false
			break
		}
		if end >= maxChunk {
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
	return items, nil
}

// fillLikersScanCache 把本页扫描结果回填到扫描缓存（加分片 TTL 与容量上限），加速同页重读。
func (s *CounterService) fillLikersScanCache(ctx context.Context, cacheKey string, items []LikerItem, entityType string, entityID uint64) {
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

// tryLikersScanCache 探测位图扫描结果缓存；取满 limit+1 条才可信（否则视为 miss）。
func (s *CounterService) tryLikersScanCache(ctx context.Context, entityType string, entityID uint64, cacheKey string, cursorUID uint64, limit int) (*LikersResponse, bool) {
	cached, err := s.redis.ZRangeByScore(ctx, cacheKey, &redis.ZRangeBy{
		Min: "(" + strconv.FormatUint(cursorUID, 10), Max: "+inf", Count: int64(limit + 1),
	}).Result()
	if err != nil || len(cached) <= limit {
		return nil, false
	}
	resp, buildErr := s.buildLikersFromCache(ctx, entityType, entityID, cached, limit)
	if buildErr != nil {
		return nil, false
	}
	return resp, true
}

// buildLikersFromCache 用扫描缓存组装一页（已确认缓存取满 limit+1 条）。
func (s *CounterService) buildLikersFromCache(_ context.Context, _ string, _ uint64, results []string, limit int) (*LikersResponse, error) {
	items := make([]LikerItem, 0, len(results))
	for _, uidStr := range results {
		uid, err := strconv.ParseUint(uidStr, 10, 64)
		if err != nil {
			continue
		}
		items = append(items, LikerItem{UserID: uid})
	}

	hasMore := len(items) > limit
	if hasMore {
		items = items[:limit]
	}

	resp := &LikersResponse{Items: items, HasMore: hasMore}
	if len(items) > 0 {
		resp.Cursor = encodeUIDCursor(items[len(items)-1].UserID)
	}
	return resp, nil
}

// fetchLikerBitmapChunks 用一次 pipeline 取回 [from, to) 区间的位图分片。
// 分片不存在时对应元素为空串；只有真实 Redis 故障才返回 error。
func (s *CounterService) fetchLikerBitmapChunks(ctx context.Context, entityType string, entityID uint64, prefix string, from, to uint64) ([]string, error) {
	pipe := s.redis.Pipeline()
	cmds := make([]*redis.StringCmd, 0, to-from)
	entityIDStr := strconv.FormatUint(entityID, 10)
	for chunk := from; chunk < to; chunk++ {
		cmds = append(cmds, pipe.Get(ctx, BitmapKey(prefix, entityType, entityIDStr, chunk)))
	}
	if _, err := pipe.Exec(ctx); err != nil && !errors.Is(err, redis.Nil) {
		return nil, err
	}

	out := make([]string, len(cmds))
	for i, cmd := range cmds {
		val, err := cmd.Result()
		if err != nil {
			if errors.Is(err, redis.Nil) {
				continue
			}
			return nil, err
		}
		out[i] = val
	}
	return out, nil
}

// appendLikersFromChunk 从单个位图分片解出点赞者（cursor 严格大于语义）。
//
// 位序必须与 Redis SETBIT 一致（大端：offset 0 是每字节最高位 0x80）。
// 早期实现用小端 `1<<bit` 解码，每字节内位置被镜像翻转，
// GetLikers 返回过成体系的错误用户 ID（给 100 点赞显示成 99）。
func appendLikersFromChunk(items []LikerItem, bmStr string, chunk, cursor uint64, need int) []LikerItem {
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
			if b&(1<<(7-bit)) == 0 {
				continue
			}
			offset := byteIdx*8 + bit
			if offset < firstOffset {
				continue
			}
			userID := chunk*ChunkSize + offset
			if userID == 0 {
				continue // 用户 ID 自增从 1 开始
			}
			items = append(items, LikerItem{UserID: userID})
			if len(items) >= need {
				return items
			}
		}
	}
	return items
}

// packLikerPage 把 rank 区间取回的成员打包成一页（已天然有序、无需跳过）。
func packLikerPage(zs []redis.Z, limit int) ([]LikerItem, bool, error) {
	items := make([]LikerItem, 0, limit+1)
	for _, z := range zs {
		member, ok := z.Member.(string)
		if !ok {
			continue
		}
		uid, err := strconv.ParseUint(member, 10, 64)
		if err != nil {
			continue
		}
		items = append(items, LikerItem{UserID: uid, LikedAt: int64(z.Score)})
		if len(items) == limit+1 {
			break
		}
	}
	hasMore := items != nil && len(items) > limit
	if hasMore {
		items = items[:limit]
	}
	if items == nil {
		items = []LikerItem{}
	}
	return items, hasMore, nil
}

// RebuildLikersTimeIndex 从位图离线重建某实体某指标的时间序索引。
//
// 用途：时间序 ZSet 随 toggle Lua 只对**新互动**生效；上线前的历史点赞
// 只存在于位图，列表只能走回退路径（userID 序）。本方法全量扫描位图，
// 把历史成员以 **score=0** 批量写入 ZSet——0 表示"时间不可考"，
// 时间倒序下自然排在所有真实时间之后（最旧），语义诚实。
//
// 写入用 ZADD NX：绝不覆盖 toggle 已写入的真实时间戳（重建期间的新点赞优先）。
// 深翻页遇到海量 score=0 并列由 rank 快路保证正确（见 readLikersByTime）。
//
// 幂等：可重复执行；返回本次新写入的成员数。
func (s *CounterService) RebuildLikersTimeIndex(ctx context.Context, entityType string, entityID uint64, metric string) (int64, error) {
	prefix := prefixLike
	if metric == metricFavorite || metric == prefixFav {
		prefix = prefixFav
	}
	zkey := likersZSetKey(prefix, entityType, strconv.FormatUint(entityID, 10))

	var added int64
	maxChunk := s.likersMaxChunk
	for chunk := uint64(0); chunk < maxChunk; chunk += chunkFetchBatch {
		end := min(chunk+chunkFetchBatch, maxChunk)
		chunks, err := s.fetchLikerBitmapChunks(ctx, entityType, entityID, prefix, chunk, end)
		if err != nil {
			return added, fmt.Errorf("rebuild likers: fetch chunks: %w", err)
		}

		var members []redis.Z
		for i, bmStr := range chunks {
			if bmStr == "" {
				continue
			}
			items := appendLikersFromChunk(nil, bmStr, chunk+uint64(i), 0, int(ChunkSize))
			for _, it := range items {
				members = append(members, redis.Z{Score: 0, Member: strconv.FormatUint(it.UserID, 10)})
			}
		}
		if len(members) == 0 {
			continue
		}
		n, err := s.redis.ZAddNX(ctx, zkey, members...).Result()
		if err != nil {
			return added, fmt.Errorf("rebuild likers: zadd: %w", err)
		}
		added += n
	}
	return added, nil
}
