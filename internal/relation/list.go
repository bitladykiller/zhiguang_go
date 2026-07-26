package relation

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"
	"go.uber.org/zap"
)

const (
	listTypeFollowing = "following"
	listTypeFollowers = "followers"
)

// getListWithOffset 读取关注/粉丝列表，使用 offset 分页（三级缓存）。
func (s *RelationService) getListWithOffset(ctx context.Context, userID uint64, listType string, limit, offset int) ([]uint64, error) {
	isBigV := s.isBigV(ctx, userID)
	if isBigV {
		if ids, ok := s.readOffsetFromL1(listType, userID, limit, offset); ok {
			return ids, nil
		}
	}

	zsetKey := s.zsetKey(listType, userID)
	exists, exErr := s.redis.Exists(ctx, zsetKey).Result()
	if exErr != nil {
		s.logger.Warn("redis exists check failed for zset cache warm", zap.String("zsetKey", zsetKey), zap.Error(exErr))
	}
	if exists == 0 {
		warmed, err := s.ensureListCacheWarm(ctx, listType, userID)
		if err != nil {
			return nil, fmt.Errorf("get list with offset: ensure cache warm: %w", err)
		}
		if !warmed {
			return []uint64{}, nil
		}
		if isBigV {
			s.fillL1(ctx, listType, userID)
		}
	}

	members, err := s.redis.ZRevRange(ctx, zsetKey, int64(offset), int64(offset+limit-1)).Result()
	if err == nil && len(members) > 0 {
		return s.toIDList(members), nil
	}
	if s.cacheEndReached(ctx, zsetKey, offset) {
		return []uint64{}, nil
	}

	return s.readOffsetFromDB(ctx, listType, userID, limit, offset)
}

// readOffsetFromL1 尝试大 V 的 L1 本地缓存快路；ok=false 表示未命中或 offset 超出缓存长度。
func (s *RelationService) readOffsetFromL1(listType string, userID uint64, limit, offset int) ([]uint64, bool) {
	l1Key := s.l1KeyStr(listType, userID)
	data, err := s.l1.Get([]byte(l1Key))
	if err != nil {
		return nil, false
	}
	ids := s.toLongList(string(data))
	if offset >= len(ids) {
		return nil, false
	}
	end := offset + limit
	if end > len(ids) {
		end = len(ids)
	}
	return ids[offset:end], true
}

// readOffsetFromDB 是 offset 分页的 DB 兜底：读前 offset+limit 条后在应用侧切窗口。
func (s *RelationService) readOffsetFromDB(ctx context.Context, listType string, userID uint64, limit, offset int) ([]uint64, error) {
	// 防止翻页过深导致 DB 负载线性增长
	maxOffset := relationMaxOffset(s.cfg)
	if offset > maxOffset {
		return []uint64{}, nil
	}

	rows, err := s.readFromDB(ctx, listType, userID, limit+offset, 0)
	if err != nil {
		return nil, fmt.Errorf("get list with offset: read from db: %w", err)
	}
	ids := make([]uint64, 0, len(rows))
	for _, entry := range rows {
		ids = append(ids, entry.UserID)
	}
	if offset >= len(ids) {
		return []uint64{}, nil
	}
	end := offset + limit
	if end > len(ids) {
		end = len(ids)
	}
	return ids[offset:end], nil
}

// listCursor 是关注/粉丝列表的复合游标：(关注时间毫秒, 对方用户 ID) 双键。
//
// WHY 必须携带 member 而不能只用 score：
//
//	score 是毫秒时间戳，并列仍然可能（批量导入、同毫秒双关注）。
//	旧实现的游标只有 score 且用排除式区间 `("——页边界上与末条并列的成员被整体跳过，
//	翻页静默丢条目。复合游标改用**包含式**上界 + 应用侧按 (score, member) 精确跳过，
//	不重不漏。格式 "s:{ms}:{uid}"，对客户端不透明；兼容历史纯数字游标（按旧语义排除式处理）。
type listCursor struct {
	set       bool
	scoreMs   int64
	memberUID uint64
	legacy    bool // 历史纯数字游标：无 member 可用，退回排除式语义
}

func encodeListCursor(scoreMs int64, uid uint64) string {
	return "s:" + strconv.FormatInt(scoreMs, 10) + ":" + strconv.FormatUint(uid, 10)
}

func parseListCursor(raw string) (listCursor, error) {
	if raw == "" || raw == "0" {
		return listCursor{}, nil
	}
	if body, ok := strings.CutPrefix(raw, "s:"); ok {
		tsStr, uidStr, ok2 := strings.Cut(body, ":")
		if !ok2 {
			return listCursor{}, fmt.Errorf("invalid cursor")
		}
		ts, err1 := strconv.ParseInt(tsStr, 10, 64)
		uid, err2 := strconv.ParseUint(uidStr, 10, 64)
		if err1 != nil || err2 != nil {
			return listCursor{}, fmt.Errorf("invalid cursor")
		}
		return listCursor{set: true, scoreMs: ts, memberUID: uid}, nil
	}
	if ts, err := strconv.ParseInt(raw, 10, 64); err == nil && ts > 0 {
		return listCursor{set: true, scoreMs: ts, legacy: true}, nil
	}
	return listCursor{}, fmt.Errorf("invalid cursor")
}

// cursorTieSlack 是并列毫秒时间戳的取数冗余。
const cursorTieSlack = 8

// getListWithCursor 读取关注/粉丝列表，复合游标分页。
func (s *RelationService) getListWithCursor(ctx context.Context, userID uint64, listType string, limit int, cursor string) ([]uint64, string, error) {
	cur, err := parseListCursor(cursor)
	if err != nil {
		return nil, "", err
	}

	zsetKey := s.zsetKey(listType, userID)
	exists, exErr := s.redis.Exists(ctx, zsetKey).Result()
	if exErr != nil {
		s.logger.Warn("redis exists check failed for cursor-based list", zap.String("zsetKey", zsetKey), zap.Error(exErr))
	}
	if exists == 0 {
		warmed, wErr := s.ensureListCacheWarm(ctx, listType, userID)
		if wErr != nil {
			return nil, "", wErr
		}
		if !warmed {
			return []uint64{}, "", nil
		}
	}

	result, lastScore, lastUID, err := s.readCursorPageFromZSet(ctx, zsetKey, cur, limit)
	if err != nil {
		return nil, "", err
	}

	// ZSet 页取不满：可能是真到尾，也可能是缓存覆盖不全——
	// 预热有 ZSetWarmLimit（默认 2000）截断，事件增量还有 2h TTL 空洞。
	// 无论哪种，都以最后已产出条目（或请求游标）为复合边界回落 DB 续读一页；
	// DB 返回空即确证到尾。这使游标翻页不再受缓存容量约束（B 树索引一页查询的代价）。
	if len(result) < limit && s.repo != nil {
		if len(result) == 0 && !cur.set {
			return result, "", nil // 首页且 ZSet 为空：预热路径已确认无数据
		}
		result, lastScore, lastUID = s.mergeDBTail(ctx, listType, userID, cur, result, lastScore, lastUID, limit)
	}

	next := ""
	if len(result) > 0 {
		next = encodeListCursor(lastScore, lastUID)
	}
	return result, next, nil
}

// mergeDBTail 以最后已产出条目（或请求游标）为复合边界，从 DB 续读补满本页并去重合并。
// DB 读失败只记 Warn 并返回已有结果——缓存页照常可用，宁可短页也不 500。
func (s *RelationService) mergeDBTail(ctx context.Context, listType string, userID uint64, cur listCursor, result []uint64, lastScore int64, lastUID uint64, limit int) ([]uint64, int64, uint64) {
	beforeMs, beforeUID := lastScore, lastUID
	if len(result) == 0 {
		beforeMs, beforeUID = cur.scoreMs, cur.memberUID
	}
	dbRows, err := s.listFromDBBefore(ctx, listType, userID, beforeMs, beforeUID, limit-len(result))
	if err != nil {
		s.logger.Warn("cursor list db fallback failed", zap.String("listType", listType), zap.Error(err))
		return result, lastScore, lastUID
	}
	seen := make(map[uint64]struct{}, len(result))
	for _, id := range result {
		seen[id] = struct{}{}
	}
	for _, e := range dbRows {
		if _, dup := seen[e.UserID]; dup {
			continue
		}
		result = append(result, e.UserID)
		lastScore, lastUID = e.CreatedAt, e.UserID
	}
	return result, lastScore, lastUID
}

// readCursorPageFromZSet 执行 ZSet 区间读与并列成员的应用侧跳过，返回本页结果与末条复合边界。
func (s *RelationService) readCursorPageFromZSet(ctx context.Context, zsetKey string, cur listCursor, limit int) ([]uint64, int64, uint64, error) {
	maxVal := "+inf"
	fetch := limit
	if cur.set {
		if cur.legacy {
			maxVal = "(" + strconv.FormatInt(cur.scoreMs, 10)
		} else {
			maxVal = strconv.FormatInt(cur.scoreMs, 10) // 包含式：并列成员在应用侧跳过
			fetch = limit + cursorTieSlack
		}
	}

	zs, err := s.redis.ZRevRangeByScoreWithScores(ctx, zsetKey, &redis.ZRangeBy{
		Min:    "-inf",
		Max:    maxVal,
		Offset: 0,
		Count:  int64(fetch),
	}).Result()
	if err != nil {
		return nil, 0, 0, err
	}

	result := make([]uint64, 0, limit)
	var lastScore int64
	var lastUID uint64
	curMember := strconv.FormatUint(cur.memberUID, 10)
	for _, z := range zs {
		member, ok := z.Member.(string)
		if !ok {
			continue
		}
		uid, parseErr := strconv.ParseUint(member, 10, 64)
		if parseErr != nil {
			continue
		}
		scoreMs := int64(z.Score)
		// 并列 score 的精确跳过：ZREVRANGEBYSCORE 对并列成员按 member 逆字典序输出，
		// 与游标 member 做同序比较即可确定相对位置（含游标本身）。
		if cur.set && !cur.legacy && scoreMs == cur.scoreMs && member >= curMember {
			continue
		}
		result = append(result, uid)
		lastScore, lastUID = scoreMs, uid
		if len(result) == limit {
			break
		}
	}

	return result, lastScore, lastUID, nil
}

// listFromDBBefore 按复合边界 (createdMs, uid) 从 DB 续读一页（游标深翻页的缓存外兜底）。
func (s *RelationService) listFromDBBefore(ctx context.Context, listType string, userID uint64, beforeMs int64, beforeUID uint64, limit int) ([]listEntry, error) {
	before := time.UnixMilli(beforeMs)
	if listType == listTypeFollowing {
		rows, err := s.repo.ListFollowingRowsBefore(ctx, userID, before, beforeUID, limit)
		if err != nil {
			return nil, err
		}
		out := make([]listEntry, len(rows))
		for i, r := range rows {
			out[i] = listEntry{UserID: r.ToUserID, CreatedAt: r.CreatedAt.UnixMilli()}
		}
		return out, nil
	}
	rows, err := s.repo.ListFollowerRowsBefore(ctx, userID, before, beforeUID, limit)
	if err != nil {
		return nil, err
	}
	out := make([]listEntry, len(rows))
	for i, r := range rows {
		out[i] = listEntry{UserID: r.FromUserID, CreatedAt: r.CreatedAt.UnixMilli()}
	}
	return out, nil
}

// zsetKey 生成 Redis ZSet 缓存键。
func (s *RelationService) zsetKey(listType string, userID uint64) string {
	return fmt.Sprintf("z:%s:%d", listType, userID)
}

// readFromDB 从数据库读取用户的关注/粉丝列表。
func (s *RelationService) readFromDB(ctx context.Context, listType string, userID uint64, limit, offset int) ([]listEntry, error) {
	if listType == listTypeFollowing {
		if s.repo == nil {
			return nil, fmt.Errorf("relation: repository is nil")
		}
		rows, err := s.repo.ListFollowingRows(ctx, userID, limit, offset)
		if err != nil {
			return nil, fmt.Errorf("read from db: list following rows: %w", err)
		}
		entries := make([]listEntry, len(rows))
		for i, r := range rows {
			entries[i] = listEntry{UserID: r.ToUserID, CreatedAt: r.CreatedAt.UnixMilli()}
		}
		return entries, nil
	}
	rows, err := s.repo.ListFollowerRows(ctx, userID, limit, offset)
	if err != nil {
		return nil, fmt.Errorf("read from db: list follower rows: %w", err)
	}
	if len(rows) == 0 {
		if s.shouldFallbackToFollowing(ctx, userID) {
			rows, err = s.repo.ListFollowerRowsFromFollowing(ctx, userID, limit, offset)
			if err != nil {
				return nil, fmt.Errorf("read from db: list follower rows from following: %w", err)
			}
			if len(rows) == 0 {
				s.markFollowerFallbackExhausted(ctx, userID)
			}
		}
	}
	entries := make([]listEntry, len(rows))
	for i, r := range rows {
		entries[i] = listEntry{UserID: r.FromUserID, CreatedAt: r.CreatedAt.UnixMilli()}
	}
	return entries, nil
}
