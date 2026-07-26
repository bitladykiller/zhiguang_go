package relation

import (
	"context"
	"fmt"
	"strconv"
	"strings"

	"github.com/redis/go-redis/v9"
	"go.uber.org/zap"
)

// getListWithOffset 读取关注/粉丝列表，使用 offset 分页（三级缓存）。
func (s *RelationService) getListWithOffset(ctx context.Context, userID uint64, listType string, limit, offset int) ([]uint64, error) {
	isBigV := s.isBigV(ctx, userID)
	if isBigV {
		l1Key := s.l1KeyStr(listType, userID)
		if data, err := s.l1.Get([]byte(l1Key)); err == nil {
			ids := s.toLongList(string(data))
			if offset < len(ids) {
				end := offset + limit
				if end > len(ids) {
					end = len(ids)
				}
				return ids[offset:end], nil
			}
		}
	}

	zsetKey := s.zsetKey(listType, userID)
	exists, err := s.redis.Exists(ctx, zsetKey).Result()
	if err != nil {
		s.logger.Warn("redis exists check failed for zset cache warm", zap.String("zsetKey", zsetKey), zap.Error(err))
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
	exists, err := s.redis.Exists(ctx, zsetKey).Result()
	if err != nil {
		s.logger.Warn("redis exists check failed for cursor-based list", zap.String("zsetKey", zsetKey), zap.Error(err))
	}
	if exists == 0 {
		warmed, err := s.ensureListCacheWarm(ctx, listType, userID)
		if err != nil {
			return nil, "", err
		}
		if !warmed {
			return []uint64{}, "", nil
		}
	}

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
		return nil, "", err
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

	next := ""
	if len(result) > 0 {
		next = encodeListCursor(lastScore, lastUID)
	}
	return result, next, nil
}

// zsetKey 生成 Redis ZSet 缓存键。
func (s *RelationService) zsetKey(listType string, userID uint64) string {
	return fmt.Sprintf("z:%s:%d", listType, userID)
}

// readFromDB 从数据库读取用户的关注/粉丝列表。
func (s *RelationService) readFromDB(ctx context.Context, listType string, userID uint64, limit, offset int) ([]listEntry, error) {
	if listType == "following" {
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
