package knowpost

import (
	"context"
	"encoding/json"
	"fmt"
	"math/rand"
	"strconv"
	"time"
)

// GetMyPublished 返回当前用户已发布的知文列表。
//
// 这个读路径不使用碎片缓存，而是直接缓存整页。
// 原因是“我的 feed”写入频率更低、数据范围更窄，整页缓存更直接。
func (s *KnowPostFeedService) GetMyPublished(userID uint64, page, size int) (*FeedPageResponse, error) {
	ctx := context.Background()
	safeSize := clamp(size, 1, 50)
	safePage := max(page, 1)
	feedVersion := s.currentMineFeedVersion(ctx, userID)
	key := fmt.Sprintf("feed:mine:%d:%d:%d:%d", userID, safeSize, safePage, feedVersion)

	if val, err := s.l1Mine.Get([]byte(key)); err == nil {
		resp, parseErr := s.parseFeedPage(val)
		if parseErr == nil {
			s.hotKey.Record(key)
			return resp, nil
		}
	}

	cached, err := s.redis.Get(ctx, key).Result()
	if err == nil && cached != "" {
		resp, parseErr := s.parseFeedPage([]byte(cached))
		if parseErr == nil {
			setFreeCacheValue(s.l1Mine, key, []byte(cached), 30)
			s.hotKey.Record(key)
			return resp, nil
		}
	}

	offset := (safePage - 1) * safeSize
	rows, err := s.repo.ListMyPublished(ctx, userID, safeSize+1, offset)
	if err != nil {
		return nil, err
	}

	hasMore := len(rows) > safeSize
	if hasMore {
		rows = rows[:safeSize]
	}

	items := s.mapRowsToItems(ctx, rows, &userID, true)
	resp := &FeedPageResponse{
		Items:   items,
		Page:    safePage,
		Size:    safeSize,
		HasMore: hasMore,
	}

	jsonBytes, _ := json.Marshal(resp)
	baseTTL := 30 + rand.Intn(21)
	s.redis.Set(ctx, key, string(jsonBytes), time.Duration(baseTTL)*time.Second)
	setFreeCacheValue(s.l1Mine, key, jsonBytes, baseTTL)
	s.hotKey.Record(key)

	return resp, nil
}

// InvalidateAfterPostMutation 在知文变更后失效相关 Feed 缓存。
//
// 策略是删除单条 item 碎片，并递增公共 feed 与我的 feed 版本号。
// 这样不需要枚举所有分页键，也不会留下部分页仍命中旧内容的问题。
func (s *KnowPostFeedService) InvalidateAfterPostMutation(ctx context.Context, postID, creatorID uint64) {
	s.redis.Del(ctx, "feed:item:"+strconv.FormatUint(postID, 10))
	s.redis.Incr(ctx, publicFeedVersionKey)
	s.redis.Incr(ctx, fmt.Sprintf(mineFeedVersionKey, creatorID))
}
