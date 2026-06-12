package knowpost

import (
	"context"
	"fmt"
	"time"

	"github.com/zhiguang/app/pkg/redislock"
)

// GetPublicFeed 获取公共 Feed 列表。
//
// 读取顺序：
//   - 先读 L1 整页缓存；
//   - 再尝试用 Redis 碎片缓存拼页；
//   - 最后在分布式锁保护下回源 MySQL。
func (s *KnowPostFeedService) GetPublicFeed(ctx context.Context, page, size int, currentUserID *uint64) (*FeedPageResponse, error) {
	safeSize := clamp(size, 1, 50)
	safePage := max(page, 1)
	feedVersion := s.currentPublicFeedVersion(ctx)
	localPageKey := fmt.Sprintf("feed:public:%d:%d:v%d:%d", safeSize, safePage, feedLayoutVer, feedVersion)

	hourSlot := time.Now().Unix() / 3600
	idsKey := fmt.Sprintf("feed:public:ids:%d:%d:%d:%d", feedVersion, safeSize, hourSlot, safePage)
	hasMoreKey := idsKey + ":hasMore"

	if val, err := s.l1Public.Get([]byte(localPageKey)); err == nil {
		resp, parseErr := s.parseFeedPage(val)
		if parseErr == nil {
			for _, item := range resp.Items {
				s.recordItemHotKey(ctx, item.ID)
			}
			return &FeedPageResponse{
				Items:   s.enrichItems(ctx, resp.Items, currentUserID),
				Page:    resp.Page,
				Size:    resp.Size,
				HasMore: resp.HasMore,
			}, nil
		}
	}

	if resp := s.assembleFromCache(ctx, idsKey, hasMoreKey, safePage, safeSize, currentUserID); resp != nil {
		s.cacheFeedPage(localPageKey, resp, s.l1Public)
		for _, item := range resp.Items {
			s.recordItemHotKey(ctx, item.ID)
		}
		return resp, nil
	}

	return s.getPublicFeedUnderLock(ctx, idsKey, hasMoreKey, localPageKey, safePage, safeSize, currentUserID)
}

// getPublicFeedUnderLock 在分布式锁保护下回源数据库并重建公共 Feed 缓存。
//
// double-check 的目的是避免多个实例串行拿锁后仍重复查库。
func (s *KnowPostFeedService) getPublicFeedUnderLock(ctx context.Context, idsKey, hasMoreKey, localPageKey string, page, size int, currentUserID *uint64) (*FeedPageResponse, error) {
	lockKey := "lock:" + idsKey
	for {
		lock, locked, err := redislock.TryAcquire(ctx, s.redis, lockKey, knowPostLockOptions())
		if err != nil {
			return nil, err
		}

		if !locked {
			if resp := s.assembleFromCache(ctx, idsKey, hasMoreKey, page, size, currentUserID); resp != nil {
				s.cacheFeedPage(localPageKey, resp, s.l1Public)
				return resp, nil
			}
			if !sleepDistributedLockRetry(ctx) {
				return nil, ctx.Err()
			}
			continue
		}

		defer lock.Release()

		if resp := s.assembleFromCache(ctx, idsKey, hasMoreKey, page, size, currentUserID); resp != nil {
			s.cacheFeedPage(localPageKey, resp, s.l1Public)
			return resp, nil
		}

		offset := (page - 1) * size
		rows, err := s.repo.ListFeedPublic(ctx, size+1, offset)
		if err != nil {
			return nil, err
		}

		hasMore := len(rows) > size
		if hasMore {
			rows = rows[:size]
		}

		items := s.mapRowsToItems(ctx, rows, currentUserID, false)
		resp := &FeedPageResponse{
			Items:   items,
			Page:    page,
			Size:    size,
			HasMore: hasMore,
		}

		s.writeFragmentCaches(ctx, idsKey, hasMoreKey, size, rows, items, hasMore)
		s.cacheFeedPage(localPageKey, resp, s.l1Public)

		return &FeedPageResponse{
			Items:   s.enrichItems(ctx, items, currentUserID),
			Page:    page,
			Size:    size,
			HasMore: hasMore,
		}, nil
	}
}
