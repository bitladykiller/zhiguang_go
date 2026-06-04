package knowpost

import (
	"context"
	"encoding/json"
	"fmt"
	"math/rand"
	"strconv"
	"sync"
	"time"

	"github.com/zhiguang/app/pkg/errcode"
)

// --- [详情读取链路] --- //

// GetDetail 返回知文详情，并补充当前用户维度的点赞/收藏状态。
//
// 三级缓存读取路径：
//  1. L1（freecache）：进程级缓存，约 50ns 可返回，不经过网络。
//  2. L2（Redis）：分布式缓存，约 1ms 响应，跨服务实例共享。
//  3. Singleflight 锁 + L3（MySQL）：缓存同时失效时防止击穿。
//
// 权限判定：
//   - 公开（public）+ 已发布（published）→ 任何人可查看
//   - 非公开 → 仅作者本人可查看（owner check）
//   - 已删除（deleted）→ 返回 404
func (s *KnowPostService) GetDetail(id uint64, currentUserID *uint64) (*KnowPostDetailResponse, error) {
	ctx := context.Background()
	pageKey := fmt.Sprintf("knowpost:detail:%d:v%d", id, detailLayoutVer)

	if val, err := s.l1Cache.Get([]byte(pageKey)); err == nil {
		s.recordHotKeyAndExtendTTL(id, pageKey)
		resp, parseErr := s.parseDetail(val)
		if parseErr == nil {
			return s.enrichDetail(ctx, resp, currentUserID, true), nil
		}
	}

	cached, err := s.redis.Get(ctx, pageKey).Result()
	if err == nil && cached != "" {
		if cached == "NULL" {
			return nil, errcode.ErrNotFound.WithMsg("content not found")
		}
		s.l1Cache.Set([]byte(pageKey), []byte(cached), 60)
		s.recordHotKeyAndExtendTTL(id, pageKey)
		resp, parseErr := s.parseDetail([]byte(cached))
		if parseErr == nil {
			return s.enrichDetail(ctx, resp, currentUserID, true), nil
		}
	}

	return s.getDetailUnderLock(ctx, id, pageKey, currentUserID)
}

func (s *KnowPostService) getDetailUnderLock(ctx context.Context, id uint64, pageKey string, currentUserID *uint64) (*KnowPostDetailResponse, error) {
	lockIface, _ := s.singleFlight.LoadOrStore(pageKey, &sync.Mutex{})
	mu := lockIface.(*sync.Mutex)
	mu.Lock()
	defer func() {
		s.singleFlight.Delete(pageKey)
		mu.Unlock()
	}()

	cached, _ := s.redis.Get(ctx, pageKey).Result()
	if cached == "NULL" {
		return nil, errcode.ErrNotFound.WithMsg("content not found")
	}
	if cached != "" {
		resp, err := s.parseDetail([]byte(cached))
		if err == nil {
			return s.enrichDetail(ctx, resp, currentUserID, true), nil
		}
	}

	row, err := s.repo.FindDetailByID(id)
	if err != nil || row == nil || row.Status == "deleted" {
		ttl := time.Duration(30+rand.Intn(31)) * time.Second
		s.redis.Set(ctx, pageKey, "NULL", ttl)
		return nil, errcode.ErrNotFound.WithMsg("content not found")
	}

	isPublic := row.Status == "published" && row.Visible == "public"
	isOwner := currentUserID != nil && *currentUserID == row.CreatorID
	if !isPublic && !isOwner {
		return nil, errcode.ErrForbidden.WithMsg("no permission to view")
	}

	resp := &KnowPostDetailResponse{
		ID:             strconv.FormatUint(row.ID, 10),
		Title:          row.Title,
		Description:    row.Description,
		ContentUrl:     row.ContentUrl,
		Images:         parseStringArray(row.ImgUrls),
		Tags:           parseStringArray(row.Tags),
		AuthorID:       strconv.FormatUint(row.CreatorID, 10),
		AuthorAvatar:   row.AuthorAvatar,
		AuthorNickname: row.AuthorNickname,
		AuthorTagJson:  row.AuthorTagJson,
		IsTop:          row.IsTop,
		Visible:        row.Visible,
		Type:           row.Type,
		PublishTime:    row.PublishTime,
	}

	if s.counter != nil {
		counts, _ := s.counter.GetCounts(ctx, "knowpost", strconv.FormatUint(id, 10), []string{"like", "fav"})
		resp.LikeCount = int64(counts["like"])
		resp.FavoriteCount = int64(counts["fav"])
	}

	jsonBytes, _ := json.Marshal(resp)
	baseTTL := 60 + rand.Intn(31)
	targetTTL := s.hotKey.TtlForPublic(baseTTL, pageKey)
	if targetTTL < baseTTL {
		targetTTL = baseTTL
	}
	s.redis.Set(ctx, pageKey, string(jsonBytes), time.Duration(targetTTL)*time.Second)
	s.l1Cache.Set([]byte(pageKey), jsonBytes, targetTTL)

	return s.enrichDetail(ctx, resp, currentUserID, false), nil
}

// enrichDetail 在基础详情上叠加实时计数和当前用户的点赞/收藏状态。
func (s *KnowPostService) enrichDetail(ctx context.Context, base *KnowPostDetailResponse, currentUserID *uint64, refreshCounts bool) *KnowPostDetailResponse {
	if s.counter == nil {
		return base
	}

	if refreshCounts {
		counts, _ := s.counter.GetCounts(ctx, "knowpost", base.ID, []string{"like", "fav"})
		if counts != nil {
			base.LikeCount = int64(counts["like"])
			base.FavoriteCount = int64(counts["fav"])
		}
	}

	if currentUserID != nil {
		liked, _ := s.counter.IsLiked(ctx, *currentUserID, "knowpost", base.ID)
		faved, _ := s.counter.IsFaved(ctx, *currentUserID, "knowpost", base.ID)
		base.Liked = &liked
		base.Faved = &faved
	}

	return base
}

func (s *KnowPostService) parseDetail(data []byte) (*KnowPostDetailResponse, error) {
	var resp KnowPostDetailResponse
	if err := json.Unmarshal(data, &resp); err != nil {
		return nil, err
	}
	return &resp, nil
}
