package knowpost

import (
	"context"
	"encoding/json"
	"fmt"
	"math/rand"
	"strconv"
	"time"

	"github.com/zhiguang/app/pkg/errcode"
	"github.com/zhiguang/app/pkg/redislock"
)

// --- [详情读取链路] --- //

// GetDetail 返回知文详情，并补充当前用户维度的点赞/收藏状态。
//
// 功能：通过三级缓存 + Redis 看门狗分布式锁机制获取知文详情。
// 先从 L1（freecache 进程内缓存）查找，未命中则查 L2（Redis 分布式缓存），
// 再未命中则进入 Redis 分布式锁保护区域回源到 L3（MySQL）。
//
// 三级缓存路径详解：
//  1. L1（freecache）：
//     - 约 50ns 可返回，不经过网络，性能极高。
//     - TTL 由上游写缓存时决定（通常是 60s + jitter）。
//     - 如果 L1 命中，仍会调用 recordHotKeyAndExtendTTL 延长在 Redis 中该 key 的 TTL，
//     即热数据会在 Layer 2 层级获得更长缓存时间。
//  2. L2（Redis）：
//     - 约 1ms 响应，跨服务实例共享。
//     - 如果缓存值为 "NULL" 特殊标记，说明该 ID 对应的资源不存在，
//     直接返回 404 以避免缓存穿透（布隆过滤器的替代方案）。
//     - 如果缓存命中，还会将其写入 L1 以供后续进程内命中。
//  3. L3（MySQL 回源 + Redis 看门狗分布式锁）：
//     - 通过 Redis SET NX PX 抢锁，并在持锁期间启动本地看门狗续约，
//     确保多实例场景下同一时刻只有一个实例回源 DB。
//     - 其余实例等待缓存被前一个实例回填后直接复用结果。
//     - 详见 getDetailUnderLock 注释。
//
// 权限判定：
//   - 公开（public）+ 已发布（published）→ 任何人可查看
//   - 非公开（如 followers、school、private、unlisted）→ 仅作者本人可查看（owner check）
//   - 已删除（deleted）→ 返回 404
//
// 参数：
//   - id: uint64，知文的雪花 ID。
//   - currentUserID: *uint64，当前正在请求的用户 ID（可选）。
//     传 nil 表示未登录，此时无法获得点赞/收藏状态和私有内容的查看权限。
//
// 返回值：
//   - *KnowPostDetailResponse: 详情响应，包含标题、描述、内容 URL、作者信息、计数等。
//   - error: 错误对象。可能的值包括 errcode.ErrNotFound（内容不存在/已删除）、
//     errcode.ErrForbidden（无权限查看）。
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

// getDetailUnderLock 在 Redis 看门狗分布式锁保护下从 MySQL 回源查询详情。
//
// 功能：这是防止缓存击穿的核心方法。
// 当 L1、L2 同时未命中时，多个并发请求会竞争同一个 pageKey 对应的 Redis 锁，
// 只有拿到锁的请求才会真正查询 MySQL，其余请求循环等待并重检 Redis 缓存。
//
// 实现细节：
//  1. 通过 Redis SET NX PX 尝试抢占分布式锁，锁 key 为 `lock:{pageKey}`。
//     抢锁成功后会启动本地看门狗协程，周期性续租，避免固定 5 秒租期过短。
//  2. 抢锁成功后再次检查 Redis（double-check）：
//     如果上一个持有锁的请求已经回填了缓存，则直接返回缓存数据，无需再次查库。
//  3. 查库使用 s.repo.FindDetailByID，该 SQL JOIN users 表拿到作者信息。
//  4. 业务状态校验：
//     - status == "deleted" → 返回 404，并在 Redis 中写入 "NULL" 标记
//     以防止对已删除内容的重复查询（缓存穿透防护）。
//     - 非公开且非作者 → 返回 403 Forbidden。
//  5. 查询 db 成功后，将结果序列化为 JSON 写入 L2（Redis）和 L1（freecache），
//     TTL 由热点探测器优化：热门内容获得更长的 TTL。
//  6. 特殊标记 "NULL" 的 TTL 为 30-60 秒随机值（jitter），
//     避免所有不存在的 key 同时过期，造成周期性的缓存穿透。
//
// 参数：
//   - ctx: context.Context。
//   - id: uint64，知文 ID。
//   - pageKey: string，缓存键名（格式："knowpost:detail:{id}:v{version}"）。
//   - currentUserID: *uint64，当前用户 ID（可选）。
//
// 返回值：
//   - *KnowPostDetailResponse: 详情响应（已追加计数）。
//   - error: errcode.ErrNotFound（内容不存在或已删除）或 errcode.ErrForbidden（无权限）。
func (s *KnowPostService) getDetailUnderLock(ctx context.Context, id uint64, pageKey string, currentUserID *uint64) (*KnowPostDetailResponse, error) {
	lockKey := "lock:" + pageKey
	for {
		lock, locked, err := redislock.TryAcquire(ctx, s.redis, lockKey, knowPostLockOptions())
		if err != nil {
			return nil, err
		}

		if !locked {
			cached, _ := s.redis.Get(ctx, pageKey).Result()
			if cached == "NULL" {
				return nil, errcode.ErrNotFound.WithMsg("content not found")
			}
			if cached != "" {
				resp, parseErr := s.parseDetail([]byte(cached))
				if parseErr == nil {
					s.l1Cache.Set([]byte(pageKey), []byte(cached), 60)
					return s.enrichDetail(ctx, resp, currentUserID, true), nil
				}
			}
			if !sleepDistributedLockRetry(ctx) {
				return nil, ctx.Err()
			}
			continue
		}

		defer lock.Release()

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

		row, err := s.repo.FindDetailByID(ctx, id)
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
}

// enrichDetail 在基础详情上叠加实时计数和当前用户的点赞/收藏状态。
//
// 功能：由 GetDetail 和 getDetailUnderLock 调用，在已有 KnowPostDetailResponse
// 的基础上，补充不可缓存的用户维度数据。
//
// 为什么这些数据不缓存：
//   - 点赞数和收藏数是实时变化的，缓存会导致用户看到过期数据。
//   - 当前用户的点赞/收藏状态是请求维度的（不同用户看到的结果不同），
//     不能在缓存中共享。
//
// 参数：
//   - ctx: context.Context。
//   - base: *KnowPostDetailResponse，基础详情响应（不含用户状态和计数）。
//     函数会直接修改此对象上的字段，不会创建副本。
//   - currentUserID: *uint64，当前用户 ID（可选）。nil 表示未登录状态。
//   - refreshCounts: bool，是否重新获取 LikeCount 和 FavoriteCount。
//     当从缓存读取且希望展示最新计数时为 true；当刚从数据库查询
//     （已在 getDetailUnderLock 中获取过计数）时为 false 以避免重复查询。
//
// 返回值：
//   - *KnowPostDetailResponse: 指向同一个 base 对象，方便链式调用。
//
// 边界情况：
//   - counter == nil：不查询计数和状态，直接返回 base，不会 panic。
//   - GetCounts 失败：静默忽略错误，保留旧计数或零值，不阻塞详情页展示。
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

// parseDetail 将 JSON 字节序列反序列化为 KnowPostDetailResponse。
//
// 功能：从缓存字节流解析详情结构体。在 L1（freecache）和 L2（Redis）缓存中，
// 详情数据以 JSON 字符串形式存储，此方法负责将其还原为 Go 结构体。
//
// 参数：
//   - data: []byte，从缓存读出的 JSON 字节数据。
//
// 返回值：
//   - *KnowPostDetailResponse: 反序列化成功后的详情对象。
//   - error: 如果 JSON 格式错误（例如缓存数据损坏）则返回解析错误。
//
// 边界情况：
//   - data 为空或格式非法：返回 nil 和 err，调用方会忽略此缓存数据，继续查下一层缓存或回源 DB。
//   - 不 panic：即使 data 格式完全破坏，也只是返回 nil + error，不会导致服务崩溃。
func (s *KnowPostService) parseDetail(data []byte) (*KnowPostDetailResponse, error) {
	var resp KnowPostDetailResponse
	if err := json.Unmarshal(data, &resp); err != nil {
		return nil, err
	}
	return &resp, nil
}
