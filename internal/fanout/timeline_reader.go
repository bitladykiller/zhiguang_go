package fanout

import (
	"context"
	"fmt"
	"sort"
	"strconv"

	"github.com/redis/go-redis/v9"
	"go.uber.org/zap"
)

// TimelineEntry 是归并后的一条信息流条目。
type TimelineEntry struct {
	PostID uint64
	// Score 是发布时间戳（秒），用于跨来源归并排序。
	Score int64
}

// TimelineReader 承载扩散的读路径。
type TimelineReader struct {
	redisClient     redis.UniversalClient
	followingLister FollowingLister
	celebrities     *Service
	logger          *zap.Logger
	cfg             Config
}

// NewTimelineReader 创建首页信息流读取器。
//
// svc 复用写侧的 Service，只为借用其大 V 名单查询能力（celebritiesAmong）。
func NewTimelineReader(
	redisClient redis.UniversalClient,
	followingLister FollowingLister,
	svc *Service,
	logger *zap.Logger,
	cfg Config,
) *TimelineReader {
	if logger == nil {
		logger = zap.NewNop()
	}
	return &TimelineReader{
		redisClient:     redisClient,
		followingLister: followingLister,
		celebrities:     svc,
		logger:          logger,
		cfg:             cfg.withDefaults(),
	}
}

// HomeTimeline 返回读者的首页信息流（关注流）。
//
// 归并两路来源：
//
//	推路：timeline:{userID}          —— 普通作者发帖时已推送进来
//	拉路：authorbox:{celebID} × N     —— 读者关注的每个大 V 的发件箱
//
// 返回按发布时间倒序的一页 postID，以及是否还有下一页。
//
// # 深度上限
//
// 信息流只保证前 TimelineMaxItems 条可翻。超出该深度返回空页，
// 这是信息流产品的通行做法：越往后翻价值越低，而归并与内存代价线性上升。
// 需要看更早内容的场景应走「作者主页」而非首页信息流。
//
// # 复杂度
//
//	Redis 往返：1（收件箱） + 1（关注列表，通常命中缓存） + 1（批量拉发件箱的 pipeline）
//	归并：O(M log M)，M = 收件箱条数 + 各大 V 发件箱条数之和，受配置上限约束。
func (r *TimelineReader) HomeTimeline(ctx context.Context, userID uint64, offset, limit int) ([]TimelineEntry, bool, error) {
	if r == nil || r.redisClient == nil {
		return nil, false, nil
	}
	if limit <= 0 {
		limit = 20
	}
	if offset < 0 {
		offset = 0
	}

	// 超出深度上限：直接返回空页，不做任何 Redis 访问。
	if offset >= r.cfg.TimelineMaxItems {
		return nil, false, nil
	}

	// 归并前需要从每一路各取「够到 offset+limit」的量，
	// 否则靠后的页可能因为某一路取少了而漏条目。
	need := offset + limit + 1
	if need > r.cfg.TimelineMaxItems {
		need = r.cfg.TimelineMaxItems
	}

	pushed, err := r.readInbox(ctx, userID, need)
	if err != nil {
		return nil, false, err
	}

	pulled, err := r.readCelebrityBoxes(ctx, userID, need)
	if err != nil {
		// 拉路失败时降级为「只返回推路结果」，而不是整个首页报错。
		// 大 V 内容缺失是可感知的体验降级，但远好于信息流整体不可用。
		r.logger.Warn("failed to pull celebrity timelines; serving pushed entries only",
			zap.Uint64("userID", userID), zap.Error(err))
		pulled = nil
	}

	merged := mergeTimelines(pushed, pulled, need)

	if offset >= len(merged) {
		return nil, false, nil
	}
	end := offset + limit
	hasMore := end < len(merged)
	if end > len(merged) {
		end = len(merged)
	}
	return merged[offset:end], hasMore, nil
}

// readInbox 读取收件箱中最新的 need 条。
func (r *TimelineReader) readInbox(ctx context.Context, userID uint64, need int) ([]TimelineEntry, error) {
	items, err := r.redisClient.ZRevRangeWithScores(ctx, timelineKey(userID), 0, int64(need-1)).Result()
	if err != nil {
		return nil, fmt.Errorf("fanout: read inbox: %w", err)
	}
	return toEntries(items), nil
}

// readCelebrityBoxes 拉取读者关注的全部大 V 的发件箱。
func (r *TimelineReader) readCelebrityBoxes(ctx context.Context, userID uint64, need int) ([]TimelineEntry, error) {
	celebs, err := r.followedCelebrities(ctx, userID)
	if err != nil {
		return nil, err
	}
	if len(celebs) == 0 {
		return nil, nil
	}

	// 一次 pipeline 取回全部大 V 的发件箱，而不是逐个往返。
	pipe := r.redisClient.Pipeline()
	cmds := make([]*redis.ZSliceCmd, len(celebs))
	for i, id := range celebs {
		cmds[i] = pipe.ZRevRangeWithScores(ctx, authorBoxKey(id), 0, int64(need-1))
	}
	if _, err := pipe.Exec(ctx); err != nil && err != redis.Nil {
		return nil, fmt.Errorf("fanout: pull celebrity boxes: %w", err)
	}

	var out []TimelineEntry
	for _, cmd := range cmds {
		items, err := cmd.Result()
		if err != nil {
			continue // 单个大 V 的发件箱缺失不影响其余
		}
		out = append(out, toEntries(items)...)
	}
	return out, nil
}

// followedCelebrities 返回读者关注对象中的大 V，按关注时间倒序，最多 MaxPullAuthors 个。
func (r *TimelineReader) followedCelebrities(ctx context.Context, userID uint64) ([]uint64, error) {
	if r.celebrities == nil || r.followingLister == nil {
		return nil, nil
	}

	// 关注列表按关注时间倒序游标翻页，直到取完或达到上限。
	var (
		following []uint64
		cursor    int64
	)
	const pageSize = 500
	for len(following) < r.cfg.MaxPullAuthors {
		page, next, err := r.followingLister.FollowingCursor(ctx, userID, pageSize, cursor)
		if err != nil {
			return nil, fmt.Errorf("fanout: list following: %w", err)
		}
		if len(page) == 0 {
			break
		}
		following = append(following, page...)
		if next == 0 || next == cursor || len(page) < pageSize {
			break
		}
		cursor = next
	}
	if len(following) == 0 {
		return nil, nil
	}

	celebs, err := r.celebrities.celebritiesAmong(ctx, following)
	if err != nil {
		return nil, fmt.Errorf("fanout: filter celebrities: %w", err)
	}
	if len(celebs) > r.cfg.MaxPullAuthors {
		celebs = celebs[:r.cfg.MaxPullAuthors]
	}
	return celebs, nil
}

// toEntries 把 Redis 的 ZSet 结果转成条目列表。
func toEntries(items []redis.Z) []TimelineEntry {
	out := make([]TimelineEntry, 0, len(items))
	for _, z := range items {
		member, ok := z.Member.(string)
		if !ok {
			continue
		}
		id, err := strconv.ParseUint(member, 10, 64)
		if err != nil {
			continue
		}
		out = append(out, TimelineEntry{PostID: id, Score: int64(z.Score)})
	}
	return out
}

// mergeTimelines 归并推拉两路，按时间倒序去重后截断到 limit。
//
// 去重是必需的：一个作者可能在「跨过大 V 阈值」的那条帖子上既推了一部分粉丝、
// 又进了发件箱，于是同一条帖子会同时出现在两路里。
func mergeTimelines(pushed, pulled []TimelineEntry, limit int) []TimelineEntry {
	if len(pushed) == 0 && len(pulled) == 0 {
		return nil
	}

	seen := make(map[uint64]struct{}, len(pushed)+len(pulled))
	merged := make([]TimelineEntry, 0, len(pushed)+len(pulled))
	for _, src := range [][]TimelineEntry{pushed, pulled} {
		for _, e := range src {
			if _, dup := seen[e.PostID]; dup {
				continue
			}
			seen[e.PostID] = struct{}{}
			merged = append(merged, e)
		}
	}

	// 按发布时间倒序；时间相同时按 postID 倒序，保证结果稳定可分页。
	sort.Slice(merged, func(i, j int) bool {
		if merged[i].Score != merged[j].Score {
			return merged[i].Score > merged[j].Score
		}
		return merged[i].PostID > merged[j].PostID
	})

	if limit > 0 && len(merged) > limit {
		merged = merged[:limit]
	}
	return merged
}

// HomeTimelinePostIDs 返回首页信息流的一页帖子 ID（按发布时间倒序）。
//
// 这是给上层（knowpost）使用的适配方法：只暴露有序的 ID 列表，
// 使调用方无需依赖 fanout 包的内部类型，也就不会与扩散实现耦合。
func (r *TimelineReader) HomeTimelinePostIDs(ctx context.Context, userID uint64, offset, limit int) ([]uint64, bool, error) {
	entries, hasMore, err := r.HomeTimeline(ctx, userID, offset, limit)
	if err != nil {
		return nil, false, err
	}
	ids := make([]uint64, len(entries))
	for i, e := range entries {
		ids[i] = e.PostID
	}
	return ids, hasMore, nil
}
