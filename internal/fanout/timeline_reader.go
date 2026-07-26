package fanout

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strconv"
	"strings"

	"github.com/redis/go-redis/v9"
	"go.uber.org/zap"

	"github.com/zhiguang/app/pkg/metrics"
)

// TimelineEntry 是归并后的一条信息流条目。
type TimelineEntry struct {
	PostID uint64
	// Score 是发布时间戳（秒），用于跨来源归并排序与游标定位。
	Score int64
}

// TimelineReader 承载扩散的读路径。
//
// 依赖的是 CelebrityRegistry（读写路径共享的名单概念），而非写侧的 *Service——
// 此前 Reader 为了借一个查询方法持有整个 Service，读路径假性依赖了写路径的全部依赖。
type TimelineReader struct {
	redisClient     redis.UniversalClient
	followingLister FollowingLister
	celebrities     *CelebrityRegistry
	logger          *zap.Logger
	cfg             Config
}

// NewTimelineReader 创建首页信息流读取器。
func NewTimelineReader(
	redisClient redis.UniversalClient,
	followingLister FollowingLister,
	celebrities *CelebrityRegistry,
	logger *zap.Logger,
	cfg Config,
) *TimelineReader {
	if logger == nil {
		logger = zap.NewNop()
	}
	return &TimelineReader{
		redisClient:     redisClient,
		followingLister: followingLister,
		celebrities:     celebrities,
		logger:          logger,
		cfg:             cfg.withDefaults(),
	}
}

// ── 游标 ────────────────────────────────────────────────────────────────────
//
// 信息流是**动态列表**：offset 分页在翻页期间有新帖插入时会跳条或重条。
// 游标以 (发布时间, postID) 双键定位，格式 "t:{score}:{postID}"，客户端不透明回传。
// 携带 postID 是为了并列时间戳（同一秒多条）下翻页不重不漏——
// 与 counter likers 的游标采用同一约定。

type timelineCursor struct {
	set    bool
	score  int64
	postID uint64
}

func encodeTimelineCursor(e TimelineEntry) string {
	return "t:" + strconv.FormatInt(e.Score, 10) + ":" + strconv.FormatUint(e.PostID, 10)
}

func parseTimelineCursor(raw string) (timelineCursor, error) {
	if raw == "" {
		return timelineCursor{}, nil
	}
	body, ok := strings.CutPrefix(raw, "t:")
	if !ok {
		return timelineCursor{}, errors.New("invalid cursor")
	}
	tsStr, idStr, ok := strings.Cut(body, ":")
	if !ok {
		return timelineCursor{}, errors.New("invalid cursor")
	}
	ts, err1 := strconv.ParseInt(tsStr, 10, 64)
	id, err2 := strconv.ParseUint(idStr, 10, 64)
	if err1 != nil || err2 != nil {
		return timelineCursor{}, errors.New("invalid cursor")
	}
	return timelineCursor{set: true, score: ts, postID: id}, nil
}

// after 判断条目是否严格位于游标之后（时间倒序方向）。
func (c timelineCursor) after(e TimelineEntry) bool {
	if !c.set {
		return true
	}
	if e.Score != c.score {
		return e.Score < c.score
	}
	return e.PostID < c.postID // 同秒内按 postID 倒序，与归并排序一致
}

// tieSlack 是并列时间戳的取数冗余：同一秒内的条目可能横跨页边界。
const tieSlack = 16

// HomeTimeline 返回读者的首页信息流（关注流），游标分页。
//
// 归并两路来源：
//
//	推路：timeline:{userID}        —— 普通作者发帖时已推送进来
//	拉路：authorbox:{celebID} × N  —— 读者关注的每个大 V 的发件箱
//
// 深度上限：整个信息流最多可翻 TimelineMaxItems 条（收件箱的保留长度），
// 这是信息流产品的通行做法；更早的内容走作者主页。
//
// 拉路失败降级为只返回推路结果：大 V 内容缺失是可感知的体验降级，
// 但远好于整个首页不可用。
func (r *TimelineReader) HomeTimeline(ctx context.Context, userID uint64, cursor string, limit int) ([]TimelineEntry, string, bool, error) {
	if r == nil || r.redisClient == nil {
		return nil, "", false, nil
	}
	if limit <= 0 {
		limit = 20
	}
	cur, err := parseTimelineCursor(cursor)
	if err != nil {
		return nil, "", false, err
	}

	// 每路取到 limit+1（判 hasMore）+ 并列冗余即可：来源各自按时间有序，
	// 游标之后的前 limit 条必然落在各来源“游标之后的前 limit+slack 条”里。
	need := limit + 1 + tieSlack

	pushed, err := r.readInbox(ctx, userID, cur, need)
	if err != nil {
		return nil, "", false, err
	}

	pulled, err := r.readCelebrityBoxes(ctx, userID, cur, need)
	if err != nil {
		r.logger.Warn("failed to pull celebrity timelines; serving pushed entries only",
			zap.Uint64("userID", userID), zap.Error(err))
		pulled = nil
	}

	merged := mergeTimelines(pushed, pulled, 0)

	// 游标过滤（含并列 score 的精确跳过），再截页。
	page := make([]TimelineEntry, 0, limit+1)
	for _, e := range merged {
		if !cur.after(e) {
			continue
		}
		page = append(page, e)
		if len(page) == limit+1 {
			break
		}
	}

	hasMore := len(page) > limit
	if hasMore {
		page = page[:limit]
	}
	next := ""
	if len(page) > 0 {
		next = encodeTimelineCursor(page[len(page)-1])
	}
	return page, next, hasMore, nil
}

// rangeMax 返回 ZRevRangeByScore 的上界：游标未设为 +inf，否则**包含**游标 score
// （并列成员靠应用侧 after() 精确跳过；排除式上界会把并列成员整页吞掉）。
func (c timelineCursor) rangeMax() string {
	if !c.set {
		return "+inf"
	}
	return strconv.FormatInt(c.score, 10)
}

// readInbox 读取收件箱中游标之后的至多 need 条。
func (r *TimelineReader) readInbox(ctx context.Context, userID uint64, cur timelineCursor, need int) ([]TimelineEntry, error) {
	items, err := r.redisClient.ZRevRangeByScoreWithScores(ctx, timelineKey(userID), &redis.ZRangeBy{
		Min: "-inf", Max: cur.rangeMax(), Offset: 0, Count: int64(need),
	}).Result()
	if err != nil {
		return nil, fmt.Errorf("fanout: read inbox: %w", err)
	}
	return toEntries(items), nil
}

// readCelebrityBoxes 拉取读者关注的全部大 V 的发件箱（一次 pipeline）。
func (r *TimelineReader) readCelebrityBoxes(ctx context.Context, userID uint64, cur timelineCursor, need int) ([]TimelineEntry, error) {
	celebs, err := r.followedCelebrities(ctx, userID)
	if err != nil {
		return nil, err
	}
	if len(celebs) == 0 {
		return nil, nil
	}

	metrics.HomeTimelinePulledAuthorsTotal.Add(float64(len(celebs)))
	pipe := r.redisClient.Pipeline()
	cmds := make([]*redis.ZSliceCmd, len(celebs))
	for i, id := range celebs {
		cmds[i] = pipe.ZRevRangeByScoreWithScores(ctx, authorBoxKey(id), &redis.ZRangeBy{
			Min: "-inf", Max: cur.rangeMax(), Offset: 0, Count: int64(need),
		})
	}
	if _, err := pipe.Exec(ctx); err != nil && !errors.Is(err, redis.Nil) {
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

// followingScanPageSize 是扫描关注列表时的单页大小。
const followingScanPageSize = 500

// followingScanMaxPages 是扫描页数上限（500×40 = 2 万关注，远超产品常规上限），
// 用作防御性护栏而非功能限制；触顶会打 Warn。
const followingScanMaxPages = 40

// followedCelebrities 返回读者关注对象中的大 V，最多 MaxPullAuthors 个。
//
// **扫描覆盖全部关注列表**。早期实现的循环条件在收集到 MaxPullAuthors 个
// *关注对象*（而非大 V）后就停止，实际只检查了最近关注的一页——
// 关注超过 500 人且大 V 是早年关注的用户，那些大 V 的内容会从首页凭空消失。
// MaxPullAuthors 只应约束**产出**（要拉多少个大 V），不应约束**扫描范围**。
//
// 另一个隐含上界来自数据源：relation 的关注 ZSet 冷启动预热最多写入
// ZSetWarmLimit（默认 2000）条——关注超过该数的用户，更早的关注对象
// 不在 ZSet 里，本扫描自然也看不到。这是 relation 侧的容量取舍，非本函数缺陷。
func (r *TimelineReader) followedCelebrities(ctx context.Context, userID uint64) ([]uint64, error) {
	if r.celebrities == nil || r.followingLister == nil {
		return nil, nil
	}

	celebs := make([]uint64, 0, 8)
	var cursor string
	for page := 0; page < followingScanMaxPages; page++ {
		batch, next, err := r.followingLister.FollowingCursor(ctx, userID, followingScanPageSize, cursor)
		if err != nil {
			return nil, fmt.Errorf("fanout: list following: %w", err)
		}
		if len(batch) == 0 {
			return celebs, nil
		}

		found, err := r.celebrities.Among(ctx, batch)
		if err != nil {
			return nil, fmt.Errorf("fanout: filter celebrities: %w", err)
		}
		for _, id := range found {
			celebs = append(celebs, id)
			if len(celebs) >= r.cfg.MaxPullAuthors {
				return celebs, nil
			}
		}

		if next == "" || next == cursor || len(batch) < followingScanPageSize {
			return celebs, nil
		}
		cursor = next
	}

	r.logger.Warn("following list scan hit the page guard; celebrities beyond it are not pulled",
		zap.Uint64("userID", userID),
		zap.Int("scanned", followingScanMaxPages*followingScanPageSize),
	)
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

// mergeTimelines 归并推拉两路：去重 → 按 (时间, postID) 双键倒序 → 截断到 limit（0 表示不截）。
//
// 去重是必需的：作者跨越大 V 阈值的那条帖子会既推给部分粉丝、又进发件箱。
// 双键排序保证并列时间戳下结果稳定可分页。
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

// HomeTimelinePage 返回一页帖子 ID 及下一页游标（knowpost 消费的窄形态）。
//
// 边界上保留 score（发布时间）直到编码进游标为止——早期版本在此处把 score 丢弃、
// 只返回 ID 列表，上层因此只能做 offset 分页；信息在抽象边界上被提前扔掉了。
func (r *TimelineReader) HomeTimelinePage(ctx context.Context, userID uint64, cursor string, limit int) (ids []uint64, nextCursor string, hasMore bool, err error) {
	entries, next, more, err := r.HomeTimeline(ctx, userID, cursor, limit)
	if err != nil {
		return nil, "", false, err
	}
	ids = make([]uint64, len(entries))
	for i, e := range entries {
		ids[i] = e.PostID
	}
	return ids, next, more, nil
}
