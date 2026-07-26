package knowpost

import (
	"context"
	"encoding/json"
	"errors"
	"strconv"
	"time"

	"github.com/redis/go-redis/v9"
	"go.uber.org/zap"

	"github.com/zhiguang/app/internal/cache"
	"github.com/zhiguang/app/pkg/config"
	"github.com/zhiguang/app/pkg/errcode"
	"github.com/zhiguang/app/pkg/jsonutil"
)

// detailEngagement 是详情读取对计数模块的**最小依赖**。
//
// WHY 在消费侧按需声明接口，而不是引用 counter 包的完整接口：
//
//	此前这里是 `type CounterClient = counter.CounterServiceInterface`——
//	把生产者侧的 13 方法胖接口整个别名过来，而详情读取只用其中 2 个。
//	代价直接体现在测试上：为了测一个 enrich，桩对象被迫实现全部 13 个方法。
//	Go 的惯用法是「接口定义在使用处，按需声明」：依赖面即真实使用面，
//	桩只需实现 2 个方法，counter 的无关演进也不再波及本模块。
type detailEngagement interface {
	GetCounts(ctx context.Context, entityType, entityID string, metrics []string) (map[string]int32, error)
	IsLikedAndFaved(ctx context.Context, userID uint64, entityType, entityID string) (liked, faved bool, err error)
}

// KnowPostDetailService 承载知文详情的读取链路。
//
// WHY 从 KnowPostService 中拆出：
//
//	拆分前 KnowPostService 同时承担写路径、详情读取、缓存协调、Bloom 维护与审计，
//	13 个依赖字段——想单测详情读取也得面对全部字段的零值语义。
//	读写路径的依赖面几乎不相交（写侧不需要 hotKey/counter，读侧不需要 db/idGen/outbox），
//	拆开后各自 8~9 个内聚依赖，构造语义与测试边界都清晰一截。
//
// 缓存结构：L1(freecache) → L2(Redis, 含 "NULL" 空值哨兵) → Bloom 预判 → 锁内回源 MySQL。
// 具体编排复用 cache.Tiered（见 internal/cache/tiered.go），本类型只提供业务件：
// 键构造、加载器、TTL 策略、热点记录与用户态叠加。
type KnowPostDetailService struct {
	repo    Repo
	redis   *redis.Client
	l1Cache *cache.PrefixCache
	hotKey  *cache.HotKeyDetector
	// bloom：第三方 RedisBloom（CF.*）适配层；nil 表示关闭，fail-open。
	bloom   *cache.RedisBloom
	counter detailEngagement
	logger  *zap.Logger
	cfg     *config.KnowPostConfig
}

// NewKnowPostDetailService 创建详情读取服务。counter/hotKey/bloom 均可为 nil（对应能力降级）。
func NewKnowPostDetailService(
	repo Repo,
	redisClient *redis.Client,
	l1Cache *cache.PrefixCache,
	hotKey *cache.HotKeyDetector,
	bloom *cache.RedisBloom,
	counter detailEngagement,
	logger *zap.Logger,
	cfg *config.KnowPostConfig,
) *KnowPostDetailService {
	if logger == nil {
		logger = zap.L()
	}
	return &KnowPostDetailService{
		repo:    repo,
		redis:   redisClient,
		l1Cache: l1Cache,
		hotKey:  hotKey,
		bloom:   bloom,
		counter: counter,
		logger:  logger,
		cfg:     cfg,
	}
}

// detailCacheParams 是详情缓存的运行时参数快照（具名结构体，取值自解释）。
type detailCacheParams struct {
	l1TTL      int // L1（freecache）TTL，秒
	nullBase   int // 空值哨兵 TTL 基准，秒
	nullJitter int // 空值哨兵 TTL 抖动上限，秒
	l2Base     int // L2（Redis）TTL 基准，秒
	l2Jitter   int // L2（Redis）TTL 抖动上限，秒
	ttlMedium  int // 热点 TTL 延长的基准档
}

// detailCacheTTLValues 返回详情缓存参数；cfg 为 nil 时使用与配置默认值同源的回退。
func (s *KnowPostDetailService) detailCacheTTLValues() detailCacheParams {
	dc := detailCacheConfig(s.cfg)
	return detailCacheParams{
		l1TTL:      dc.L1TTLSeconds,
		nullBase:   dc.NullTTLBase,
		nullJitter: dc.NullJitter,
		l2Base:     dc.L2TTLBase,
		l2Jitter:   dc.L2Jitter,
		ttlMedium:  dc.TTLMedium,
	}
}

// versions 返回详情版本号读取器（无状态，可按调用构造）。
func (s *KnowPostDetailService) versions() *cache.Versions {
	return newDetailVersions(s.redis, s.l1Cache, s.cfg)
}

// GetDetail 返回知文详情，并补充当前用户维度的点赞/收藏状态。
//
// 读取顺序（由 cache.Tiered 编排）：
//
//	L1 → L2（"NULL" 哨兵直接 404）→ Bloom 预判（一定不存在则 404）→ 锁内 double-check → MySQL
//
// 顺序要点：
//   - Bloom 放在两级缓存**之后**：缓存命中即证明存在，先问 Bloom 是白付一次往返；
//     扫号请求必然缓存 miss，到达 Bloom 的成本与放在最前完全一样。
//   - 用户态（liked/faved）在 Get 返回**之后**叠加——Tiered 保证缓存里只有共享视图，
//     该字段结构上进不了缓存（历史串号事故的结构性防复发）。
//
// 权限：public+published 任何人可看；其余仅作者本人；已删除 404。
func (s *KnowPostDetailService) GetDetail(ctx context.Context, id uint64, currentUserID *uint64) (*KnowPostDetailResponse, error) {
	p := s.detailCacheTTLValues()
	pageKey := detailPageKey(id, s.versions().Get(ctx, detailVersionKey(id)))

	// forbiddenErr 用于把 403 从 Loader 里带出来：
	// 它与「不存在」不同——不写空值哨兵（内容存在，只是请求者无权看）。
	var forbiddenErr error

	tiered := &cache.Tiered[*KnowPostDetailResponse]{
		L1:           s.l1Cache,
		Redis:        s.redis,
		Logger:       s.logger,
		Encode:       func(v *KnowPostDetailResponse) ([]byte, error) { return json.Marshal(v) },
		Decode:       parseJSON[*KnowPostDetailResponse],
		L1TTLSeconds: p.l1TTL,
		L2TTL: func() time.Duration {
			base := p.l2Base + jitterN(p.l2Jitter)
			if s.hotKey != nil {
				// 热点内容拿更长的 L2 TTL，降低周期性回源。
				base = s.hotKey.TTLForPublic(ctx, base, hotKeyID(id))
			}
			return time.Duration(base) * time.Second
		},
		NullSentinel: "NULL",
		NullTTL: func() time.Duration {
			return time.Duration(p.nullBase+jitterN(p.nullJitter)) * time.Second
		},
		PreLoad: func(ctx context.Context) error {
			// Bloom：一定不存在 → 直接 404，不打 DB。模块缺失/故障 fail-open。
			if s.bloom != nil {
				if ok, _ := s.bloom.MightContainUint64(ctx, id); !ok {
					return errcode.ErrNotFound.WithMsg("content not found")
				}
			}
			return nil
		},
		LockKey:     lockKeyFor(pageKey),
		LockOptions: knowPostLockOptions(),
		LockRetry:   knowPostLockRetryInterval,
	}

	resp, hit, err := tiered.Get(ctx, pageKey, func(ctx context.Context) (*KnowPostDetailResponse, bool, error) {
		if s.repo == nil {
			return nil, false, nil // 视为不存在：写哨兵，保持零依赖单测语义
		}
		v, loadErr := s.queryDetailFromDB(ctx, id, currentUserID)
		if loadErr != nil {
			if errors.Is(loadErr, errcode.ErrNotFound) {
				return nil, false, nil // 触发空值哨兵
			}
			forbiddenErr = loadErr // 403：不缓存，直接透传
			return nil, false, loadErr
		}
		// 回源确认存在：渐进补齐 Bloom（无需全量预热任务）。
		if s.bloom != nil {
			s.bloom.AddUint64(ctx, id)
		}
		s.fillCounts(ctx, v)
		return v, true, nil
	})

	switch {
	case errors.Is(err, cache.ErrNullCached):
		return nil, errcode.ErrNotFound.WithMsg("content not found")
	case forbiddenErr != nil:
		return nil, forbiddenErr
	case err != nil:
		return nil, err
	}

	s.recordHotKeyAndExtendTTL(ctx, id, pageKey)

	// L2 命中的载荷携带的是写缓存那一刻的计数，可能已过期 → 刷新；
	// L1 命中窗口极短（秒级）、回源刚查过 → 不刷新。
	return s.enrichDetail(ctx, resp, currentUserID, hit == cache.HitL2), nil
}

// queryDetailFromDB 回源查询并做状态/权限判定。
func (s *KnowPostDetailService) queryDetailFromDB(ctx context.Context, id uint64, currentUserID *uint64) (*KnowPostDetailResponse, error) {
	row, err := s.repo.FindDetailByID(ctx, id)
	if err != nil || row == nil || row.Status == KnowPostStatusDeleted {
		return nil, errcode.ErrNotFound.WithMsg("content not found")
	}

	isPublic := row.Status == KnowPostStatusPublished && row.Visible == KnowPostVisibilityPublic
	isOwner := currentUserID != nil && *currentUserID == row.CreatorID
	if !isPublic && !isOwner {
		return nil, errcode.ErrForbidden.WithMsg("no permission to view")
	}

	return &KnowPostDetailResponse{
		ID:             strconv.FormatUint(row.ID, 10),
		Title:          row.Title,
		Description:    row.Description,
		ContentURL:     row.ContentURL,
		Images:         jsonutil.ParseStringArray(row.ImgUrls),
		Tags:           jsonutil.ParseStringArray(row.Tags),
		AuthorID:       strconv.FormatUint(row.CreatorID, 10),
		AuthorAvatar:   row.AuthorAvatar,
		AuthorNickname: row.AuthorNickname,
		AuthorTagJSON:  row.AuthorTagJSON,
		IsTop:          row.IsTop,
		Visible:        string(row.Visible),
		Type:           row.Type,
		PublishTime:    row.PublishTime,
	}, nil
}

// fillCounts 在回源结果上填充点赞/收藏计数（进入共享缓存的部分）。
func (s *KnowPostDetailService) fillCounts(ctx context.Context, resp *KnowPostDetailResponse) {
	if s.counter == nil {
		return
	}
	counts, err := s.counter.GetCounts(ctx, "knowpost", resp.ID, []string{"like", "fav"})
	if err != nil {
		s.logger.Warn("failed to get detail counts", zap.String("knowpostID", resp.ID), zap.Error(err))
		return
	}
	resp.LikeCount = int64(counts["like"])
	resp.FavoriteCount = int64(counts["fav"])
}

// enrichDetail 在共享视图上叠加实时计数与当前用户的点赞/收藏状态。
//
// 这些字段不进缓存：计数会过期，用户态是请求维度的。
// refreshCounts 仅在 L2 命中时为 true（载荷计数可能陈旧）。
func (s *KnowPostDetailService) enrichDetail(ctx context.Context, base *KnowPostDetailResponse, currentUserID *uint64, refreshCounts bool) *KnowPostDetailResponse {
	if s.counter == nil {
		return base
	}

	if refreshCounts {
		counts, err := s.counter.GetCounts(ctx, "knowpost", base.ID, []string{"like", "fav"})
		if err != nil {
			s.logger.Warn("failed to enrich detail counts", zap.String("knowpostID", base.ID), zap.Error(err))
		} else if counts != nil {
			base.LikeCount = int64(counts["like"])
			base.FavoriteCount = int64(counts["fav"])
		}
	}

	if currentUserID != nil {
		liked, faved, err := s.counter.IsLikedAndFaved(ctx, *currentUserID, "knowpost", base.ID)
		if err != nil {
			s.logger.Warn("failed to check IsLiked/IsFaved in enrichDetail", zap.String("knowpostID", base.ID), zap.Error(err))
		} else {
			base.Liked = &liked
			base.Faved = &faved
		}
	}

	return base
}

// recordHotKeyAndExtendTTL 记录热点访问，并为热点内容延长详情与 Feed 碎片的 TTL。
//
// TTL 延长用 Lua 保证只增不减（多实例并发不会把 TTL 改短）；
// 冷键直接跳过——目标 TTL 不高于基准时延长是空操作，不值得付一次往返。
func (s *KnowPostDetailService) recordHotKeyAndExtendTTL(ctx context.Context, id uint64, pageKey string) {
	if s.hotKey == nil {
		return
	}
	s.hotKey.Record(hotKeyID(id)) // 纯本地计数，无 Redis IO

	baseTTL := s.detailCacheTTLValues().ttlMedium
	target := s.hotKey.TTLForPublic(ctx, baseTTL, hotKeyID(id))
	if target <= baseTTL {
		return
	}

	if err := extendTTLDualScript.Run(ctx, s.redis, []string{pageKey, feedItemKeyU(id)}, target).Err(); err != nil {
		s.logger.Warn("extend dual ttl failed", zap.Uint64("id", id), zap.Error(err))
	}
}

// extendTTLDualScript 原子地对两个键做「只增不减」的 TTL 延长。
var extendTTLDualScript = redis.NewScript(`
local current = redis.call('TTL', KEYS[1])
if current > 0 and current < tonumber(ARGV[1]) then
    redis.call('EXPIRE', KEYS[1], ARGV[1])
end
current = redis.call('TTL', KEYS[2])
if current > 0 and current < tonumber(ARGV[1]) then
    redis.call('EXPIRE', KEYS[2], ARGV[1])
end
return 1
`)

// WarmDetailBloom 从数据库游标扫描未删除知文 ID，批量写入 CF 过滤器。
// 预热失败不阻塞启动（fail-open）；未预热期间 Bloom 判定放行，由空值哨兵兜底。
func (s *KnowPostDetailService) WarmDetailBloom(ctx context.Context) error {
	if s == nil || s.bloom == nil || s.repo == nil {
		return nil
	}
	var lastID uint64
	total := 0
	for {
		ids, err := s.repo.ListIDsForBloom(ctx, lastID, 1000)
		if err != nil {
			return err
		}
		if len(ids) == 0 {
			break
		}
		for _, id := range ids {
			s.bloom.AddUint64(ctx, id)
		}
		total += len(ids)
		lastID = ids[len(ids)-1]
	}
	s.logger.Info("detail bloom warmed", zap.Int("count", total))
	return nil
}
