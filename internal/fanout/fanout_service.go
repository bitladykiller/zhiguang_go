// Package fanout 实现「推拉结合」（Hybrid Fan-out）的信息流扩散。
//
// # 三种扩散模型
//
// 假设作者 A 有 F 个粉丝，读者 U 关注了 N 个人。
//
//	读扩散（Pull / Fan-out on read）
//	  写：只写作者自己的发件箱，O(1)。
//	  读：读者读取时遍历自己关注的 N 个人的发件箱并归并，O(N)。
//	  优点：写入极轻，发帖不产生放大；关注/取关立即生效，无需清理历史。
//	  缺点：读延迟随关注数增长；读是高频操作，等于把成本压在最热的路径上。
//
//	写扩散（Push / Fan-out on write）
//	  写：发帖时把 postID 推进全部 F 个粉丝的收件箱，O(F)。
//	  读：读者只读自己的收件箱，O(1)。
//	  优点：读极快，天然支持分页。
//	  缺点：写放大随粉丝数线性增长。一个百万粉作者发一条帖 = 百万次写入，
//	        且这些写入必须在可接受的延迟内完成，否则粉丝看不到新内容。
//
//	推拉结合（Hybrid，本项目采用）
//	  按作者粉丝数分流：普通作者走推，大 V 走拉。
//	  读者的首页 = 自己的收件箱（推来的） ⊕ 所关注大 V 的发件箱（拉来的），归并排序。
//	  这样写放大被 CelebrityThreshold 封顶，读成本被「关注的大 V 数」封顶，
//	  而这两个量在真实社区里都很小。
//
// # 为什么必须是混合方案
//
// 纯写扩散在大 V 场景下不可行——这正是本项目此前的状态：
// 粉丝数超过上限就**整条跳过扩散**，且没有任何读路径兜底，
// 结果是大 V 的帖子永远到不了任何粉丝的信息流里。
// 纯读扩散则会让每次刷新首页都要归并几百个发件箱，读延迟不可控。
//
// # 数据流
//
//	发布知文（事务内写 outbox）
//	  → Canal 捕获 binlog → Kafka canal-outbox
//	  → fanout.Consumer 过滤 knowpost.published
//	  → Service.FanoutPost：写发件箱（必做） + 按需推收件箱
//	读取首页
//	  → TimelineReader.HomeTimeline：收件箱 ⊕ 大 V 发件箱 → 归并 → 分页
package fanout

import (
	"context"
	"fmt"
	"strconv"

	"github.com/redis/go-redis/v9"
	"go.uber.org/zap"

	"github.com/zhiguang/app/internal/model"
	"github.com/zhiguang/app/pkg/metrics"
)

// FollowerLister 提供按游标分页的粉丝列表。
//
// 用游标而非 offset：写扩散要遍历的是一个持续变动的集合，
// offset 分页在集合变化时会漏读或重读。游标为不透明串（relation 的复合游标），
// 空串表示第一页；本模块只负责原样回传，不解释其内容。
type FollowerLister interface {
	FollowersCursor(ctx context.Context, userID uint64, limit int, cursor string) ([]uint64, string, error)
}

// FollowingLister 提供按游标分页的关注列表，供读路径求「我关注的大 V」。
type FollowingLister interface {
	FollowingCursor(ctx context.Context, userID uint64, limit int, cursor string) ([]uint64, string, error)
}

// Service 承载扩散的写路径。
type Service struct {
	redisClient    redis.UniversalClient
	followerLister FollowerLister
	celebrities    *CelebrityRegistry
	logger         *zap.Logger
	cfg            Config
}

// NewService 创建扩散服务。
//
// followerCounter 可以为 nil：此时大 V 判定完全走「边推边数」的慢路径，
// 行为仍然正确，只是每次都要开始遍历粉丝才能发现对方是大 V。
func NewService(
	redisClient redis.UniversalClient,
	followerLister FollowerLister,
	followerCounter FollowerCounter,
	logger *zap.Logger,
	cfg Config,
) *Service {
	if logger == nil {
		logger = zap.NewNop()
	}
	cfg = cfg.withDefaults()
	return &Service{
		redisClient:    redisClient,
		followerLister: followerLister,
		celebrities:    NewCelebrityRegistry(redisClient, followerCounter, cfg.CelebrityThreshold, logger),
		logger:         logger,
		cfg:            cfg,
	}
}

// Celebrities 暴露大 V 名单，供读路径（TimelineReader）共享同一份判定逻辑。
func (s *Service) Celebrities() *CelebrityRegistry {
	if s == nil {
		return nil
	}
	return s.celebrities
}

// FanoutPost 处理一条新发布的知文。
//
// 执行顺序（顺序本身很重要）：
//
//  1. **先写发件箱**。这一步无条件执行且是 O(1)。
//     即使随后的推送失败或作者是大 V，读者仍能通过拉路看到这条帖子——
//     发件箱是内容可达性的兜底。
//  2. 判定作者是否大 V；是则直接结束（读者会拉）。
//  3. 否则游标遍历粉丝并分批推送，**边推边数**：
//     累计触达超过阈值即中止推送、把作者补记为大 V，后续帖子自动走拉路。
//
// 幂等性：ZADD 对同一 member 是覆盖语义，重复消费同一条消息不会产生重复条目，
// 因此本方法可以安全地被 Kafka 重投。
func (s *Service) FanoutPost(ctx context.Context, event *model.FanoutEvent) error {
	if s == nil || s.redisClient == nil || event == nil {
		return nil
	}

	// 1) 发件箱：拉路的数据来源，也是取关清理的依据。
	if err := s.writeAuthorBox(ctx, event); err != nil {
		return fmt.Errorf("fanout: write author box: %w", err)
	}

	// 2) 大 V 直接走拉，不做任何推送。
	celebrity, known := s.celebrities.IsCelebrity(ctx, event.CreatorID)
	if known && celebrity {
		metrics.FanoutPostsTotal.WithLabelValues("pull").Inc()
		s.logger.Debug("fanout skipped for celebrity author; readers will pull",
			zap.Uint64("authorID", event.CreatorID), zap.Uint64("postID", event.PostID))
		return nil
	}

	// 3) 普通作者：推送到粉丝收件箱。
	return s.pushToFollowers(ctx, event)
}

// writeAuthorBox 把帖子写入作者发件箱，并裁剪到配置的长度上限。
func (s *Service) writeAuthorBox(ctx context.Context, event *model.FanoutEvent) error {
	key := authorBoxKey(event.CreatorID)
	member := strconv.FormatUint(event.PostID, 10)

	pipe := s.redisClient.Pipeline()
	pipe.ZAdd(ctx, key, redis.Z{Score: float64(event.CreatedAt), Member: member})
	// 只保留最近 AuthorBoxMaxItems 条：ZSet 按 score 升序，负 rank 从末尾计数，
	// 因此删除 [0, -(N+1)] 即删掉除最新 N 条以外的全部。
	pipe.ZRemRangeByRank(ctx, key, 0, int64(-s.cfg.AuthorBoxMaxItems-1))
	pipe.Expire(ctx, key, s.cfg.AuthorBoxTTL)
	_, err := pipe.Exec(ctx)
	return err
}

// pushToFollowers 把帖子推送到全部粉丝的收件箱，并在触达量超阈值时转为拉模式。
func (s *Service) pushToFollowers(ctx context.Context, event *model.FanoutEvent) error {
	member := strconv.FormatUint(event.PostID, 10)
	score := float64(event.CreatedAt)

	var (
		cursor  string
		reached int
	)

	for {
		fans, next, err := s.followerLister.FollowersCursor(ctx, event.CreatorID, s.cfg.FanoutBatchSize, cursor)
		if err != nil {
			return fmt.Errorf("fanout: list followers: %w", err)
		}
		if len(fans) == 0 {
			break
		}

		if err := s.pushBatch(ctx, fans, member, score); err != nil {
			return err
		}
		reached += len(fans)
		metrics.FanoutPushedFollowersTotal.Add(float64(len(fans)))

		// 边推边数：真实触达量超过阈值 → 该作者其实是大 V。
		// 已推出去的部分不需要回滚（收件箱里多几条是无害的），
		// 从此刻起停止推送，并把作者补记进名单，后续帖子直接走拉路。
		if reached >= s.cfg.CelebrityThreshold {
			s.celebrities.Mark(ctx, event.CreatorID)
			metrics.FanoutPostsTotal.WithLabelValues("promoted").Inc()
			s.logger.Info("author crossed the celebrity threshold during fanout; switching to pull",
				zap.Uint64("authorID", event.CreatorID),
				zap.Int("reached", reached),
				zap.Int("threshold", s.cfg.CelebrityThreshold),
			)
			return nil
		}

		// 兜底保护：粉丝计数失真等异常情况下，防止单条消息拖垮消费者。
		if reached >= s.cfg.FanoutMaxFans {
			s.celebrities.Mark(ctx, event.CreatorID)
			metrics.FanoutPostsTotal.WithLabelValues("guard").Inc()
			s.logger.Warn("fanout hit the max-fans guard; author marked as celebrity",
				zap.Uint64("authorID", event.CreatorID), zap.Int("reached", reached))
			return nil
		}

		if next == "" || next == cursor || len(fans) < s.cfg.FanoutBatchSize {
			break // 游标没有推进或已取完最后一页
		}
		cursor = next
	}

	metrics.FanoutPostsTotal.WithLabelValues("push").Inc()
	s.logger.Debug("fanout pushed to followers",
		zap.Uint64("postID", event.PostID),
		zap.Uint64("authorID", event.CreatorID),
		zap.Int("followers", reached),
	)
	return nil
}

// pushBatch 用一次 pipeline 把帖子写入一批粉丝的收件箱。
func (s *Service) pushBatch(ctx context.Context, fans []uint64, member string, score float64) error {
	pipe := s.redisClient.Pipeline()
	for _, fanID := range fans {
		key := timelineKey(fanID)
		pipe.ZAdd(ctx, key, redis.Z{Score: score, Member: member})
		pipe.ZRemRangeByRank(ctx, key, 0, int64(-s.cfg.TimelineMaxItems-1))
		pipe.Expire(ctx, key, s.cfg.TimelineTTL)
	}
	if _, err := pipe.Exec(ctx); err != nil {
		return fmt.Errorf("fanout: push batch: %w", err)
	}
	return nil
}

// RemovePost 从发件箱与指定粉丝的收件箱中移除一条帖子。
//
// 用于知文被删除或转为不可见时清理扩散痕迹。
// 收件箱的清理无法穷举（粉丝可能极多），因此这里只保证发件箱一定被清干净——
// 读路径会在批量取详情时过滤掉已删除的帖子，收件箱中的残留条目不会被渲染出来。
func (s *Service) RemovePost(ctx context.Context, authorID, postID uint64) error {
	if s == nil || s.redisClient == nil {
		return nil
	}
	member := strconv.FormatUint(postID, 10)
	if err := s.redisClient.ZRem(ctx, authorBoxKey(authorID), member).Err(); err != nil {
		return fmt.Errorf("fanout: remove post from author box: %w", err)
	}
	return nil
}
