package fanout

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/segmentio/kafka-go"
	"go.uber.org/zap"

	"github.com/zhiguang/app/internal/model"
	"github.com/zhiguang/app/internal/outbox"
)

// 知文事件在 outbox 中的类型标识（与 knowpost 包内常量保持一致）。
const (
	knowPostPublishedType         = "KnowPostPublished"
	knowPostDeletedType           = "KnowPostDeleted"
	knowPostVisibilityUpdatedType = "KnowPostVisibilityUpdated"
)

// Consumer 消费 canal-outbox 主题，把知文发布事件转成扩散动作。
//
// WHY 订阅 canal-outbox 而不是独立的 fanout 主题：
//
//	原设计是「发帖时由 FanoutPublisher 直接写 fanout 主题」。这有两个问题：
//
//	1. **它从未被接线**——NewFanoutPublisher 在整个生产代码里没有任何调用方，
//	   于是 fanout 主题永远没有生产者，收件箱恒为空，写扩散形同虚设。
//	2. 即使接上，它也是一次**双写**：数据库事务提交 + Kafka 投递是两个独立操作，
//	   进程在两者之间崩溃就会丢事件，粉丝永久看不到那条帖子。
//
//	改为复用已有的事务性 outbox 链路（写库与写 outbox 在同一事务内，
//	再由 Canal 捕获 binlog 投递到 Kafka），投递保证与搜索投影完全一致，
//	不需要为扩散单独维护一套可靠性机制。
type Consumer struct {
	inner *outbox.Consumer
}

// NewConsumer 创建扩散消费者。
//
// reader 应订阅 outbox.CanalOutboxTopic，并使用独立的消费者组，
// 这样扩散与搜索、关系投影各自独立推进位点，互不阻塞。
func NewConsumer(reader *kafka.Reader, service *Service, logger *zap.Logger) *Consumer {
	if reader == nil || service == nil {
		return nil
	}
	if logger == nil {
		logger = zap.NewNop()
	}
	handler := &publishRowHandler{service: service, logger: logger}
	return &Consumer{inner: outbox.NewConsumer(reader, handler, logger)}
}

// SetFailedMessageRecorder 注入死信记录（透传给内部的 outbox.Consumer）。
func (c *Consumer) SetFailedMessageRecorder(r outbox.FailedMessageRecorder) {
	if c != nil && c.inner != nil {
		c.inner.SetFailedMessageRecorder(r)
	}
}

// Start 阻塞消费直到 ctx 取消。
//
// 消费循环由 outbox.Consumer 提供，它逐条拉取并提交。
// 原先的自研循环有一个隐蔽缺陷：它固定 `for i := 0; i < 100; i++ { FetchMessage }`
// 攒满 100 条才处理，而 FetchMessage 是阻塞调用——
// 低流量时消息会一直卡在缓冲里不被处理，信息流迟迟不更新。
func (c *Consumer) Start(ctx context.Context) {
	if c == nil || c.inner == nil {
		return
	}
	c.inner.Start(ctx)
}

// String 让监督器能给出可读的任务名。
func (c *Consumer) String() string { return "fanout-consumer" }

// publishRowHandler 从 outbox 行中挑出知文发布事件并触发扩散。
type publishRowHandler struct {
	service *Service
	logger  *zap.Logger
}

// publishPayload 是知文事件的载荷结构（发布/删除/可见性共用超集）。
type publishPayload struct {
	Entity      string `json:"entity"`
	Type        string `json:"type"`
	ID          uint64 `json:"id"`
	CreatorID   uint64 `json:"creator_id"`
	PublishedAt int64  `json:"published_at"`
	Visible     string `json:"visible"`
}

// HandleRow 处理单条 outbox 行。
//
// 非发布事件直接放行（返回 nil 即提交位点），因为 canal-outbox 是多消费者共享主题，
// 里面混有关系、搜索等各类事件。
func (h *publishRowHandler) HandleRow(ctx context.Context, row outbox.Row) error {
	switch row.Type {
	case knowPostPublishedType, knowPostDeletedType, knowPostVisibilityUpdatedType:
	default:
		return nil
	}
	if len(row.Payload) == 0 {
		return nil
	}

	var p publishPayload
	if err := json.Unmarshal(row.Payload, &p); err != nil {
		// 载荷损坏无法通过重试修复，记日志后放行，避免卡住整个分区。
		h.logger.Warn("fanout: malformed publish payload, skipping",
			zap.String("aggregateID", row.AggregateID), zap.Error(err))
		return nil
	}
	if p.ID == 0 || p.CreatorID == 0 {
		h.logger.Warn("fanout: publish payload missing id or creator_id, skipping",
			zap.String("aggregateID", row.AggregateID))
		return nil
	}

	// 删除 / 可见性收紧：清理发件箱，拉路不再分发该帖。
	//
	// 收件箱副本无法穷举清理（粉丝可能极多），由读路径的 FindByIDs 过滤兜底渲染；
	// 发件箱是拉路与关注回填的数据源，必须清干净，否则死 ID 持续占据
	// AuthorBoxMaxItems 的槽位、还会被回填复制进新粉丝的收件箱。
	if row.Type == knowPostDeletedType || (row.Type == knowPostVisibilityUpdatedType && !visibleInFeeds(p.Visible)) {
		if p.CreatorID == 0 {
			h.logger.Debug("fanout: removal event without creator_id (pre-feature), skipping",
				zap.String("aggregateID", row.AggregateID))
			return nil
		}
		if err := h.service.RemovePost(ctx, p.CreatorID, p.ID); err != nil {
			return fmt.Errorf("fanout: remove post %d from author box: %w", p.ID, err)
		}
		return nil
	}
	if row.Type == knowPostVisibilityUpdatedType {
		return nil // 转公开/粉丝可见：不主动补录（缺发布时间），由后续行为自然收敛
	}

	if p.PublishedAt <= 0 {
		// 无发布时间的事件必然产生于本功能上线之前（新事件的载荷总带该字段）。
		// 历史帖子不应再被扩散——早期这里用“当前时间”兜底，叠加新消费者组
		// 默认从最早位点回放，会把陈年旧帖以“现在”的时间戳刷满所有人的收件箱。
		h.logger.Debug("fanout: pre-feature event without published_at, skipping",
			zap.String("aggregateID", row.AggregateID))
		return nil
	}

	event := &model.FanoutEvent{
		PostID:    p.ID,
		CreatorID: p.CreatorID,
		CreatedAt: p.PublishedAt,
	}
	if err := h.service.FanoutPost(ctx, event); err != nil {
		return fmt.Errorf("fanout: handle published post %d: %w", p.ID, err)
	}
	return nil
}

// visibleInFeeds 判定某可见性等级是否仍应被信息流分发。
func visibleInFeeds(visible string) bool {
	return visible == "public" || visible == "followers"
}
