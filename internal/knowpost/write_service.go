package knowpost

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/jmoiron/sqlx"
	"github.com/redis/go-redis/v9"

	"github.com/zhiguang/app/internal/outbox"
	"github.com/zhiguang/app/pkg/contextutil"
	"github.com/zhiguang/app/pkg/errcode"
	"github.com/zhiguang/app/pkg/jsonutil"
)

// --- [写操作] --- //

const (
	// draftIdemTTL 幂等键存活时间；覆盖弱网重试窗口，过期后允许重新创建。
	draftIdemTTL = 5 * time.Minute
	// draftIdemPendingPrefix 认领占位前缀；写库完成前 key 为此形态，完成后覆盖为数字 id。
	draftIdemPendingPrefix = "pending:"
	// draftIdemWaitInterval / draftIdemMaxWait 并发请求等待「认领者」固化正式 id 的策略。
	draftIdemWaitInterval = 50 * time.Millisecond
	draftIdemMaxWait      = 3 * time.Second
)

// draftIdemCASDelScript：仅当 value 仍是本请求的 pending 标记时删除，避免误删其他请求已固化的 id。
var draftIdemCASDelScript = redis.NewScript(`
if redis.call('GET', KEYS[1]) == ARGV[1] then
  return redis.call('DEL', KEYS[1])
end
return 0
`)

// CreateDraft 创建一篇新的知文草稿，并返回其雪花算法生成的 ID。
//
// 幂等（X-Idempotency-Key 非空时）：
//  1. Redis SET NX 认领 key = idem:draft:{creator}:{key}，值为 pending:{token}
//  2. 仅认领成功者执行 InsertDraft
//  3. 写库成功后将 key 覆盖为正式 draft id（TTL 刷新）
//  4. 写库失败 CAS 删除本请求的 pending，允许重试
//  5. 认领失败：若已是正式 id 则直接返回（真幂等，无 ErrConflict）；若仍是 pending 则短暂等待
//
// WHY 不用「先 GET 再 SET」：并发下两个请求都可能 miss 并各插一条草稿。
// WHY 不用「先 Insert 再 SETNX」：两个请求都可能 Insert 成功，库内仍双写。
// WHY 不返回 ErrConflict：幂等重放应返回同一 id，由 handler 正常 201/成功响应，避免前端当失败再重试。
func (s *KnowPostService) CreateDraft(ctx context.Context, creatorID uint64, idempotencyKey string) (uint64, error) {
	var (
		idemKey       string
		pendingMarker string
		claimed       bool
	)

	if idempotencyKey != "" {
		idemKey = fmt.Sprintf("idem:draft:%d:%s", creatorID, idempotencyKey)
		// 认领 token 使用雪花 id，全局唯一且无需额外 UUID 依赖。
		pendingMarker = draftIdemPendingPrefix + strconv.FormatUint(s.idGen.NextID(), 10)

		existing, ok, err := s.claimDraftIdempotency(ctx, idemKey, pendingMarker)
		if err != nil {
			return 0, err
		}
		if ok {
			// 已有正式 draft id：幂等命中，直接返回。
			return existing, nil
		}
		claimed = true
	}

	id := s.idGen.NextID()
	now := time.Now()
	post := &KnowPost{
		ID:         id,
		CreatorID:  creatorID,
		Status:     KnowPostStatusDraft,
		Type:       "image_text",
		Visible:    KnowPostVisibilityPublic,
		IsTop:      false,
		CreateTime: now,
		UpdateTime: now,
	}
	if err := s.repo.InsertDraft(ctx, post); err != nil {
		if claimed {
			// 仅清理自己的 pending，避免误删后来请求已固化的正式 id。
			if _, delErr := draftIdemCASDelScript.Run(ctx, s.redis, []string{idemKey}, pendingMarker).Result(); delErr != nil {
				// 清理失败只记日志语义由调用方通过 insert 错误感知；不覆盖原始 err。
				_ = delErr
			}
		}
		return 0, fmt.Errorf("create draft: insert: %w", err)
	}

	if claimed {
		// 覆盖 pending 为正式 id；即使并发读到过 pending，短暂等待后也能拿到 id。
		if err := s.redis.Set(ctx, idemKey, id, draftIdemTTL).Err(); err != nil {
			// 写库已成功：幂等键失败不应导致客户端重试再插一条。
			// 返回成功 id；后续重放可能因 key 缺失再创建（短窗口风险），依赖 TTL 与重试概率。
			if s.bloom != nil {
				s.bloom.AddUint64(ctx, id)
			}
			return id, nil
		}
	}

	// 草稿创建成功即加入 Bloom：作者随后读详情不会被「一定不存在」误拦。
	if s.bloom != nil {
		s.bloom.AddUint64(ctx, id)
	}

	return id, nil
}

// claimDraftIdempotency 尝试认领幂等键。
//
// 返回：
//   - existingID, true, nil：key 已是正式 draft id，调用方应直接返回该 id
//   - 0, false, nil：本请求 SET NX 认领成功，可继续 Insert
//   - 0, false, err：等待超时 / Redis / context 错误
func (s *KnowPostService) claimDraftIdempotency(ctx context.Context, idemKey, pendingMarker string) (existingID uint64, alreadyDone bool, err error) {
	deadline := time.Now().Add(draftIdemMaxWait)
	for {
		// 1) 尝试原子认领
		ok, setErr := s.redis.SetNX(ctx, idemKey, pendingMarker, draftIdemTTL).Result()
		if setErr != nil {
			return 0, false, fmt.Errorf("create draft: claim idempotency: %w", setErr)
		}
		if ok {
			return 0, false, nil
		}

		// 2) 认领失败：读当前值
		val, getErr := s.redis.Get(ctx, idemKey).Result()
		if getErr == redis.Nil {
			// key 刚好过期，下一轮重新 SET NX
			if time.Now().After(deadline) {
				return 0, false, errcode.ErrInternal.WithMsg("create draft: idempotency claim timeout")
			}
			if !contextutil.Sleep(ctx, draftIdemWaitInterval) {
				return 0, false, ctx.Err()
			}
			continue
		}
		if getErr != nil {
			return 0, false, fmt.Errorf("create draft: get idempotency: %w", getErr)
		}

		if strings.HasPrefix(val, draftIdemPendingPrefix) {
			// 其他请求正在写库：等待其固化正式 id
			if time.Now().After(deadline) {
				return 0, false, errcode.ErrInternal.WithMsg("create draft: wait idempotency timeout")
			}
			if !contextutil.Sleep(ctx, draftIdemWaitInterval) {
				return 0, false, ctx.Err()
			}
			continue
		}

		id, parseErr := strconv.ParseUint(val, 10, 64)
		if parseErr != nil || id == 0 {
			// 脏值：删除后重试认领（仅本业务前缀 key，风险可控）
			_ = s.redis.Del(ctx, idemKey).Err()
			if time.Now().After(deadline) {
				return 0, false, errcode.ErrInternal.WithMsg("create draft: invalid idempotency value")
			}
			continue
		}
		return id, true, nil
	}
}

// ConfirmContent 在用户上传内容后记录 OSS 对象元数据。
//
// 采用"先写 DB → 后删缓存"策略：利用 read-through 缓存加载时的 Redis
// 分布式锁来保证并发场景下的串行化，避免"先删 → 写 DB → 再删"双删策略
// 的中间窗口问题。
func (s *KnowPostService) ConfirmContent(ctx context.Context, creatorID, id uint64, objectKey, etag, sha256 string, size uint64) error {
	post := &KnowPost{
		ID:               id,
		CreatorID:        creatorID,
		ContentObjectKey: &objectKey,
		ContentEtag:      &etag,
		ContentSize:      &size,
		ContentSha256:    &sha256,
		ContentURL:       jsonutil.StrPtr(s.publicURL(objectKey)),
		UpdateTime:       time.Now(),
	}

	affected, err := s.repo.UpdateContent(ctx, post)
	if err != nil {
		return fmt.Errorf("confirm content: update: %w", err)
	}
	if affected == 0 {
		return errcode.ErrNotFound.WithMsg("draft not found or permission denied")
	}

	s.invalidateCache(ctx, id)
	s.invalidateFeedCaches(ctx, id, creatorID)

	if s.auditLog != nil {
		s.auditLog.LogAction(ctx, "delete_post", int64(creatorID), "knowpost", strconv.FormatUint(id, 10), "delete knowpost")
	}

	return nil
}

// UpdateMetadata 更新标题、标签、可见性等元数据，事务内同时写入 outbox 事件。
func (s *KnowPostService) UpdateMetadata(ctx context.Context, creatorID, id uint64, req *KnowPostPatchRequest) error {
	post := &KnowPost{
		ID:         id,
		CreatorID:  creatorID,
		Title:      req.Title,
		TagID:      req.TagID,
		Visible:    visiblePtr(req.Visible),
		Type:       "image_text",
		UpdateTime: time.Now(),
	}

	if req.Tags != nil {
		post.Tags = jsonutil.StrPtr(toJSON(req.Tags))
	}
	if req.ImgUrls != nil {
		post.ImgUrls = jsonutil.StrPtr(toJSON(req.ImgUrls))
	}
	if req.Description != nil {
		post.Description = req.Description
	}

	if err := s.runKnowPostTx(ctx, id, outboxTypeKnowPostMetadataUpdated, func(txRepo Repo) error {
		affected, err := txRepo.UpdateMetadata(ctx, post)
		if err != nil {
			return err
		}
		if affected == 0 {
			return errcode.ErrNotFound.WithMsg("draft not found or permission denied")
		}
		return nil
	}); err != nil {
		return err
	}

	s.invalidateCache(ctx, id)
	s.invalidateFeedCaches(ctx, id, creatorID)
	return nil
}

// Publish 把知文状态从草稿流转为已发布。
func (s *KnowPostService) Publish(ctx context.Context, creatorID, id uint64) error {
	// 载荷追加 creator_id / published_at：扩散消费者据此决定推给谁、如何定序，
	// 无需为每条消息回查数据库。
	publishExtra := map[string]any{
		"creator_id":   creatorID,
		"published_at": time.Now().Unix(),
	}
	if err := s.runKnowPostTxWithPayload(ctx, id, outboxTypeKnowPostPublished, publishExtra, func(txRepo Repo) error {
		affected, err := txRepo.Publish(ctx, id, creatorID)
		if err != nil {
			return err
		}
		if affected == 0 {
			return errcode.ErrNotFound.WithMsg("draft not found or permission denied")
		}
		return nil
	}); err != nil {
		return err
	}
	s.invalidateCache(ctx, id)
	s.invalidateFeedCaches(ctx, id, creatorID)

	if s.auditLog != nil {
		s.auditLog.LogAction(ctx, "create_post", int64(creatorID), "knowpost", strconv.FormatUint(id, 10), "publish knowpost")
	}

	return nil
}

// UpdateTop 更新知文的置顶标记。
func (s *KnowPostService) UpdateTop(ctx context.Context, creatorID, id uint64, isTop bool) error {
	if err := s.runKnowPostTx(ctx, id, outboxTypeKnowPostTopUpdated, func(txRepo Repo) error {
		affected, err := txRepo.UpdateTop(ctx, id, creatorID, isTop)
		if err != nil {
			return err
		}
		if affected == 0 {
			return errcode.ErrNotFound.WithMsg("draft not found or permission denied")
		}
		return nil
	}); err != nil {
		return err
	}
	s.invalidateCache(ctx, id)
	s.invalidateFeedCaches(ctx, id, creatorID)
	return nil
}

// UpdateVisibility 修改知文的可见性设置。
func (s *KnowPostService) UpdateVisibility(ctx context.Context, creatorID, id uint64, visible KnowPostVisibility) error {
	if !isValidVisible(visible) {
		return errcode.ErrBadRequest.WithMsg("invalid visibility value")
	}
	// 载荷带上目标可见性：扩散消费者据此决定是否把该帖从发件箱清除
	// （转为非公开/非粉丝可见后，拉路不应再把它分发出去）。
	if err := s.runKnowPostTxWithPayload(ctx, id, outboxTypeKnowPostVisibilityUpdated, map[string]any{"visible": string(visible), "creator_id": creatorID}, func(txRepo Repo) error {
		affected, err := txRepo.UpdateVisibility(ctx, id, creatorID, visible)
		if err != nil {
			return err
		}
		if affected == 0 {
			return errcode.ErrNotFound.WithMsg("draft not found or permission denied")
		}
		return nil
	}); err != nil {
		return err
	}
	s.invalidateCache(ctx, id)
	s.invalidateFeedCaches(ctx, id, creatorID)
	return nil
}

// Delete 对知文执行软删除。
//
// 事务成功后：
//  1. CF.DEL 从 RedisBloom 过滤器移除（避免软删 ID 仍被 MightContain 放行）
//  2. 失效详情 / Feed 缓存（版本号 + NULL 仍作兜底）
func (s *KnowPostService) Delete(ctx context.Context, creatorID, id uint64) error {
	// 载荷带 creator_id：扩散消费者据此定位并清理作者发件箱。
	if err := s.runKnowPostTxWithPayload(ctx, id, outboxTypeKnowPostDeleted, map[string]any{"creator_id": creatorID}, func(txRepo Repo) error {
		affected, err := txRepo.SoftDelete(ctx, id, creatorID)
		if err != nil {
			return err
		}
		if affected == 0 {
			return errcode.ErrNotFound.WithMsg("draft not found or permission denied")
		}
		return nil
	}); err != nil {
		return err
	}

	if s.bloom != nil {
		s.bloom.DeleteUint64(ctx, id)
	}
	s.invalidateCache(ctx, id)
	s.invalidateFeedCaches(ctx, id, creatorID)
	return nil
}

// runKnowPostTx 在数据库事务中执行业务变更和 outbox 事件写入（事务性发件箱模式）。
func (s *KnowPostService) runKnowPostTx(ctx context.Context, id uint64, eventType string, mutate func(txRepo Repo) error, extraEvents ...outbox.OutboxEvent) error {
	return s.runKnowPostTxWithPayload(ctx, id, eventType, nil, mutate, extraEvents...)
}

// runKnowPostTxWithPayload 与 runKnowPostTx 相同，但允许在 outbox 载荷中追加字段。
//
// WHY 需要追加字段：
//
//	基础载荷只有 entity/id/op/type，够搜索投影用（它会自己回查详情），
//	但不够扩散用——扩散需要**作者 ID** 才知道该推给谁的粉丝，
//	需要**发布时间**才能给信息流条目定序。
//	让事件自带这些字段，消费者就不必为每条消息回查一次数据库。
//	这也是 outbox 模式的应有之义：事件应当自描述，而不是只做一个「去查吧」的通知。
func (s *KnowPostService) runKnowPostTxWithPayload(
	ctx context.Context,
	id uint64,
	eventType string,
	extra map[string]any,
	mutate func(txRepo Repo) error,
	extraEvents ...outbox.OutboxEvent,
) error {
	fields := map[string]interface{}{
		"entity": "knowpost",
		"id":     id,
		"op":     knowPostOutboxOp(eventType),
		"type":   eventType,
	}
	for k, v := range extra {
		fields[k] = v
	}
	payload, err := json.Marshal(fields)
	if err != nil {
		return fmt.Errorf("marshal outbox payload: %w", err)
	}
	baseEvent := outbox.OutboxEvent{
		ID:            s.idGen.NextID(),
		AggregateType: "knowpost",
		AggregateID:   &id,
		EventType:     eventType,
		Payload:       json.RawMessage(payload),
	}
	allEvents := append([]outbox.OutboxEvent{baseEvent}, extraEvents...)
	return outbox.RunInTx(ctx, s.db, func(tx *sqlx.Tx) error {
		return mutate(s.repo.WithDB(tx))
	}, allEvents)
}

const (
	outboxOpUpsert = "upsert"
	outboxOpDelete = "delete"
)

func knowPostOutboxOp(eventType string) string {
	if eventType == outboxTypeKnowPostDeleted {
		return outboxOpDelete
	}
	return outboxOpUpsert
}
