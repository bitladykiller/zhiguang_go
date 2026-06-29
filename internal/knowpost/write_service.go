package knowpost

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"time"

	"github.com/jmoiron/sqlx"

	"github.com/zhiguang/app/internal/outbox"
	"github.com/zhiguang/app/pkg/errcode"
	"github.com/zhiguang/app/pkg/jsonutil"
)

// --- [写操作] --- //

// CreateDraft 创建一篇新的知文草稿，并返回其雪花算法生成的 ID。
func (s *KnowPostService) CreateDraft(ctx context.Context, creatorID uint64) (uint64, error) {
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
		return 0, fmt.Errorf("create draft: insert: %w", err)
	}
	return id, nil
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
		ContentUrl:       jsonutil.StrPtr(s.publicURL(objectKey)),
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
	if err := s.runKnowPostTx(ctx, id, outboxTypeKnowPostPublished, func(txRepo Repo) error {
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
	if err := s.runKnowPostTx(ctx, id, outboxTypeKnowPostVisibilityUpdated, func(txRepo Repo) error {
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
func (s *KnowPostService) Delete(ctx context.Context, creatorID, id uint64) error {
	if err := s.runKnowPostTx(ctx, id, outboxTypeKnowPostDeleted, func(txRepo Repo) error {
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

	s.invalidateCache(ctx, id)
	s.invalidateFeedCaches(ctx, id, creatorID)
	return nil
}

// runKnowPostTx 在数据库事务中执行业务变更和 outbox 事件写入（事务性发件箱模式）。
func (s *KnowPostService) runKnowPostTx(ctx context.Context, id uint64, eventType string, mutate func(txRepo Repo) error, extraEvents ...outbox.OutboxEvent) error {
	payload, err := json.Marshal(map[string]interface{}{
		"entity": "knowpost",
		"id":     id,
		"op":     knowPostOutboxOp(eventType),
		"type":   eventType,
	})
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

func knowPostOutboxOp(eventType string) string {
	if eventType == outboxTypeKnowPostDeleted {
		return "delete"
	}
	return "upsert"
}
