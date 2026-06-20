package knowpost

import (
	"context"
	"encoding/json"
	"time"

	"github.com/zhiguang/app/pkg/errcode"
)

// --- [写操作] --- //

// CreateDraft 创建一篇新的知文草稿，并返回其雪花算法生成的 ID。
func (s *KnowPostService) CreateDraft(ctx context.Context, creatorID uint64) (uint64, error) {
	id := s.idGen.NextID()
	now := time.Now()
	post := &KnowPost{
		ID:         id,
		CreatorID:  creatorID,
		Status:     "draft",
		Type:       "image_text",
		Visible:    "public",
		IsTop:      false,
		CreateTime: now,
		UpdateTime: now,
	}
	if err := s.repo.InsertDraft(ctx, post); err != nil {
		return 0, err
	}
	return id, nil
}

// ConfirmContent 在用户上传内容后记录 OSS 对象元数据，采用缓存双删策略。
func (s *KnowPostService) ConfirmContent(ctx context.Context, creatorID, id uint64, objectKey, etag, sha256 string, size uint64) error {
	s.invalidateCache(ctx, id)

	post := &KnowPost{
		ID:               id,
		CreatorID:        creatorID,
		ContentObjectKey: &objectKey,
		ContentEtag:      &etag,
		ContentSize:      &size,
		ContentSha256:    &sha256,
		ContentUrl:       strPtr(s.publicURL(objectKey)),
		UpdateTime:       time.Now(),
	}

	affected, err := s.repo.UpdateContent(ctx, post)
	if err != nil {
		return err
	}
	if affected == 0 {
		return errcode.ErrNotFound.WithMsg("draft not found or permission denied")
	}

	s.invalidateCache(ctx, id)

	return nil
}

// UpdateMetadata 更新标题、标签、可见性等元数据，事务内同时写入 outbox 事件。
func (s *KnowPostService) UpdateMetadata(ctx context.Context, creatorID, id uint64, req *KnowPostPatchRequest) error {
	s.invalidateCache(ctx, id)

	post := &KnowPost{
		ID:         id,
		CreatorID:  creatorID,
		Title:      req.Title,
		TagID:      req.TagID,
		Visible:    strVal(req.Visible),
		Type:       "image_text",
		UpdateTime: time.Now(),
	}

	if req.Tags != nil {
		post.Tags = strPtr(toJSON(req.Tags))
	}
	if req.ImgUrls != nil {
		post.ImgUrls = strPtr(toJSON(req.ImgUrls))
	}
	if req.Description != nil {
		post.Description = req.Description
	}

	if err := s.runKnowPostTx(ctx, id, outboxTypeKnowPostMetadataUpdated, func(txRepo *KnowPostRepository) error {
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
	if err := s.runKnowPostTx(ctx, id, outboxTypeKnowPostPublished, func(txRepo *KnowPostRepository) error {
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

	return nil
}

// UpdateTop 更新知文的置顶标记。
func (s *KnowPostService) UpdateTop(ctx context.Context, creatorID, id uint64, isTop bool) error {
	s.invalidateCache(ctx, id)
	if err := s.runKnowPostTx(ctx, id, outboxTypeKnowPostTopUpdated, func(txRepo *KnowPostRepository) error {
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
func (s *KnowPostService) UpdateVisibility(ctx context.Context, creatorID, id uint64, visible string) error {
	if !isValidVisible(visible) {
		return errcode.ErrBadRequest.WithMsg("invalid visibility value")
	}
	s.invalidateCache(ctx, id)
	if err := s.runKnowPostTx(ctx, id, outboxTypeKnowPostVisibilityUpdated, func(txRepo *KnowPostRepository) error {
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
	s.invalidateCache(ctx, id)
	if err := s.runKnowPostTx(ctx, id, outboxTypeKnowPostDeleted, func(txRepo *KnowPostRepository) error {
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

// writeOutboxEvent 在事务内写入 outbox 事件消息。
func (s *KnowPostService) writeOutboxEvent(ctx context.Context, repo *KnowPostRepository, id uint64, aggType, eventType string, payloadData interface{}) error {
	outID := s.idGen.NextID()
	payload, err := json.Marshal(payloadData)
	if err != nil {
		return err
	}
	return repo.InsertOutbox(ctx, outID, aggType, &id, eventType, string(payload))
}

// runKnowPostTx 在数据库事务中执行业务变更和 outbox 事件写入（Transactional Outbox Pattern）。
func (s *KnowPostService) runKnowPostTx(ctx context.Context, id uint64, eventType string, mutate func(txRepo *KnowPostRepository) error) error {
	tx, err := s.db.BeginTxx(ctx, nil)
	if err != nil {
		return err
	}
	txRepo := s.repo.WithDB(tx)
	defer func() {
		if r := recover(); r != nil {
			_ = tx.Rollback()
		}
	}()

	if err := mutate(txRepo); err != nil {
		_ = tx.Rollback()
		return err
	}

	if err := s.writeOutboxEvent(ctx, txRepo, id, "knowpost", eventType, map[string]interface{}{
		"entity": "knowpost",
		"id":     id,
		"op":     knowPostOutboxOp(eventType),
		"type":   eventType,
	}); err != nil {
		_ = tx.Rollback()
		return err
	}

	return tx.Commit()
}

func knowPostOutboxOp(eventType string) string {
	if eventType == outboxTypeKnowPostDeleted {
		return "delete"
	}
	return "upsert"
}
