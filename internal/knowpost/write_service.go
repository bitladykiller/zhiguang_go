package knowpost

import (
	"context"
	"encoding/json"
	"time"

	"github.com/zhiguang/app/pkg/errcode"
)

// --- [写操作] --- //

// CreateDraft 创建一篇新的知文草稿，并返回其雪花 ID。
func (s *KnowPostService) CreateDraft(creatorID uint64) (uint64, error) {
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
	if err := s.repo.InsertDraft(post); err != nil {
		return 0, err
	}
	return id, nil
}

// ConfirmContent 在用户上传内容后记录 OSS 对象元数据。
// 这里采用缓存双删策略：写入前删一次，写入后再删一次。
func (s *KnowPostService) ConfirmContent(creatorID, id uint64, objectKey, etag, sha256 string, size uint64) error {
	s.invalidateCache(id)

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

	affected, err := s.repo.UpdateContent(post)
	if err != nil {
		return err
	}
	if affected == 0 {
		return errcode.ErrNotFound.WithMsg("draft not found or permission denied")
	}

	s.invalidateCache(id)

	if s.ragIndexer != nil {
		go func() {
			if err := s.ragIndexer.EnsureIndexed(id); err != nil {
				// 这里故意采用尽力而为策略：内容写入成功不能依赖异步建索引是否成功。
			}
		}()
	}

	return nil
}

// UpdateMetadata 更新标题、标签、可见性等元数据。
// 同时会写入 outbox 事件，供搜索索引异步同步。
func (s *KnowPostService) UpdateMetadata(creatorID, id uint64, req *KnowPostPatchRequest) error {
	s.invalidateCache(id)

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

	if err := s.runKnowPostTx(id, outboxTypeKnowPostMetadataUpdated, func(txRepo *KnowPostRepository) error {
		affected, err := txRepo.UpdateMetadata(post)
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

	s.invalidateCache(id)
	s.invalidateFeedCaches(id, creatorID)
	return nil
}

// Publish 把草稿状态流转为已发布。
func (s *KnowPostService) Publish(creatorID, id uint64) error {
	if err := s.runKnowPostTx(id, outboxTypeKnowPostPublished, func(txRepo *KnowPostRepository) error {
		affected, err := txRepo.Publish(id, creatorID)
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
	s.invalidateCache(id)
	s.invalidateFeedCaches(id, creatorID)

	if s.ragIndexer != nil {
		go func() { _ = s.ragIndexer.EnsureIndexed(id) }()
	}

	return nil
}

// UpdateTop 更新置顶标记。
func (s *KnowPostService) UpdateTop(creatorID, id uint64, isTop bool) error {
	s.invalidateCache(id)
	if err := s.runKnowPostTx(id, outboxTypeKnowPostTopUpdated, func(txRepo *KnowPostRepository) error {
		affected, err := txRepo.UpdateTop(id, creatorID, isTop)
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
	s.invalidateCache(id)
	s.invalidateFeedCaches(id, creatorID)
	return nil
}

// UpdateVisibility 修改可见性设置。
func (s *KnowPostService) UpdateVisibility(creatorID, id uint64, visible string) error {
	if !isValidVisible(visible) {
		return errcode.ErrBadRequest.WithMsg("invalid visibility value")
	}
	s.invalidateCache(id)
	if err := s.runKnowPostTx(id, outboxTypeKnowPostVisibilityUpdated, func(txRepo *KnowPostRepository) error {
		affected, err := txRepo.UpdateVisibility(id, creatorID, visible)
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
	s.invalidateCache(id)
	s.invalidateFeedCaches(id, creatorID)
	return nil
}

// Delete 对知文执行软删除。
func (s *KnowPostService) Delete(creatorID, id uint64) error {
	s.invalidateCache(id)
	if err := s.runKnowPostTx(id, outboxTypeKnowPostDeleted, func(txRepo *KnowPostRepository) error {
		affected, err := txRepo.SoftDelete(id, creatorID)
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

	s.invalidateCache(id)
	s.invalidateFeedCaches(id, creatorID)
	return nil
}

func (s *KnowPostService) writeOutboxEvent(repo *KnowPostRepository, id uint64, aggType, eventType string, payloadData interface{}) error {
	outID := s.idGen.NextID()
	payload, err := json.Marshal(payloadData)
	if err != nil {
		return err
	}
	return repo.InsertOutbox(outID, aggType, &id, eventType, string(payload))
}

func (s *KnowPostService) runKnowPostTx(id uint64, eventType string, mutate func(txRepo *KnowPostRepository) error) error {
	tx, err := s.db.BeginTxx(context.Background(), nil)
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

	if err := s.writeOutboxEvent(txRepo, id, "knowpost", eventType, map[string]interface{}{
		"entity": "knowpost",
		"id":     id,
		"type":   eventType,
	}); err != nil {
		_ = tx.Rollback()
		return err
	}

	return tx.Commit()
}
