package knowpost

import (
	"context"
	"encoding/json"
	"time"

	"github.com/zhiguang/app/pkg/errcode"
)

// --- [写操作] --- //

// CreateDraft 创建一篇新的知文草稿，并返回其雪花算法生成的 ID。
//
// 功能：生成一个唯一的雪花 ID，填充默认字段后写入数据库。
//
// 草稿的初始状态：
//   - Status: "draft"（草稿状态，尚未发布）
//   - Type: "image_text"（图文类型，当前仅支持这一种知文类型）
//   - Visible: "public"（默认公开可见）
//   - IsTop: false（默认不置顶）
//   - CreateTime/UpdateTime: 当前时间
//
// 边界情况：
//   - InsertDraft 失败：返回 0 和错误。不创建草稿 ID 不回滚（尚无操作可回滚）。
//
// 参数：
//   - creatorID: uint64，创建者（作者）的用户 ID。
//
// 返回值：
//   - uint64: 新创建的知文雪花 ID。
//   - error: 数据库写入失败时的错误。
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
//
// 功能：用户在客户端将文件直传到 OSS 后，调用此接口将 OSS 对象的元数据
// （objectKey、etag、sha256、size）持久化到数据库，并生成公开访问地址（content_url）。
//
// 这里采用缓存双删策略（Cache-Aside pattern with double invalidation）：
//   1. 写入前先删一次缓存（invalidateCache）。
//   2. 执行 UPDATE。
//   3. 写入后再删一次缓存。
// 这种做法可以确保在并发读写场景下，不会发生「写入线程刚清空缓存，
// 读取线程就加载了旧数据并回填」的时序竞争问题。
//
// 参数：
//   - creatorID: uint64，作者用户 ID（用于鉴权）。
//   - id: uint64，知文 ID。
//   - objectKey: string，OSS 对象键。
//   - etag: string，OSS 对象的 ETag（MD5 或 其它哈希）。
//   - sha256: string，OSS 对象的 SHA256 哈希值。
//   - size: uint64，OSS 对象的大小（字节）。
//
// 返回值：
//   - error: 如果 affected == 0（即未找到对应的草稿或无权修改）则返回 ErrNotFound；
//     如果 UpdateContent 失败则返回原始错误。
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

	return nil
}

// UpdateMetadata 更新标题、标签、可见性等元数据。
//
// 功能：更新知文的核心元数据字段（标题、标签、图片、描述等），
// 同时会写入 outbox 事件，供搜索索引异步同步（Transactional Outbox Pattern）。
//
// 操作流程：
//  1. 失效详情缓存（双删策略的第一步）。
//  2. 在一个数据库事务中同时执行 UPDATE 和写入 outbox 事件
//     （通过 runKnowPostTx 实现）。
//  3. 再次失效详情缓存（双删策略的第二步）。
//  4. 失效 Feed 缓存（InvalidateAfterPostMutation），使公共 Feed 和"我的 Feed"
//     中的旧数据自然过期。
//
// 事务内写入 outbox 事件的意义：
//   确保"数据库更新"和"事件发布"是原子性的。
//   如果先更新数据库再独立发送事件，可能因服务崩溃而丢失事件。
//   outbox 表与业务数据在同一个事务中写入，再由 Canal 组件消费到 Kafka。
//   详见 Tansactional Outbox Pattern。
//
// 参数：
//   - creatorID: uint64，作者用户 ID。
//   - id: uint64，知文 ID。
//   - req: *KnowPostPatchRequest，包含需要更新的字段。
//
// 返回值：
//   - error: 数据库或权限校验失败时返回错误。
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

// Publish 把知文状态从草稿（draft）流转为已发布（published）。
//
// 功能：在数据库事务中执行状态更新并写入 outbox 事件，
// 同时触发 Feed 缓存失效，使新文章出现在公共 Feed 和"我的 Feed"中。
//
// 发布的前提条件：
//   - 知文当前状态必须是 "draft"（草稿）。
//   - 请求用户必须是该知文的 creatorID。
//   - 数据库 UPDATE 使用 WHERE status = 'draft' 防止重复发布。
//
// 参数：
//   - creatorID: uint64，作者用户 ID。
//   - id: uint64，知文 ID。
//
// 返回值：
//   - error: 如果知文不存在、无权发布或状态非 draft 则返回 ErrNotFound。
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

	return nil
}

// UpdateTop 更新知文的置顶标记。
//
// 功能：切换某个知文的置顶状态（is_top 字段）。置顶的知文在"我的 Feed"中优先显示
// （ORDER BY is_top DESC）。
//
// 事务流程：失效缓存 → 事务内 UPDATE + Outbox 事件 → 失效缓存 → 失效 Feed 缓存。
//
// 参数：
//   - creatorID: uint64，作者用户 ID。
//   - id: uint64，知文 ID。
//   - isTop: bool，true 表示置顶，false 表示取消置顶。
//
// 返回值：
//   - error: 知文不存在、无权修改或数据库错误时返回对应错误。
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

// UpdateVisibility 修改知文的可见性设置。
//
// 功能：将知文的 visible 字段修改为指定值。支持的可见性选项通过 isValidVisible 校验。
//
// 可见性选项说明：
//   - public：公开，所有人可见
//   - followers：仅粉丝可见（当前仅做标记，权限判定由业务层处理）
//   - school：仅同校用户可见（预留）
//   - private：仅自己可见
//   - unlisted：不列出（有链接即可访问，但不展示在 Feed 中）
//
// 参数：
//   - creatorID: uint64，作者用户 ID。
//   - id: uint64，知文 ID。
//   - visible: string，可见性值。不合法时返回 ErrBadRequest。
//
// 返回值：
//   - error: 参数不合法、权限不足或数据库错误时返回。
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
//
// 功能：将知文的 status 字段设置为 "deleted"（软删除），
// 而不是真正从数据库中删除。软删除保留数据的可恢复性和外键完整性。
//
// 软删除后的影响：
//   - 详情页：GetDetail 会检查 status == "deleted" 并返回 404。
//   - Feed 列表：ListMyPublished 使用 WHERE status != 'deleted' 过滤掉已删除的条目。
//   - 搜索索引：通过 outbox 事件同步触发删除。
//
// 文档已删除后仍保留哪些数据：
//   - 知文标题、内容等元数据仍然存在，只是标记为已删除。
//   - outbox 事件日志仍然完整。
//   - 计数器的点赞/收藏数据不会被自动清除（可以后续手动清理或等待过期）。
//
// 参数：
//   - creatorID: uint64，作者用户 ID。
//   - id: uint64，知文 ID。
//
// 返回值：
//   - error: 知文不存在、无权删除或数据库错误时返回。
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

// writeOutboxEvent 在 outbox 表中写入一条事务消息。
//
// 功能：生成一个唯一的雪花 ID 作为 outbox 记录的主键，
// 将事件元数据序列化为 JSON 后写入 outbox 表。
//
// 参数：
//   - repo: *KnowPostRepository，绑定了事务的 repository 实例（确保在同一个事务中写入）。
//   - id: uint64，关联的聚合根 ID（知文 ID）。
//   - aggType: string，聚合类型，如 "knowpost"。
//   - eventType: string，事件类型标记，如 "KnowPostPublished"、"KnowPostDeleted" 等。
//   - payloadData: interface{}，事件载荷，会被序列化为 JSON 字符串。
//     包含 entity、id、op、type 等字段。
//
// 返回值：
//   - error: 雪花 ID 生成或数据库写入失败时返回错误。
//
// 设计决策：
//   使用雪花算法（而非自增主键）作为 outbox 表主键，
//   避免了在分布式环境下多个服务实例同时写入 outbox 表时产生的主键冲突。
func (s *KnowPostService) writeOutboxEvent(repo *KnowPostRepository, id uint64, aggType, eventType string, payloadData interface{}) error {
	outID := s.idGen.NextID()
	payload, err := json.Marshal(payloadData)
	if err != nil {
		return err
	}
	return repo.InsertOutbox(outID, aggType, &id, eventType, string(payload))
}

// runKnowPostTx 在一个数据库事务中执行业务变更和 outbox 事件写入。
//
// 功能：这是 Transactional Outbox Pattern 的核心实现。
// 它开启一个数据库事务，执行传入的 mutate 函数（业务 UPDATE 操作），
// 然后在同一事务中写入 outbox 事件，最后提交事务。
//
// 事务流程：
//  1. s.db.BeginTxx 开启事务。
//  2. 通过 s.repo.WithDB(tx) 创建绑定到该事务的 repository 实例。
//  3. 传入的 mutate 函数使用 txRepo 执行 UPDATE 操作。
//  4. 调用 writeOutboxEvent 在同一个事务中写入 outbox 记录。
//  5. 若 mutate 失败，回滚事务并返回错误。
//  6. 若 writeOutboxEvent 失败，回滚事务（业务 UPDATE 也被撤销，保证原子性）。
//  7. 全部成功后，tx.Commit 持久化变更。
//
// 参数：
//   - id: uint64，知文 ID（用于 outbox 事件）。
//   - eventType: string，事件类型（用于 outbox 事件）。
//   - mutate: func(txRepo *KnowPostRepository) error，在事务内执行的业务变更函数。
//     如果该函数返回 error，事务会回滚。
//
// 返回值：
//   - error: 事务开启、业务变更、outbox 写入或事务提交任意环节失败时返回错误。
//
// 设计模式说明（Transactional Outbox）：
//   先更新 DB 再发送消息（如通过 Kafka）的模式存在风险：
//   如果消息发送失败但 DB 已经提交，会出现数据不一致。
//   通过把业务数据和 outbox 事件写入同一个 DB 事务，
//   Canal 组件可以安全地消费 outbox 表并发布到 Kafka，
//   实现了"至少一次投递"的语义保证。
//
// 安全措施：
//   - defer + recover 捕获 panic，确保 panic 时事务回滚。
//   - writeOutboxEvent 失败后手动 Rollback，防止事务悬挂。
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
