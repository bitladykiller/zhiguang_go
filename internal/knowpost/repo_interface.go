package knowpost

import (
	"context"

	"github.com/jmoiron/sqlx"
)

// Repo 是数据访问接口，使 Service 和 FeedService 可被 mock。
type Repo interface {
	InsertDraft(ctx context.Context, post *KnowPost) error
	UpdateContent(ctx context.Context, post *KnowPost) (int64, error)
	UpdateMetadata(ctx context.Context, post *KnowPost) (int64, error)
	Publish(ctx context.Context, id, creatorID uint64) (int64, error)
	UpdateTop(ctx context.Context, id, creatorID uint64, isTop bool) (int64, error)
	UpdateVisibility(ctx context.Context, id, creatorID uint64, visible KnowPostVisibility) (int64, error)
	SoftDelete(ctx context.Context, id, creatorID uint64) (int64, error)
	FindDetailByID(ctx context.Context, id uint64) (*KnowPostDetailRow, error)
	ListFeedPublic(ctx context.Context, limit, offset int) ([]KnowPostFeedRow, error)
	ListMyPublished(ctx context.Context, userID uint64, limit, offset int) ([]KnowPostFeedRow, error)
	WithDB(db sqlx.ExtContext) Repo
}

// 编译期断言：*KnowPostRepository 实现了 Repo。
var _ Repo = (*KnowPostRepository)(nil)