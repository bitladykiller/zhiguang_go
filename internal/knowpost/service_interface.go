package knowpost

import "context"

// KnowPostWriteService 定义知文写操作对外暴露的业务方法。
//
// Handler 依赖此接口而非具体 *KnowPostService，使得 handler 可以独立于
// service 实现进行单元测试。
type KnowPostWriteService interface {
	CreateDraft(ctx context.Context, creatorID uint64) (uint64, error)
	ConfirmContent(ctx context.Context, creatorID, id uint64, objectKey, etag, sha256 string, size uint64) error
	UpdateMetadata(ctx context.Context, creatorID, id uint64, req *KnowPostPatchRequest) error
	Publish(ctx context.Context, creatorID, id uint64) error
	UpdateTop(ctx context.Context, creatorID, id uint64, isTop bool) error
	UpdateVisibility(ctx context.Context, creatorID, id uint64, visible string) error
	Delete(ctx context.Context, creatorID, id uint64) error
}

// KnowPostReadService 定义知文读操作对外暴露的业务方法。
type KnowPostReadService interface {
	GetDetail(ctx context.Context, id uint64, currentUserID *uint64) (*KnowPostDetailResponse, error)
}

// KnowPostFeedServiceInterface 定义 Feed 流读操作对外暴露的业务方法。
type KnowPostFeedServiceInterface interface {
	GetPublicFeed(ctx context.Context, page, size int, currentUserID *uint64) (*FeedPageResponse, error)
	GetMyPublished(ctx context.Context, userID uint64, page, size int) (*FeedPageResponse, error)
}

// 编译期断言。
var (
	_ KnowPostWriteService         = (*KnowPostService)(nil)
	_ KnowPostReadService          = (*KnowPostService)(nil)
	_ KnowPostFeedServiceInterface = (*KnowPostFeedService)(nil)
)
