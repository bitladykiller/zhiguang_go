package knowpost

import "context"

// KnowPostUseCase 定义知文写路径和详情读取所需的业务接口。
//
// 这里没有把 Feed 相关方法也塞进来，而是把“写/详情”和“列表读取”拆开，
// 目的是让 handler 只依赖当前路由真正用到的能力。
type KnowPostUseCase interface {
	CreateDraft(ctx context.Context, creatorID uint64) (uint64, error)
	ConfirmContent(ctx context.Context, creatorID, id uint64, objectKey, etag, sha256 string, size uint64) error
	UpdateMetadata(ctx context.Context, creatorID, id uint64, req *KnowPostPatchRequest) error
	Publish(ctx context.Context, creatorID, id uint64) error
	UpdateTop(ctx context.Context, creatorID, id uint64, isTop bool) error
	UpdateVisibility(ctx context.Context, creatorID, id uint64, visible string) error
	Delete(ctx context.Context, creatorID, id uint64) error
	GetDetail(ctx context.Context, id uint64, currentUserID *uint64) (*KnowPostDetailResponse, error)
}

// KnowPostFeedUseCase 定义知文 Feed 读取所需的业务接口。
//
// Feed 是一个独立读模型，后续即使演进成独立缓存或独立服务，HTTP 层接口也能保持稳定。
type KnowPostFeedUseCase interface {
	GetPublicFeed(ctx context.Context, page, size int, currentUserID *uint64) (*FeedPageResponse, error)
	GetMyPublished(ctx context.Context, userID uint64, page, size int) (*FeedPageResponse, error)
}
