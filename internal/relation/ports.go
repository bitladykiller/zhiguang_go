package relation

import "context"

// RelationUseCase 定义关系 HTTP 层依赖的业务接口。
//
// Handler 只关心“关注关系如何对外暴露”，不需要知道底层是否同时写了 Redis、
// 是否触发了异步 outbox，因而这里保持在协议语义层。
type RelationUseCase interface {
	Follow(ctx context.Context, fromUserID, toUserID uint64) (bool, error)
	Unfollow(ctx context.Context, fromUserID, toUserID uint64) (bool, error)
	RelationStatus(ctx context.Context, fromUserID, toUserID uint64) (string, error)
	Following(ctx context.Context, userID uint64, limit, offset int) ([]uint64, error)
	Followers(ctx context.Context, userID uint64, limit, offset int) ([]uint64, error)
	FollowingCursor(ctx context.Context, userID uint64, limit int, cursor int64) ([]uint64, int64, error)
	FollowersCursor(ctx context.Context, userID uint64, limit int, cursor int64) ([]uint64, int64, error)
}
