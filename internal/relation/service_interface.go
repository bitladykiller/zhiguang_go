package relation

import "context"

// RelationServiceInterface 定义关系模块对外暴露的业务方法。
//
// Handler 依赖此接口而非具体 *RelationService，使得 handler 可以独立于
// service 实现进行单元测试。
type RelationServiceInterface interface {
	Follow(ctx context.Context, fromUserID, toUserID uint64) (bool, error)
	Unfollow(ctx context.Context, fromUserID, toUserID uint64) (bool, error)
	IsFollowing(ctx context.Context, fromUserID, toUserID uint64) (bool, error)
	Following(ctx context.Context, userID uint64, limit, offset int) ([]uint64, error)
	Followers(ctx context.Context, userID uint64, limit, offset int) ([]uint64, error)
	FollowingCursor(ctx context.Context, userID uint64, limit int, cursor int64) ([]uint64, int64, error)
	FollowersCursor(ctx context.Context, userID uint64, limit int, cursor int64) ([]uint64, int64, error)
	RelationStatus(ctx context.Context, fromUserID, toUserID uint64) (string, error)
}

// 编译期断言：*RelationService 实现了 RelationServiceInterface。
var _ RelationServiceInterface = (*RelationService)(nil)
