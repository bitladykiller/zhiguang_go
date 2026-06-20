package relation

import (
	"context"

	"github.com/jmoiron/sqlx"
)

// Repo 是关系域的数据访问接口，使 Service 可被 mock。
type Repo interface {
	UpsertFollowing(ctx context.Context, id, fromUserID, toUserID uint64, status int) error
	UpsertFollower(ctx context.Context, id, toUserID, fromUserID uint64, status int) error
	CancelFollowing(ctx context.Context, fromUserID, toUserID uint64) (int64, error)
	CancelFollower(ctx context.Context, toUserID, fromUserID uint64) (int64, error)
	ExistsFollowing(ctx context.Context, fromUserID, toUserID uint64) (int, error)
	ListFollowingRows(ctx context.Context, userID uint64, limit, offset int) ([]FollowingRow, error)
	ListFollowerRows(ctx context.Context, userID uint64, limit, offset int) ([]FollowerRow, error)
	ListFollowerRowsFromFollowing(ctx context.Context, userID uint64, limit, offset int) ([]FollowerRow, error)
	WithDB(db sqlx.ExtContext) *RelationRepository
}

// 编译期断言：*RelationRepository 实现了 Repo。
var _ Repo = (*RelationRepository)(nil)