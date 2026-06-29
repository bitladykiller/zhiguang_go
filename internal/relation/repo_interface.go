package relation

import (
	"context"

	"github.com/jmoiron/sqlx"
)

// RepoWriter 定义关系写操作接口。
type RepoWriter interface {
	UpsertFollowing(ctx context.Context, id, fromUserID, toUserID uint64, status int) error
	UpsertFollower(ctx context.Context, id, toUserID, fromUserID uint64, status int) error
	CancelFollowing(ctx context.Context, fromUserID, toUserID uint64) (int64, error)
	CancelFollower(ctx context.Context, toUserID, fromUserID uint64) (int64, error)
}

// RepoReader 定义关系读操作接口。
type RepoReader interface {
	ExistsFollowing(ctx context.Context, fromUserID, toUserID uint64) (int, error)
	ListFollowingRows(ctx context.Context, userID uint64, limit, offset int) ([]FollowingRow, error)
	ListFollowerRows(ctx context.Context, userID uint64, limit, offset int) ([]FollowerRow, error)
	ListFollowerRowsFromFollowing(ctx context.Context, userID uint64, limit, offset int) ([]FollowerRow, error)
}

// Repo 是关系域的数据访问接口，组合读写接口，使 Service 可被 mock。
type Repo interface {
	RepoWriter
	RepoReader
	WithDB(db sqlx.ExtContext) *RelationRepository
}

// 编译期断言：*RelationRepository 实现了 Repo。
var _ Repo = (*RelationRepository)(nil)