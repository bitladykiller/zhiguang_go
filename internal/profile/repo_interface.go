package profile

import (
	"context"

	"github.com/jmoiron/sqlx"
)

// Repo 是数据访问接口，使 Service 可被 mock。
type Repo interface {
	FindByID(ctx context.Context, id uint64) (*UserProfile, error)
	Update(ctx context.Context, id uint64, req *ProfilePatchRequest) error
	WithDB(db sqlx.ExtContext) *Repository
}

// 编译期断言：*Repository 实现了 Repo。
var _ Repo = (*Repository)(nil)