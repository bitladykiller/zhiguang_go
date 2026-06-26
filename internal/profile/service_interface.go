package profile

import (
	"context"
	"time"

	"github.com/zhiguang/app/pkg/errcode"
)

// UserProfile 是 profile 模块导出的用户公开资料 DTO。
//
// 与 auth.User 不同，UserProfile 只包含公开可见的字段，
// 不含密码哈希等敏感信息。这样 profile 模块就不需要依赖 auth 包。
type UserProfile struct {
	ID        uint64     `json:"id"`
	Nickname  string     `json:"nickname"`
	Avatar    *string    `json:"avatar,omitempty"`
	Phone     *string    `json:"phone,omitempty"`
	Email     *string    `json:"email,omitempty"`
	ZgID      *string    `json:"zg_id,omitempty"`
	Birthday  *time.Time `json:"birthday,omitempty"`
	School    *string    `json:"school,omitempty"`
	Bio       *string    `json:"bio,omitempty"`
	Gender    *string    `json:"gender,omitempty"`
	TagsJSON  *string    `json:"tags_json,omitempty"`
	CreatedAt time.Time  `json:"created_at"`
	UpdatedAt time.Time  `json:"updated_at"`
}

// ProfileServicer 定义资料模块对外暴露的业务方法。
//
// Handler 依赖此接口而非具体 *Service，使得 handler 可以独立于
// service 实现进行单元测试。
type ProfileServicer interface {
	GetProfile(ctx context.Context, id uint64) (*UserProfile, *errcode.AppError)
	UpdateProfile(ctx context.Context, callerID, targetID uint64, req *ProfilePatchRequest) *errcode.AppError
}

// 编译期断言：*Service 实现了 ProfileServicer。
var _ ProfileServicer = (*Service)(nil)
