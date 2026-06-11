package profile

import (
	"context"

	"github.com/zhiguang/app/internal/auth"
	"github.com/zhiguang/app/pkg/errcode"
)

// ProfileUseCase 定义资料 HTTP 层依赖的业务接口。
//
// 这个接口保持很窄，只覆盖资料读取和更新，避免 profile handler 持有不必要的实现依赖。
type ProfileUseCase interface {
	GetProfile(ctx context.Context, id uint64) (*auth.User, *errcode.AppError)
	UpdateProfile(ctx context.Context, callerID, targetID uint64, req *ProfilePatchRequest) *errcode.AppError
}
