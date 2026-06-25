package profile

import (
	"context"

	"github.com/jmoiron/sqlx"
	"github.com/zhiguang/app/pkg/errcode"
)

// Repo 是数据访问接口，使 Service 可被 mock。
type Repo interface {
	FindByID(ctx context.Context, id uint64) (*UserProfile, error)
	Update(ctx context.Context, id uint64, req *ProfilePatchRequest) error
	WithDB(db sqlx.ExtContext) *Repository
}

// Service 负责资料领域的业务编排。
type Service struct {
	repo Repo
}

// NewProfileService 创建资料服务实例。
func NewProfileService(repo Repo) *Service {
	return &Service{repo: repo}
}

// GetProfile 根据用户 ID 查询用户公开资料。
func (s *Service) GetProfile(ctx context.Context, id uint64) (*UserProfile, *errcode.AppError) {
	user, err := s.repo.FindByID(ctx, id)
	if err != nil {
		return nil, errcode.ErrNotFound
	}
	return user, nil
}

// UpdateProfile 更新用户资料信息。只有资料所有者本人可以更新。
func (s *Service) UpdateProfile(ctx context.Context, callerID, targetID uint64, req *ProfilePatchRequest) *errcode.AppError {
	if callerID != targetID {
		return errcode.ErrForbidden
	}
	if isNoOp(req) {
		return errcode.ErrBadRequest.WithMsg("no fields to update")
	}
	if err := s.repo.Update(ctx, targetID, req); err != nil {
		return errcode.ErrInternal.WithMsg("failed to update profile")
	}
	return nil
}

// isNoOp 判断资料更新请求是否所有字段都为空（无实际操作）。
// 注意：新增 ProfilePatchRequest 字段时需要同步更新此函数。
func isNoOp(req *ProfilePatchRequest) bool {
	return req.Nickname == nil && req.Avatar == nil && req.Bio == nil && req.Gender == nil &&
		req.Birthday == nil && req.School == nil && req.TagsJson == nil
}
