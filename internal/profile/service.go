package profile

import (
	"context"

	"github.com/zhiguang/app/pkg/errcode"
)

// Service 负责资料领域的业务编排。
type Service struct {
	repo Repo
}

// NewProfileService 创建资料服务实例。
func NewProfileService(repo Repo) *Service {
	return &Service{repo: repo}
}

// GetProfile 根据用户 ID 查询用户公开资料。
func (s *Service) GetProfile(ctx context.Context, id uint64) (*UserProfile, error) {
	user, err := s.repo.FindByID(ctx, id)
	if err != nil {
		return nil, errcode.ErrNotFound
	}
	return user, nil
}

// UpdateProfile 更新用户资料信息。只有资料所有者本人可以更新。
func (s *Service) UpdateProfile(ctx context.Context, callerID, targetID uint64, req *ProfilePatchRequest) error {
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
func isNoOp(req *ProfilePatchRequest) bool {
	return req.Nickname == nil && req.Avatar == nil && req.Bio == nil && req.Gender == nil &&
		req.Birthday == nil && req.School == nil && req.TagsJson == nil
}
