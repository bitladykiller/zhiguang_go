package profile

import (
	"context"

	"github.com/zhiguang/app/internal/auth"
	"github.com/zhiguang/app/pkg/errcode"
)

// Service 负责资料领域的业务编排。
type Service struct {
	repo *Repository
}

func NewProfileService(repo *Repository) *Service {
	return &Service{repo: repo}
}

func (s *Service) GetProfile(ctx context.Context, id uint64) (*auth.User, *errcode.AppError) {
	user, err := s.repo.FindByID(ctx, id)
	if err != nil {
		return nil, errcode.ErrNotFound
	}
	return user, nil
}

func (s *Service) UpdateProfile(ctx context.Context, callerID, targetID uint64, req *ProfilePatchRequest) *errcode.AppError {
	if callerID != targetID {
		return errcode.ErrForbidden
	}
	if req.Nickname == nil && req.Avatar == nil && req.Bio == nil && req.Gender == nil &&
		req.Birthday == nil && req.School == nil && req.TagsJson == nil {
		return errcode.ErrBadRequest.WithMsg("no fields to update")
	}
	if err := s.repo.Update(ctx, targetID, req); err != nil {
		return errcode.ErrInternal.WithMsg("failed to update profile")
	}
	return nil
}
