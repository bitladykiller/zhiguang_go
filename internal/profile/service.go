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

// NewProfileService 创建资料服务实例。
//
// 参数:
//   - repo: 资料数据仓库，封装了 MySQL 用户表的数据访问逻辑
//
// 返回值:
//   - *Service: 服务实例
//
// 说明:
//   Service 层负责业务编排，包括权限校验（仅本人可修改）、
//   无效请求检测（没有可更新的字段）和错误码转换。
func NewProfileService(repo *Repository) *Service {
	return &Service{repo: repo}
}

// GetProfile 根据用户 ID 查询用户完整资料。
//
// 参数：
//   - id: 要查询的用户 ID
//
// 返回值：
//   - *auth.User: 用户完整信息，包含所有公开字段
//   - *errcode.AppError: 用户不存在时返回 404
//
// 注意：返回的 User 对象包含 PasswordHash 字段，但该字段的 json tag 为 "-"，
// 在 HTTP 响应序列化时会被忽略，不会泄露密码哈希。
func (s *Service) GetProfile(ctx context.Context, id uint64) (*auth.User, *errcode.AppError) {
	user, err := s.repo.FindByID(ctx, id)
	if err != nil {
		return nil, errcode.ErrNotFound
	}
	return user, nil
}

// UpdateProfile 更新用户资料信息。
//
// 只有资料所有者本人可以更新（callerID == targetID）。
// 如果所有字段都为 nil（没有提供任何更新数据），返回 400。
//
// 参数：
//   - callerID: 发起请求的用户 ID（从 JWT 中获取）
//   - targetID: URL 路径中指定的目标用户 ID
//   - req: 要更新的字段集合（仅非 nil 的字段会被更新）
//
// 返回值：
//   - *errcode.AppError: 权限不足返回 403，无效请求返回 400，更新失败返回 500
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
