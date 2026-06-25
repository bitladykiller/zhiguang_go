package profile

import (
	"context"
	"fmt"

	"github.com/jmoiron/sqlx"
	"github.com/zhiguang/app/internal/model"
	"github.com/zhiguang/app/pkg/sqlutil"
)

// Repository 封装资料领域的数据访问逻辑。
type Repository struct {
	db sqlx.ExtContext
}

// NewProfileRepository 创建资料数据访问仓库。
func NewProfileRepository(db sqlx.ExtContext) *Repository {
	return &Repository{db: db}
}

// WithDB 克隆绑定到指定 sqlx 句柄的新仓储实例，用于事务上下文。
func (r *Repository) WithDB(db sqlx.ExtContext) *Repository {
	return &Repository{db: db}
}

// FindByID 根据用户 ID 查询用户公开资料。
func (r *Repository) FindByID(ctx context.Context, id uint64) (*UserProfile, error) {
	var row model.User
	if err := sqlx.GetContext(ctx, r.db, &row, `
	SELECT id, phone, email, nickname, avatar, bio, zg_id, gender, birthday, school, tags_json, created_at, updated_at
	FROM users
	WHERE id = ?
	`, id); err != nil {
		return nil, err
	}
	return toUserProfile(&row), nil
}

// Update 动态更新用户资料的部分字段（PATCH 语义）。
func (r *Repository) Update(ctx context.Context, id uint64, req *ProfilePatchRequest) error {
	sets, args := sqlutil.BuildSetClause(
		sqlutil.SetIf(req.Nickname != nil, "nickname = ?", *req.Nickname),
		sqlutil.SetIf(req.Avatar != nil, "avatar = ?", *req.Avatar),
		sqlutil.SetIf(req.Bio != nil, "bio = ?", *req.Bio),
		sqlutil.SetIf(req.Gender != nil, "gender = ?", *req.Gender),
		sqlutil.SetIf(req.School != nil, "school = ?", *req.School),
		sqlutil.SetIf(req.TagsJson != nil, "tags_json = ?", *req.TagsJson),
		sqlutil.SetIf(req.Birthday != nil, "birthday = ?", *req.Birthday),
	)
	if len(sets) == 0 {
		return nil
	}

	args = append(args, id)
	query := "UPDATE users SET " + sets + " WHERE id = ?"
	_, err := r.db.ExecContext(ctx, query, args...)
	if err != nil {
		return fmt.Errorf("profile repository: update user: %w", err)
	}
	return nil
}

// toUserProfile 将 model.User 映射为对外 DTO，过滤敏感字段。
func toUserProfile(row *model.User) *UserProfile {
	return &UserProfile{
		ID:        row.ID,
		Nickname:  row.Nickname,
		Avatar:    row.Avatar,
		Phone:     row.Phone,
		Email:     row.Email,
		ZgID:      row.ZgID,
		Birthday:  row.Birthday,
		School:    row.School,
		Bio:       row.Bio,
		Gender:    row.Gender,
		TagsJSON:  row.TagsJSON,
		CreatedAt: row.CreatedAt,
		UpdatedAt: row.UpdatedAt,
	}
}
