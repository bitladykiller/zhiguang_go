package profile

import (
	"context"
	"fmt"
	"strings"

	"github.com/jmoiron/sqlx"
	"github.com/zhiguang/app/internal/model"
)

var profileUpdateFields = map[string]string{
	"nickname":  "nickname",
	"avatar":    "avatar",
	"bio":       "bio",
	"gender":    "gender",
	"school":    "school",
	"tags_json": "tags_json",
	"birthday":  "birthday",
}

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
	sets := make([]string, 0, 7)
	args := make([]interface{}, 0, 8)
	if req.Nickname != nil {
		if _, ok := profileUpdateFields["nickname"]; !ok {
			return fmt.Errorf("unknown field: nickname")
		}
		sets = append(sets, "nickname = ?")
		args = append(args, *req.Nickname)
	}
	if req.Avatar != nil {
		if _, ok := profileUpdateFields["avatar"]; !ok {
			return fmt.Errorf("unknown field: avatar")
		}
		sets = append(sets, "avatar = ?")
		args = append(args, *req.Avatar)
	}
	if req.Bio != nil {
		if _, ok := profileUpdateFields["bio"]; !ok {
			return fmt.Errorf("unknown field: bio")
		}
		sets = append(sets, "bio = ?")
		args = append(args, *req.Bio)
	}
	if req.Gender != nil {
		if _, ok := profileUpdateFields["gender"]; !ok {
			return fmt.Errorf("unknown field: gender")
		}
		sets = append(sets, "gender = ?")
		args = append(args, *req.Gender)
	}
	if req.School != nil {
		if _, ok := profileUpdateFields["school"]; !ok {
			return fmt.Errorf("unknown field: school")
		}
		sets = append(sets, "school = ?")
		args = append(args, *req.School)
	}
	if req.TagsJson != nil {
		if _, ok := profileUpdateFields["tags_json"]; !ok {
			return fmt.Errorf("unknown field: tags_json")
		}
		sets = append(sets, "tags_json = ?")
		args = append(args, *req.TagsJson)
	}
	if req.Birthday != nil {
		if _, ok := profileUpdateFields["birthday"]; !ok {
			return fmt.Errorf("unknown field: birthday")
		}
		sets = append(sets, "birthday = ?")
		args = append(args, *req.Birthday)
	}
	if len(sets) == 0 {
		return nil
	}

	args = append(args, id)
	query := "UPDATE users SET " + strings.Join(sets, ", ") + " WHERE id = ?"
	_, err := r.db.ExecContext(ctx, query, args...)
	return err
}

// toUserProfile 将 model.User 映射为对外 DTO，过滤敏感字段。
func toUserProfile(row *model.User) *UserProfile {
	return &UserProfile{
		ID:       row.ID,
		Nickname: row.Nickname,
		Avatar:   row.Avatar,
		Phone:    row.Phone,
		Email:    row.Email,
		ZgID:     row.ZgID,
		Birthday: row.Birthday,
		School:   row.School,
		Bio:      row.Bio,
		Gender:   row.Gender,
		TagsJSON: row.TagsJSON,
		CreatedAt: row.CreatedAt,
		UpdatedAt: row.UpdatedAt,
	}
}
