package profile

import (
	"context"
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
	sets, args := buildUpdateSet(req)
	if len(sets) == 0 {
		return nil
	}

	args = append(args, id)
	query := "UPDATE users SET " + strings.Join(sets, ", ") + " WHERE id = ?"
	_, err := r.db.ExecContext(ctx, query, args...)
	return err
}

// buildUpdateSet 根据 ProfilePatchRequest 中非 nil 字段构建 SQL SET 子句和参数。
// 返回的 sets 和 args 一一对应，sets[i] = "col = ?"，args[i] = 对应值。
func buildUpdateSet(req *ProfilePatchRequest) ([]string, []interface{}) {
	sets := make([]string, 0, 7)
	args := make([]interface{}, 0, 7)

	type fieldDef struct {
		ptr  interface{}
		name string
	}

	fields := []fieldDef{
		{req.Nickname, "nickname"},
		{req.Avatar, "avatar"},
		{req.Bio, "bio"},
		{req.Gender, "gender"},
		{req.School, "school"},
		{req.TagsJson, "tags_json"},
		{req.Birthday, "birthday"},
	}

	for _, f := range fields {
		if f.ptr == nil {
			continue
		}
		if _, ok := profileUpdateFields[f.name]; !ok {
			continue
		}
		// 利用 switch 解引用不同类型的指针
		switch v := f.ptr.(type) {
		case *string:
			if v == nil {
				continue
			}
			sets = append(sets, f.name+" = ?")
			args = append(args, *v)
		}
	}

	return sets, args
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
