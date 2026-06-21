package profile

import (
	"context"
	"strings"

	"github.com/jmoiron/sqlx"
)

// userRow 是 users 表的数据库行映射，仅供本包内部使用。
//
// 与 auth.User 的区别：
//   - 这是一个纯数据库行结构，不带 json tag，不会泄露到 API 层。
//   - profile 包完全自治，不依赖 auth 包。
type userRow struct {
	ID           uint64  `db:"id"`
	Phone        *string `db:"phone"`
	Email        *string `db:"email"`
	PasswordHash *string `db:"password_hash"`
	Nickname     string  `db:"nickname"`
	Avatar       *string `db:"avatar"`
	Bio          *string `db:"bio"`
	ZgID         *string `db:"zg_id"`
	Gender       *string `db:"gender"`
	Birthday     *string `db:"birthday"`
	School       *string `db:"school"`
	TagsJSON     *string `db:"tags_json"`
	CreatedAt    *string `db:"created_at"`
	UpdatedAt    *string `db:"updated_at"`
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
	var row userRow
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
		sets = append(sets, "nickname = ?")
		args = append(args, *req.Nickname)
	}
	if req.Avatar != nil {
		sets = append(sets, "avatar = ?")
		args = append(args, *req.Avatar)
	}
	if req.Bio != nil {
		sets = append(sets, "bio = ?")
		args = append(args, *req.Bio)
	}
	if req.Gender != nil {
		sets = append(sets, "gender = ?")
		args = append(args, *req.Gender)
	}
	if req.School != nil {
		sets = append(sets, "school = ?")
		args = append(args, *req.School)
	}
	if req.TagsJson != nil {
		sets = append(sets, "tags_json = ?")
		args = append(args, *req.TagsJson)
	}
	if req.Birthday != nil {
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

// toUserProfile 将内部行映射为对外 DTO，过滤敏感字段。
func toUserProfile(row *userRow) *UserProfile {
	return &UserProfile{
		ID:       row.ID,
		Nickname: row.Nickname,
		Avatar:   row.Avatar,
		Phone:    row.Phone,
		Email:    row.Email,
		ZgID:     row.ZgID,
		Birthday: parseTimePtr(row.Birthday),
		School:   row.School,
		Bio:      row.Bio,
		Gender:   row.Gender,
		TagsJSON: row.TagsJSON,
	}
}
