package profile

import (
	"context"
	"strings"

	"github.com/jmoiron/sqlx"
	"github.com/zhiguang/app/internal/auth"
)

// Repository 封装资料领域的数据访问逻辑。
type Repository struct {
	db *sqlx.DB
}

func NewProfileRepository(db *sqlx.DB) *Repository {
	return &Repository{db: db}
}

func (r *Repository) FindByID(ctx context.Context, id uint64) (*auth.User, error) {
	var user auth.User
	if err := r.db.GetContext(ctx, &user, `
SELECT id, phone, email, password_hash, nickname, avatar, bio, zg_id, gender, birthday, school, tags_json, created_at, updated_at
FROM users
WHERE id = ?
`, id); err != nil {
		return nil, err
	}
	return &user, nil
}

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
