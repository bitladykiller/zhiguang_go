package profile

import (
	"context"
	"strings"

	"github.com/jmoiron/sqlx"
	"github.com/zhiguang/app/internal/auth"
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

// FindByID 根据用户 ID 查询完整用户资料。
//
// 功能：从 users 表查询用户全部字段（含密码哈希），通过 sqlx.GetContext 映射到 auth.User 结构体。
// 查询结果包含敏感字段 password_hash，但该字段在 API 响应序列化时会被忽略。
//
// 参数：
//   - ctx: context.Context，上下文。
//   - id: uint64，用户 ID。
//
// 返回值：
//   - *auth.User: 用户完整信息，包含所有数据库字段。
//   - error: 用户不存在时返回 sql.ErrNoRows。
func (r *Repository) FindByID(ctx context.Context, id uint64) (*auth.User, error) {
	var user auth.User
	if err := sqlx.GetContext(ctx, r.db, &user, `
SELECT id, phone, email, password_hash, nickname, avatar, bio, zg_id, gender, birthday, school, tags_json, created_at, updated_at
FROM users
WHERE id = ?
`, id); err != nil {
		return nil, err
	}
	return &user, nil
}

// Update 动态更新用户资料的部分字段（PATCH 语义）。
//
// 功能：根据 ProfilePatchRequest 中非 nil 的字段动态构建 SET 子句。
// 只更新调用方指定的字段，其他字段保持不变。
//
// 参数：
//   - ctx: context.Context，上下文。
//   - id: uint64，目标用户 ID。
//   - req: *ProfilePatchRequest，包含要更新的字段。仅非 nil 的字段会被更新。
//
// 返回值：
//   - error: 数据库执行失败时的错误。
//
// 边界情况：
//   - req 所有字段都为 nil：跳过执行，返回 nil（无实际更新）。
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
