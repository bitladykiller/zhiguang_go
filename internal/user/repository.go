// user 包提供用户相关的数据访问能力。
package user

import (
	"context"

	"github.com/jmoiron/sqlx"
	"github.com/zhiguang/app/internal/auth"
)

// UserRepository 封装所有用户数据访问操作。
// 它基于 sqlx 构造查询，并向服务层返回领域模型。
type UserRepository struct {
	db *sqlx.DB
}

// NewUserRepository 基于给定 sqlx DB 实例创建仓储对象。
func NewUserRepository(db *sqlx.DB) *UserRepository {
	return &UserRepository{db: db}
}

const userSelectColumns = `
SELECT id, phone, email, password_hash, nickname, avatar, bio, zg_id, gender, birthday, school, tags_json, created_at, updated_at
FROM users
`

// Create 插入一条新的用户记录。
func (r *UserRepository) Create(user *auth.User) error {
	result, err := r.db.NamedExecContext(context.Background(), `
INSERT INTO users (
    phone, email, password_hash, nickname, avatar, bio, zg_id, gender, birthday, school, tags_json
) VALUES (
    :phone, :email, :password_hash, :nickname, :avatar, :bio, :zg_id, :gender, :birthday, :school, :tags_json
)`, user)
	if err != nil {
		return err
	}

	insertID, err := result.LastInsertId()
	if err != nil {
		return err
	}
	user.ID = uint64(insertID)
	return nil
}

// ExistsByPhone 检查是否存在指定手机号的用户。
func (r *UserRepository) ExistsByPhone(phone string) bool {
	var count int
	if err := r.db.GetContext(context.Background(), &count, "SELECT COUNT(1) FROM users WHERE phone = ?", phone); err != nil {
		return false
	}
	return count > 0
}

// ExistsByEmail 检查是否存在指定邮箱的用户。
func (r *UserRepository) ExistsByEmail(email string) bool {
	var count int
	if err := r.db.GetContext(context.Background(), &count, "SELECT COUNT(1) FROM users WHERE email = ?", email); err != nil {
		return false
	}
	return count > 0
}

// FindByPhone 根据手机号查询用户。
func (r *UserRepository) FindByPhone(phone string) (*auth.User, error) {
	var user auth.User
	if err := r.db.GetContext(context.Background(), &user, userSelectColumns+" WHERE phone = ? LIMIT 1", phone); err != nil {
		return nil, err
	}
	return &user, nil
}

// FindByEmail 根据邮箱查询用户。
func (r *UserRepository) FindByEmail(email string) (*auth.User, error) {
	var user auth.User
	if err := r.db.GetContext(context.Background(), &user, userSelectColumns+" WHERE email = ? LIMIT 1", email); err != nil {
		return nil, err
	}
	return &user, nil
}

// FindByID 根据主键查询用户。
func (r *UserRepository) FindByID(id uint64) (*auth.User, error) {
	var user auth.User
	if err := r.db.GetContext(context.Background(), &user, userSelectColumns+" WHERE id = ?", id); err != nil {
		return nil, err
	}
	return &user, nil
}

// UpdatePassword 仅更新指定用户的 password_hash 字段。
func (r *UserRepository) UpdatePassword(id uint64, passwordHash string) error {
	_, err := r.db.ExecContext(context.Background(), "UPDATE users SET password_hash = ? WHERE id = ?", passwordHash, id)
	return err
}

// Update 保存用户对象中的全部非零字段。
func (r *UserRepository) Update(user *auth.User) error {
	_, err := r.db.NamedExecContext(context.Background(), `
UPDATE users
SET phone = :phone,
    email = :email,
    password_hash = :password_hash,
    nickname = :nickname,
    avatar = :avatar,
    bio = :bio,
    zg_id = :zg_id,
    gender = :gender,
    birthday = :birthday,
    school = :school,
    tags_json = :tags_json
WHERE id = :id
`, user)
	return err
}

// ListByIDs 按主键批量查询多个用户。
// 当 ids 为空或没有匹配结果时，返回空切片而不是 nil。
func (r *UserRepository) ListByIDs(ids []uint64) ([]auth.User, error) {
	var users []auth.User
	if len(ids) == 0 {
		return users, nil
	}
	query, args, err := sqlx.In(userSelectColumns+" WHERE id IN (?)", ids)
	if err != nil {
		return nil, err
	}
	query = r.db.Rebind(query)
	if err := r.db.SelectContext(context.Background(), &users, query, args...); err != nil {
		return nil, err
	}
	return users, nil
}
