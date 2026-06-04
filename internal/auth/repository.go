package auth

import (
	"context"
	"database/sql"

	"github.com/jmoiron/sqlx"
)

const authUserSelectColumns = `
SELECT id, phone, email, password_hash, nickname, avatar, bio, zg_id, gender, birthday, school, tags_json, created_at, updated_at
FROM users
`

// AuthRepository 封装鉴权领域的数据访问操作。
type AuthRepository struct {
	db *sqlx.DB
}

func NewAuthRepository(db *sqlx.DB) *AuthRepository {
	return &AuthRepository{db: db}
}

func (r *AuthRepository) CreateUser(ctx context.Context, user *User) error {
	result, err := r.db.NamedExecContext(ctx, `
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

func (r *AuthRepository) FindUserByID(ctx context.Context, id uint64) (*User, error) {
	var user User
	if err := r.db.GetContext(ctx, &user, authUserSelectColumns+" WHERE id = ?", id); err != nil {
		return nil, err
	}
	return &user, nil
}

func (r *AuthRepository) FindUserByIdentifier(ctx context.Context, idType IdentifierType, identifier string) (*User, error) {
	var user User
	var err error
	switch idType {
	case IdentifierPhone:
		err = r.db.GetContext(ctx, &user, authUserSelectColumns+" WHERE phone = ? LIMIT 1", identifier)
	case IdentifierEmail:
		err = r.db.GetContext(ctx, &user, authUserSelectColumns+" WHERE email = ? LIMIT 1", identifier)
	default:
		return nil, sql.ErrNoRows
	}
	if err != nil {
		return nil, err
	}
	return &user, nil
}

func (r *AuthRepository) IdentifierExists(ctx context.Context, idType IdentifierType, identifier string) bool {
	var count int
	var err error
	switch idType {
	case IdentifierPhone:
		err = r.db.GetContext(ctx, &count, "SELECT COUNT(1) FROM users WHERE phone = ?", identifier)
	case IdentifierEmail:
		err = r.db.GetContext(ctx, &count, "SELECT COUNT(1) FROM users WHERE email = ?", identifier)
	default:
		return false
	}
	if err != nil {
		return false
	}
	return count > 0
}

func (r *AuthRepository) UpdatePassword(ctx context.Context, id uint64, passwordHash string) error {
	_, err := r.db.ExecContext(ctx, "UPDATE users SET password_hash = ? WHERE id = ?", passwordHash, id)
	return err
}

func (r *AuthRepository) RecordLoginLog(ctx context.Context, log *LoginLog) {
	_, _ = r.db.NamedExecContext(ctx, `
INSERT INTO login_logs (user_id, identifier, channel, ip, user_agent, status)
VALUES (:user_id, :identifier, :channel, :ip, :user_agent, :status)
`, log)
}
