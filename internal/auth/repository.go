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
//
// 提供以下能力：
//   - 创建用户（INSERT users）
//   - 通过 ID 或标识（手机号/邮箱）查询用户
//   - 检查标识是否已存在
//   - 更新密码
//   - 记录登录审计日志
type AuthRepository struct {
	db *sqlx.DB
}

func NewAuthRepository(db *sqlx.DB) *AuthRepository {
	return &AuthRepository{db: db}
}

// CreateUser 在数据库中创建新用户记录。
//
// 使用 sqlx 的 NamedExecContext 执行带命名参数的 INSERT 语句。
//
// sqlx.NamedExecContext 说明：
//   - 允许使用 :fieldname 形式的命名参数绑定结构体字段
//   - 自动根据结构体字段的 db tag（如 `db:"phone"`）映射到 SQL 列名
//   - 相比按位置传参的 ExecContext，NamedExecContext 更易维护、不易出错
//   - 当结构体字段较多时（本例 users 表有 11 个字段）优势更明显
//
// 插入成功后通过 result.LastInsertId() 获取自增主键 ID 并回写到 user.ID。
// 注意 LastInsertId() 依赖于数据库驱动实现：
//   - MySQL 驱动支持（基于自增列）
//   - PostgreSQL 需要 RETURNING 子句，此处不适用
//
// 参数:
//   - ctx: 上下文，用于超时控制或链路追踪
//   - user: 包含用户注册信息的 User 结构体指针。插入成功后 user.ID 会被赋值为新记录的自增 ID
//
// 返回值:
//   - error: 插入失败（如唯一约束冲突）或 LastInsertId 调用失败时返回
//
// 边界情况:
//   - 因唯一约束（如 phone/email 重复）导致的插入失败由数据库返回错误，透传给调用方处理
//   - user 结构体中未赋值的字段会插入 NULL 或默认值（取决于表定义）
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

// FindUserByID 根据主键 ID 查询用户信息。
//
// 使用 sqlx 的 GetContext 执行查询，将结果的一行映射到 User 结构体。
//
// sqlx.GetContext 说明：
//   - 执行查询并将结果的第一行扫描到目标结构体
//   - 通过 db tag（如 `db:"phone"`）将列名与结构体字段关联
//   - 如果查询结果为空（无匹配行），GetContext 返回 sql.ErrNoRows
//   - 适用于确定只返回 0 或 1 行的查询（主键查询、带 LIMIT 1 的查询）
//   - 内部调用 sqlx.StructScan 进行反射映射，字段顺序无需与 SELECT 列顺序一致
//
// 参数:
//   - ctx: 上下文
//   - id: 用户主键 ID
//
// 返回值:
//   - *User: 查询到的用户信息（不为 nil 时保证所有字段已填充）
//   - error: sql.ErrNoRows（用户不存在）或数据库异常时返回
//
// 边界情况:
//   - 用户不存在时返回 sql.ErrNoRows，由调用方统一处理为"用户不存在"业务错误
//   - 如果 SELECT 返回多行（逻辑上不应发生，因为 id 是主键），GetContext 只扫描第一行
func (r *AuthRepository) FindUserByID(ctx context.Context, id uint64) (*User, error) {
	var user User
	if err := r.db.GetContext(ctx, &user, authUserSelectColumns+" WHERE id = ?", id); err != nil {
		return nil, err
	}
	return &user, nil
}

// FindUserByIdentifier 根据标识类型（手机号/邮箱）查询用户信息。
//
// 根据 idType 切换查询条件字段（phone 或 email）；
// 对非 IdentifierPhone/IdentifierEmail 类型直接返回 sql.ErrNoRows 避免 SQL 注入风险。
//
// 参数:
//   - ctx: 上下文
//   - idType: 标识类型（IdentifierPhone 或 IdentifierEmail）
//   - identifier: 标识值（手机号或邮箱字符串）
//
// 返回值:
//   - *User: 查询到的用户信息
//   - error: sql.ErrNoRows（未找到）或数据库异常时返回
//
// 边界情况:
//   - 传入不支持的标识类型（default 分支）返回 sql.ErrNoRows 而非 panic，体现防御式编程
//   - 查询结果为空时 GetContext 返回 sql.ErrNoRows
//   - SELECT 语句已带 LIMIT 1 确保即使有重复记录也只返回一条
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

// IdentifierExists 检查手机号或邮箱是否已被其他用户注册。
//
// 使用 SELECT COUNT(1) 而非直接查询用户记录，减少数据传输量（只返回一个整数）。
// 相比先查询用户再判断 err == sql.ErrNoRows，COUNT 方式更简洁高效。
//
// 参数:
//   - ctx: 上下文
//   - idType: 标识类型（IdentifierPhone / IdentifierEmail）
//   - identifier: 标识值（手机号或邮箱字符串）
//
// 返回值:
//   - bool: true 表示已存在，false 表示不存在或查询异常
//
// 边界情况:
//   - 数据库异常时默认返回 false（倾向于可用性，防止误判为已存在导致用户无法注册）
//   - 不支持的标识类型直接返回 false（防御式编程）
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

// UpdatePassword 更新指定用户的密码哈希值。
//
// 使用 sqlx 的 ExecContext 执行 UPDATE 语句。ExecContext 是标准 database/sql 接口的扩展，
// 支持 ? 占位符按位置传参。与 GetContext 不同，ExecContext 不返回行数据，仅返回 sql.Result。
//
// 注意：passwordHash 是已经过 bcrypt 等算法哈希处理后的值，绝不可在数据库中存储明文密码。
// 哈希应在调用此方法之前由上层服务完成。
//
// 参数:
//   - ctx: 上下文
//   - id: 用户主键 ID
//   - passwordHash: 经过 bcrypt/scrypt 等算法哈希处理后的密码字符串
//
// 返回值:
//   - error: 数据库异常时返回
//
// 边界情况:
//   - 如果 id 对应的用户不存在，ExecContext 不会报错（UPDATE 0 rows 不视为错误），
//     调用方需自行确认用户存在后再调用此方法
//   - 如果新的 passwordHash 与原有值相同，UPDATE 仍然会成功（affected rows 可能为 0）
func (r *AuthRepository) UpdatePassword(ctx context.Context, id uint64, passwordHash string) error {
	_, err := r.db.ExecContext(ctx, "UPDATE users SET password_hash = ? WHERE id = ?", passwordHash, id)
	return err
}

// RecordLoginLog 记录登录审计日志到数据库。
//
// 使用 sqlx NamedExecContext 将 LoginLog 结构体映射插入 login_logs 表，
// 通过 :fieldname 命名参数绑定结构体字段。
//
// 该方法不返回 error（异常已内部忽略），因为登录日志写入失败不应影响主登录流程的成功返回。
// 采用"尽力而为"（best-effort）策略：日志写入失败静默忽略，后续可通过离线日志聚合分析补偿。
// 这是典型的非关键路径容错模式：避免辅助功能拖垮核心业务。
//
// 参数:
//   - ctx: 上下文
//   - log: 包含用户 ID、标识、登录渠道、IP、User-Agent、登录状态的 LoginLog 结构体指针
//
// 返回值: 无
//
// 边界情况:
//   - 数据库异常时静默忽略 error（_ 丢弃），不影响调用方的登录流程
func (r *AuthRepository) RecordLoginLog(ctx context.Context, log *LoginLog) {
	_, _ = r.db.NamedExecContext(ctx, `
INSERT INTO login_logs (user_id, identifier, channel, ip, user_agent, status)
VALUES (:user_id, :identifier, :channel, :ip, :user_agent, :status)
`, log)
}
