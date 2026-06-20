// Package database 提供 MySQL（基于 sqlx）与 Redis（基于 go-redis）的连接工厂。
//
// 这些工厂函数会在启动装配流程（bootstrap.InitializeApp）中被直接调用。
// 连接池参数从 config 中读取，确保与 YAML 配置联动。
//
// 设计决策：
//   - 不使用 ORM，而是用 sqlx 做轻量映射，以获得对 SQL 的完全控制力。
//   - 连接池的三个核心参数（MaxOpenConns、MaxIdleConns、ConnMaxLifetime）
//     都会在 NewDB 中设置，避免走 Go 默认值（默认不限制，可能耗尽数据库连接数）。
//   - Redis 目前只使用一个 DB 实例，后续有压力瓶颈时可以考虑读写分离。
package database

import (
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/jmoiron/sqlx"
	"github.com/redis/go-redis/v9"
	"github.com/zhiguang/app/pkg/config"
)

// NewDB 根据传入的 DatabaseConfig 创建带连接池配置的 sqlx 数据库连接。
//
// 参数：
//   - cfg: MySQL 连接配置（主机、端口、用户名、密码、库名、连接池大小等）
//
// 返回值：
//   - *sqlx.DB: 数据库连接对象，包含已配置的连接池
//   - error: 如果连接或 Ping 失败则返回错误
//
// 函数调用说明：
//   - sqlx.Open("mysql", cfg.DSN()):
//     sqlx 是 Go 的 SQL 扩展库。Open 不会立即建立连接，只创建一个连接池实例。
//     第一个参数 "mysql" 是驱动名（需要 _ "github.com/go-sql-driver/mysql" 注册）。
//     第二个参数是 DSN 数据源连接串，格式为 user:password@tcp(host:port)/dbname?charset=utf8mb4&parseTime=True
//   - db.Ping():
//     真正建立连接并验证数据库是否可达。如果连接失败，立刻关闭 db 句柄并返回错误。
//   - db.SetMaxOpenConns() / SetMaxIdleConns():
//     控制连接池大小。
//     SetMaxOpenConns: 最大打开连接数（默认 0 表示不限制）
//     SetMaxIdleConns: 最大空闲连接数（默认 2）
//   - db.SetConnMaxLifetime():
//     设置连接的最大存活时间。超过此时间后连接会被优雅关闭并替换为新连接。
//     这是防止长时间运行的连接被 MySQL 服务端断开的重要措施。
//
// 超时配置：
//   - 连接超时、读超时、写超时已通过 cfg.DSN() 中的 DSN 参数传递给 MySQL 驱动。
func NewDB(cfg *config.DatabaseConfig) (*sqlx.DB, error) {
	db, err := sqlx.Open("mysql", cfg.DSN())
	if err != nil {
		return nil, err
	}
	if err := db.Ping(); err != nil {
		_ = db.Close()
		return nil, err
	}

	db.SetMaxOpenConns(cfg.MaxOpenConns)
	db.SetMaxIdleConns(cfg.MaxIdleConns)
	db.SetConnMaxLifetime(time.Duration(cfg.ConnMaxLifetime) * time.Second)

	return db, nil
}

// NewRedisClient 根据给定 RedisConfig 创建 go-redis 客户端。
//
// 参数：
//   - cfg: Redis 连接配置（主机、端口、密码、库编号、连接池大小）
//
// 返回值：
//   - *redis.Client: 已配置的 Redis 客户端实例
//
// 函数调用说明：
//   - redis.NewClient(&redis.Options{...}):
//     go-redis 库的客户端构造函数。Options 中的主要字段：
//     - Addr: Redis 地址（host:port 格式），由 cfg.Addr() 拼接
//     - Password: 认证密码（无密码时为空字符串）
//     - DB: 数据库编号（0-15），不同业务可以隔离到不同 db
//     - PoolSize: 连接池大小，默认 10/CPU
//     注意：NewClient 不会立即连接 Redis，而是在首次操作时懒加载建立连接。
//
// 超时配置：
//   - DialTimeout: 连接建立超时时间
//   - ReadTimeout: 读取响应超时时间
//   - WriteTimeout: 发送命令超时时间
//   - MinIdleConns: 最小空闲连接数，确保连接池预热
//   - MaxRetries: 命令执行失败时的最大重试次数
//   - ConnMaxLifetime: 连接最大生命周期（秒），超时后连接会被关闭重建
func NewRedisClient(cfg *config.RedisConfig) *redis.Client {
	opts := &redis.Options{
		Addr:     cfg.Addr(),
		Password: cfg.Password,
		DB:       cfg.DB,
		PoolSize: cfg.PoolSize,
	}

	// 设置超时和重试参数
	if cfg.DialTimeoutMs > 0 {
		opts.DialTimeout = time.Duration(cfg.DialTimeoutMs) * time.Millisecond
	}
	if cfg.ReadTimeoutMs > 0 {
		opts.ReadTimeout = time.Duration(cfg.ReadTimeoutMs) * time.Millisecond
	}
	if cfg.WriteTimeoutMs > 0 {
		opts.WriteTimeout = time.Duration(cfg.WriteTimeoutMs) * time.Millisecond
	}
	if cfg.MinIdleConns > 0 {
		opts.MinIdleConns = cfg.MinIdleConns
	}
	if cfg.MaxRetries > 0 {
		opts.MaxRetries = cfg.MaxRetries
	}
	if cfg.ConnMaxLifetime > 0 {
		opts.ConnMaxLifetime = time.Duration(cfg.ConnMaxLifetime) * time.Second
	}

	return redis.NewClient(opts)
}
