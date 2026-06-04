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
func NewRedisClient(cfg *config.RedisConfig) *redis.Client {
	return redis.NewClient(&redis.Options{
		Addr:     cfg.Addr(),
		Password: cfg.Password,
		DB:       cfg.DB,
		PoolSize: cfg.PoolSize,
	})
}
