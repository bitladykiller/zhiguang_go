// Package testutil 提供测试公共工具函数。
package testutil

import (
	"testing"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
)

// StartTestRedis 启动 miniredis 实例并返回 go-redis 客户端。
// 调用方通过 t.Cleanup 自动关闭 miniredis。
func StartTestRedis(t *testing.T) *redis.Client {
	t.Helper()
	mr := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{
		Addr: mr.Addr(),
	})
	t.Cleanup(func() {
		rdb.Close()
	})
	return rdb
}