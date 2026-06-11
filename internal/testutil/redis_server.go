package testutil

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
)

// StartRedisServer 启动一个仅供测试使用的临时 redis-server。
//
// 这里故意不用 mock Redis，因为当前项目有不少逻辑依赖真实 Redis 语义：
//   - Lua 脚本原子执行
//   - TTL / EXPIRE 行为
//   - offset / watermark 推进规则
//
// 因此这类测试更接近轻量集成测试。
func StartRedisServer(t *testing.T) *redis.Client {
	t.Helper()

	redisServerPath, err := exec.LookPath("redis-server")
	if err != nil {
		t.Skip("redis-server is not available in PATH")
	}

	workdir, err := os.MkdirTemp("/tmp", "redis-test-")
	if err != nil {
		t.Fatalf("create redis temp dir: %v", err)
	}
	t.Cleanup(func() {
		_ = os.RemoveAll(workdir)
	})
	socketPath := filepath.Join(workdir, "redis.sock")
	cmd := exec.Command(
		redisServerPath,
		"--save", "",
		"--appendonly", "no",
		"--port", "0",
		"--unixsocket", socketPath,
		"--unixsocketperm", "700",
		"--dir", filepath.Clean(workdir),
	)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	if err := cmd.Start(); err != nil {
		t.Fatalf("start redis-server: %v", err)
	}

	client := redis.NewClient(&redis.Options{
		Network: "unix",
		Addr:    socketPath,
	})

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	for {
		if err := client.Ping(ctx).Err(); err == nil {
			break
		}
		if ctx.Err() != nil {
			_ = cmd.Process.Kill()
			_ = cmd.Wait()
			t.Fatalf("redis-server not ready: %v", ctx.Err())
		}
		time.Sleep(50 * time.Millisecond)
	}

	t.Cleanup(func() {
		_ = client.Close()
		if cmd.Process != nil {
			_ = cmd.Process.Kill()
		}
		_ = cmd.Wait()
	})

	return client
}
