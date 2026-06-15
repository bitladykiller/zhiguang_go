package testutil

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
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
	logPath := filepath.Join(workdir, "redis.log")
	logFile, err := os.Create(logPath)
	if err != nil {
		t.Fatalf("create redis log file: %v", err)
	}
	t.Cleanup(func() {
		_ = logFile.Close()
	})
	cmd := exec.Command(
		redisServerPath,
		"--save", "",
		"--appendonly", "no",
		"--port", "0",
		"--unixsocket", socketPath,
		"--unixsocketperm", "700",
		"--dir", filepath.Clean(workdir),
	)
	cmd.Stdout = logFile
	cmd.Stderr = logFile
	if err := cmd.Start(); err != nil {
		t.Fatalf("start redis-server: %v", err)
	}
	waitCh := make(chan error, 1)
	go func() {
		waitCh <- cmd.Wait()
	}()

	client := redis.NewClient(&redis.Options{
		Network: "unix",
		Addr:    socketPath,
	})

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	ticker := time.NewTicker(50 * time.Millisecond)
	defer ticker.Stop()

	for {
		if err := client.Ping(ctx).Err(); err == nil {
			break
		}

		select {
		case err := <-waitCh:
			_ = client.Close()
			logOutput := readRedisServerLog(logPath)
			if shouldSkipRedisServerStart(logOutput) {
				t.Skipf("redis-server cannot start in this environment: %v", strings.TrimSpace(logOutput))
			}
			t.Fatalf("redis-server exited before ready: %v, stderr: %s", err, strings.TrimSpace(logOutput))
		case <-ticker.C:
		case <-ctx.Done():
			_ = client.Close()
			if cmd.Process != nil {
				_ = cmd.Process.Kill()
			}
			<-waitCh
			logOutput := readRedisServerLog(logPath)
			if shouldSkipRedisServerStart(logOutput) {
				t.Skipf("redis-server cannot start in this environment: %v", strings.TrimSpace(logOutput))
			}
			t.Fatalf("redis-server not ready: %v, stderr: %s", ctx.Err(), strings.TrimSpace(logOutput))
		}
	}

	t.Cleanup(func() {
		_ = client.Close()
		if cmd.Process != nil {
			_ = cmd.Process.Kill()
		}
		<-waitCh
	})

	return client
}

func shouldSkipRedisServerStart(stderr string) bool {
	msg := strings.ToLower(stderr)
	return strings.Contains(msg, "operation not permitted") ||
		strings.Contains(msg, "failed opening unix socket") ||
		strings.Contains(msg, "permission denied")
}

func readRedisServerLog(path string) string {
	data, err := os.ReadFile(path)
	if err != nil {
		return ""
	}
	return string(data)
}
