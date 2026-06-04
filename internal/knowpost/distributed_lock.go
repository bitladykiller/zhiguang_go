package knowpost

import (
	"context"
	"time"

	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
)

const (
	knowPostLockTTL           = 5 * time.Second
	knowPostLockRetryInterval = 50 * time.Millisecond
)

var releaseDistributedLockScript = redis.NewScript(`
if redis.call("GET", KEYS[1]) == ARGV[1] then
  return redis.call("DEL", KEYS[1])
end
return 0
`)

func acquireDistributedLock(ctx context.Context, client *redis.Client, lockKey string) (string, bool, error) {
	token := uuid.NewString()
	locked, err := client.SetNX(ctx, lockKey, token, knowPostLockTTL).Result()
	if err != nil || !locked {
		return "", locked, err
	}
	return token, true, nil
}

func releaseDistributedLock(client *redis.Client, lockKey, token string) {
	releaseCtx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	_, _ = releaseDistributedLockScript.Run(releaseCtx, client, []string{lockKey}, token).Result()
}

func sleepDistributedLockRetry(ctx context.Context) bool {
	timer := time.NewTimer(knowPostLockRetryInterval)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}
