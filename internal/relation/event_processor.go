package relation

import (
	"context"
	"fmt"
	"strconv"
	"time"

	"github.com/redis/go-redis/v9"
	"go.uber.org/zap"
)

// UserCounterUpdater defines the user-dimension counter update interface needed for relation event processing.
type UserCounterUpdater interface {
	IncrementFollowings(ctx context.Context, userID uint64, delta int) error
	IncrementFollowers(ctx context.Context, userID uint64, delta int) error
}

// EventProcessor processes relation events driven by canal-outbox.
//
// Responsible for consuming FollowCreated and FollowCanceled events and performing the following operations:
//  1. Idempotency check (10-minute deduplication window based on Redis SETNX)
//  2. Update the following/follower ZSet caches in Redis
//  3. Update the user-dimension follow/follower counts (via CounterService)
//
// WHY: Caches are not updated directly in the follow/unfollow API because the Redis cache may have expired,
// or the API request may have failed before the cache update. Event-driven asynchronous updates
// ensure eventual consistency of the follow/follower list caches.
type EventProcessor struct {
	redis   *redis.Client
	counter UserCounterUpdater
	logger  *zap.Logger
}

// NewEventProcessor creates a relation event processor instance.
//
// Function: initializes EventProcessor, requires a Redis client and a counter updater.
//
// Parameters:
//   - redisClient: *redis.Client, used for idempotency check and ZSet updates.
//   - counter: UserCounterUpdater, used to update user follow/follower counts.
//
// Returns: *EventProcessor, returns nil when redisClient is nil (to avoid panics on subsequent calls).
//
// Design decision:
//   Returning nil instead of panicking allows callers to safely consume messages
//   even when the event processor is not initialized (graceful degradation when config is incomplete).
func NewEventProcessor(redisClient *redis.Client, counter UserCounterUpdater, logger *zap.Logger) *EventProcessor {
	if redisClient == nil {
		return nil
	}
	return &EventProcessor{
		redis:   redisClient,
		counter: counter,
		logger:  logger,
	}
}

// Process processes relation events (FollowCreated / FollowCanceled), updating Redis ZSet and user counts.
//
// Function: consumes relation events driven by canal-outbox, performing the following operations:
//  1. Idempotency check (SETNX): based on a 10-minute deduplication key window, preventing duplicate processing.
//  2. Update Redis ZSet according to event type:
//     - FollowCreated: add relationship to both following ZSet and followers ZSet.
//     - FollowCanceled: remove the member from both ZSets.
//  3. Update follow/follower counts (via UserCounterUpdater interface).
//
// SETNX idempotency check notes:
//   - SetNX is the Redis "SET if Not eXists" command.
//   - Format: SetNX(ctx, "dedup:rel:{eventType}:{fromUserID}:{toUserID}:{relationID}", "1", 10min).
//   - Returns true for first-time processing; false if already processed (dedup hit), skip.
//   - The dedup window is 10 minutes, ensuring no duplicate processing within the consumer retry window.
//   - The dedup key includes relationID (optional), so that a FollowCreated and its corresponding FollowCanceled
//     have independent dedup keys and do not interfere with each other.
//
// Parameters:
//   - ctx: context.Context.
//   - evt: RelationEvent, contains event type and the two involved user IDs.
//
// Returns:
//   - error: returns error on idempotency check failure, Redis operation failure, or count update failure.
//
// Edge cases:
//   - p == nil: returns nil, allowing safe invocation when uninitialized.
//   - Unknown event type: silently skipped (no error).
func (p *EventProcessor) Process(ctx context.Context, evt RelationEvent) error {
	if p == nil {
		return nil
	}

	dedupeKey := fmt.Sprintf("dedup:rel:%s:%d:%d:%s", evt.EventType, evt.FromUserID, evt.ToUserID, relationIDValue(evt.RelationID))
	first, err := p.redis.SetNX(ctx, dedupeKey, "1", 10*time.Minute).Result()
	if err != nil {
		return err
	}
	if !first {
		return nil
	}

	switch evt.EventType {
	case "FollowCreated":
		now := float64(time.Now().UnixMilli())
		if err := p.redis.ZAdd(ctx, followingZSetKey(evt.FromUserID), redis.Z{Score: now, Member: strconv.FormatUint(evt.ToUserID, 10)}).Err(); err != nil {
			return err
		}
		if err := p.redis.ZAdd(ctx, followersZSetKey(evt.ToUserID), redis.Z{Score: now, Member: strconv.FormatUint(evt.FromUserID, 10)}).Err(); err != nil {
			return err
		}
		if _, err := p.redis.Expire(ctx, followingZSetKey(evt.FromUserID), 2*time.Hour).Result(); err != nil {
			p.logger.Warn("failed to set expire on following zset", zap.String("zsetKey", followingZSetKey(evt.FromUserID)), zap.Error(err))
		}
		if _, err := p.redis.Expire(ctx, followersZSetKey(evt.ToUserID), 2*time.Hour).Result(); err != nil {
			p.logger.Warn("failed to set expire on followers zset", zap.String("zsetKey", followersZSetKey(evt.ToUserID)), zap.Error(err))
		}
		if p.counter != nil {
			if err := p.counter.IncrementFollowings(ctx, evt.FromUserID, 1); err != nil {
				return err
			}
			if err := p.counter.IncrementFollowers(ctx, evt.ToUserID, 1); err != nil {
				return err
			}
		}
	case "FollowCanceled":
		if err := p.redis.ZRem(ctx, followingZSetKey(evt.FromUserID), strconv.FormatUint(evt.ToUserID, 10)).Err(); err != nil {
			return err
		}
		if err := p.redis.ZRem(ctx, followersZSetKey(evt.ToUserID), strconv.FormatUint(evt.FromUserID, 10)).Err(); err != nil {
			return err
		}
		if _, err := p.redis.Expire(ctx, followingZSetKey(evt.FromUserID), 2*time.Hour).Result(); err != nil {
			p.logger.Warn("failed to set expire on following zset", zap.String("zsetKey", followingZSetKey(evt.FromUserID)), zap.Error(err))
		}
		if _, err := p.redis.Expire(ctx, followersZSetKey(evt.ToUserID), 2*time.Hour).Result(); err != nil {
			p.logger.Warn("failed to set expire on followers zset", zap.String("zsetKey", followersZSetKey(evt.ToUserID)), zap.Error(err))
		}
		if p.counter != nil {
			if err := p.counter.IncrementFollowings(ctx, evt.FromUserID, -1); err != nil {
				return err
			}
			if err := p.counter.IncrementFollowers(ctx, evt.ToUserID, -1); err != nil {
				return err
			}
		}
	}

	return nil
}

// relationIDValue converts an optional relation ID pointer to a string for dedup key construction.
//
// Function: safe dereference of *uint64. Returns "0" for nil pointer.
//
// Parameters:
//   - id: *uint64, optional relation record ID.
//
// Returns: string, string representation of the ID; "0" for nil.
func relationIDValue(id *uint64) string {
	if id == nil {
		return "0"
	}
	return strconv.FormatUint(*id, 10)
}

// followingZSetKey generates the ZSet key for a user's following list.
//
// Function: format is "z:following:{userID}", consistent with zsetKey("following", userID) in relation/service.go.
//
// Parameters:
//   - userID: uint64, user ID.
//
// Returns: string, ZSet key name.
func followingZSetKey(userID uint64) string {
	return fmt.Sprintf("z:following:%d", userID)
}

// followersZSetKey generates the ZSet key for a user's followers list.
//
// Function: format is "z:followers:{userID}".
//
// Parameters:
//   - userID: uint64, user ID.
//
// Returns: string, ZSet key name.
func followersZSetKey(userID uint64) string {
	return fmt.Sprintf("z:followers:%d", userID)
}
