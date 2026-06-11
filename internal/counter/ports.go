package counter

import "context"

// CounterUseCase 定义计数 HTTP 层依赖的业务接口。
//
// 计数领域内部已经包含位图状态判定、Kafka 聚合消费、失败补偿等复杂实现，
// 但 HTTP 层只应依赖“点赞/收藏/查计数”这些稳定动作语义。
type CounterUseCase interface {
	Like(ctx context.Context, userID uint64, entityType, entityID string) (bool, error)
	Unlike(ctx context.Context, userID uint64, entityType, entityID string) (bool, error)
	Fav(ctx context.Context, userID uint64, entityType, entityID string) (bool, error)
	Unfav(ctx context.Context, userID uint64, entityType, entityID string) (bool, error)
	GetCounts(ctx context.Context, entityType, entityID string, metrics []string) (map[string]int32, error)
	IsLiked(ctx context.Context, userID uint64, entityType, entityID string) (bool, error)
	IsFaved(ctx context.Context, userID uint64, entityType, entityID string) (bool, error)
}
