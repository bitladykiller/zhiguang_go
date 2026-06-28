package counter

import "context"

// CounterServiceInterface 定义计数器模块对外暴露的业务方法。
//
// Handler 依赖此接口而非具体 *CounterService，使得 handler 可以独立于
// service 实现进行单元测试。
type CounterServiceInterface interface {
	Like(ctx context.Context, userID uint64, entityType, entityID string) (bool, error)
	Unlike(ctx context.Context, userID uint64, entityType, entityID string) (bool, error)
	Fav(ctx context.Context, userID uint64, entityType, entityID string) (bool, error)
	Unfav(ctx context.Context, userID uint64, entityType, entityID string) (bool, error)
	GetCounts(ctx context.Context, entityType, entityID string, metrics []string) (map[string]int32, error)
	GetCountsBatch(ctx context.Context, entityType string, entityIDs, metrics []string) (map[string]map[string]int32, error)
	IsLiked(ctx context.Context, userID uint64, entityType, entityID string) (bool, error)
	IsFaved(ctx context.Context, userID uint64, entityType, entityID string) (bool, error)
	BatchIsLiked(ctx context.Context, userID uint64, entityType string, entityIDs []string) (map[string]bool, error)
	BatchIsFaved(ctx context.Context, userID uint64, entityType string, entityIDs []string) (map[string]bool, error)
	GetLikers(ctx context.Context, entityType string, entityID uint64, metric string, cursor uint64, limit int) (*LikersResponse, error)
}

// 编译期断言：*CounterService 实现了 CounterServiceInterface。
var _ CounterServiceInterface = (*CounterService)(nil)