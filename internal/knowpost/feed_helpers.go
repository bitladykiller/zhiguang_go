package knowpost

import (
	"context"
	"fmt"
)

// 本文件包含 Feed 服务使用的辅助工具函数。
// 这些函数被 KnowPostFeedService 内部调用，独立于此以便测试和复用。

// clamp 将整数 v 限制在 [lo, hi] 范围内。
//
// 功能：将给定的整数 v 限制在 [lo, hi] 闭区间内。
// 常用于分页查询中限制 page 和 pageSize 的合法范围。
//
// 参数：
//   - v: int，要限制的值。
//   - lo: int，下限（包含）。
//   - hi: int，上限（包含）。
//
// 返回值：
//   - int，限制在 [lo, hi] 范围内的值。
//
// 边界情况：
//   - v < lo 返回 lo。
//   - v > hi 返回 hi。
//   - lo <= v <= hi 返回 v。
func clamp(v, lo, hi int) int {
	if v < lo {
		return lo
	}
	if v > hi {
		return hi
	}
	return v
}

// max 返回两个整数中的较大者。
//
// 功能：用于确保页码最小值为 1。
// Go 标准库 math.Max 只支持 float64，这里提供 int 版本以避免类型转换。
//
// 参数：
//   - a: int，第一个值。
//   - b: int，第二个值。
//
// 返回值：int，a 和 b 中较大的值。
func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}

// boolToStr 将布尔值转换为 Redis 易于存储的字符串 "1" 或 "0"。
//
// 功能：Redis 的字符串值不能直接存储 Go 的 bool 类型，
// 此函数将 true 映射为 "1"、false 映射为 "0"。
// 读取时通过检查字符串是否等于 "1" 来还原布尔值。
//
// 参数：
//   - b: bool，输入的布尔值。
//
// 返回值：string，"1"（true）或 "0"（false）。
func boolToStr(b bool) string {
	if b {
		return "1"
	}
	return "0"
}

// currentPublicFeedVersion 返回公共 Feed 的当前版本号。
//
// 功能：从 Redis 读取 "feed:public:version" 键的值。
// 若该键不存在或值 <= 0，返回默认版本号 1。
// 每次有任意知文发生变更（发布、编辑、删除等）时，此版本号会递增。
func (s *KnowPostFeedService) currentPublicFeedVersion(ctx context.Context) int64 {
	return s.feedVersion(ctx, publicFeedVersionKey)
}

// currentMineFeedVersion 返回指定用户的"我的 Feed"当前版本号。
//
// 功能：从 Redis 读取 "feed:mine:version:{userID}" 键的值。
// 每次该用户自己的知文发生变更时，此版本号会递增。
//
// 参数：
//   - ctx: context.Context。
//   - userID: uint64，用户 ID。
//
// 返回值：int64，当前版本号。若不存在或无效则返回 1。
func (s *KnowPostFeedService) currentMineFeedVersion(ctx context.Context, userID uint64) int64 {
	return s.feedVersion(ctx, fmt.Sprintf(mineFeedVersionKey, userID))
}

// feedVersion 通用的 Feed 版本号读取函数。
//
// 功能：从 Redis 读取指定 key 的整数值作为版本号。
// Redis GET 返回字符串，通过 Int64() 解析为 int64。
// 若 key 不存在、值不是合法整数或值 <= 0，返回 1（默认版本）。
//
// 参数：
//   - ctx: context.Context。
//   - key: string，Redis 键名，如 "feed:public:version" 或 "feed:mine:version:{userID}"。
//
// 返回值：int64，当前版本号。默认返回 1。
func (s *KnowPostFeedService) feedVersion(ctx context.Context, key string) int64 {
	version, err := s.redis.Get(ctx, key).Int64()
	if err == nil && version > 0 {
		return version
	}
	return 1
}
