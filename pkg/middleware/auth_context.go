package middleware

import "github.com/gin-gonic/gin"

// GetUserID 从 Gin 上下文中提取已认证用户的 ID。
//
// 功能：
//
//	读取 Gin 上下文中的 user_id 值，并尝试将其转换为 uint64 类型。
//	支持多种数值类型（uint64、float64、int64、int），兼容 JSON 自动解析
//	和 JWT 服务返回的不同数值类型。
//
// 参数：
//   - c: Gin 上下文
//
// 返回值：
//   - uint64: 用户 ID（如果上下文中存在且类型可转换）
//   - bool:   true=成功获取用户 ID；false=上下文中没有用户 ID
//
// 边界情况：
//   - 上下文中不存在 user_id → 返回 0, false
//   - JSON 数字在多次序列化/反序列化后可能变为 float64，
//     因此需要显式支持 float64 → uint64 转换。
//   - 不支持的类型（如 string）→ 返回 0, false
//
// 兼容性说明：
//
//	Gin 的 Set/Get 是 interface{} 存取，不保留原始类型。
//	如果 user_id 设置时是 uint64，直接断言成功；
//	如果经过 JSON 编解码（如中间层 serialization），
//	可能变成 float64，需要额外处理。
func GetUserID(c *gin.Context) (uint64, bool) {
	val, exists := c.Get(string(ctxUserID))
	if !exists {
		return 0, false
	}

	switch v := val.(type) {
	case uint64:
		return v, true
	case float64:
		return uint64(v), true
	case int64:
		return uint64(v), true
	case int:
		return uint64(v), true
	default:
		return 0, false
	}
}
