package httputil

import (
	"github.com/gin-gonic/gin"
)

// Pagination 封装通用的分页查询参数。
type Pagination struct {
	Page int // 页码，从 1 开始
	Size int // 每页数量
}

// ParsePagination 从 Gin 查询参数中解析分页参数。
//
// 参数：
//   - c: Gin 上下文
//   - pageKey: 页码参数名（通常为 "page"）
//   - sizeKey: 每页数量参数名（通常为 "size"）
//   - defaultPage: 默认页码
//   - defaultSize: 默认每页数量
//
// 返回值：
//   - Pagination: 包含解析后的页码和每页数量
//
// 边界情况：
//   - 参数缺失或非法时使用默认值
//   - page <= 0 时强制为 1
//   - size <= 0 时强制为默认值
func ParsePagination(c *gin.Context, pageKey, sizeKey string, defaultPage, defaultSize int) Pagination {
	page := QueryInt(c, pageKey, defaultPage)
	size := QueryInt(c, sizeKey, defaultSize)
	if page <= 0 {
		page = 1
	}
	if size <= 0 {
		size = defaultSize
	}
	return Pagination{Page: page, Size: size}
}

// LimitOffset 封装 offset 风格的分页参数。
type LimitOffset struct {
	Limit  int // 返回数量
	Offset int // 偏移量
}

// ParseLimitOffset 从 Gin 查询参数中解析 limit/offset 风格的分页参数。
//
// 参数：
//   - c: Gin 上下文
//   - limitKey: 数量参数名（通常为 "limit"）
//   - offsetKey: 偏移量参数名（通常为 "offset"）
//   - defaultLimit: 默认数量
//   - defaultOffset: 默认偏移量
//
// 返回值：
//   - LimitOffset: 包含解析后的 limit 和 offset
func ParseLimitOffset(c *gin.Context, limitKey, offsetKey string, defaultLimit, defaultOffset int) LimitOffset {
	limit := QueryInt(c, limitKey, defaultLimit)
	offset := QueryInt(c, offsetKey, defaultOffset)
	if limit <= 0 {
		limit = defaultLimit
	}
	if offset < 0 {
		offset = 0
	}
	return LimitOffset{Limit: limit, Offset: offset}
}
