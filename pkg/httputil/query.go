// Package httputil 提供 HTTP 相关的通用工具函数。
package httputil

import (
	"strconv"

	"github.com/gin-gonic/gin"
)

// QueryInt 从 Gin 查询参数中解析 int 值，缺失或非法时返回默认值。
//
// 参数：
//   - c: Gin 上下文
//   - key: 查询参数名
//   - def: 默认值
//
// 返回值：解析成功返回整数值，失败返回 def。
func QueryInt(c *gin.Context, key string, def int) int {
	s := c.Query(key)
	if s == "" {
		return def
	}
	v, err := strconv.Atoi(s)
	if err != nil || v <= 0 {
		return def
	}
	return v
}
