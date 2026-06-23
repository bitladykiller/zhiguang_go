// Package middleware 提供一组 Gin 中间件组件。
package middleware

import (
	"context"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
)

// TraceIDHeader 是向下游服务传递 trace id 的 HTTP 头名称。
const TraceIDHeader = "X-Trace-ID"

// contextKeyTraceID 是 Gin 上下文中存储 trace id 的键。
const contextKeyTraceID = "trace_id"

// TraceMiddleware 为每个请求注入 trace id，并设置请求超时。
//
// 功能：
//  1. 从请求头 X-Trace-ID 提取上游传入的 trace id，若无则生成新的 UUID。
//  2. 将 trace id 写入 Gin 上下文（供 handler 和日志使用）。
//  3. 将 trace id 通过 X-Trace-ID 响应头返回给客户端。
//  4. 创建带超时的 context，替换 c.Request.Context()。
//
// 参数：
//   - timeout: 请求超时时间。<=0 时不设置超时（依赖 Gin 自身超时控制）。
//
// 返回值：
//   - gin.HandlerFunc: 中间件函数
//
// 设计决策：
//
//	使用 google/uuid 而非 crypto/rand 生成 trace id，因为 UUID 更标准、
//	可读性更好，且性能足够。
//
//	将 trace id 写入响应头 X-Trace-ID，方便客户端在出错时提供该 ID 用于排查。
//
// 使用方式：
//
//	r.Use(middleware.TraceMiddleware(30 * time.Second))
func TraceMiddleware(timeout time.Duration) gin.HandlerFunc {
	return func(c *gin.Context) {
		// 提取或生成 trace id
		traceID := c.GetHeader(TraceIDHeader)
		if traceID == "" {
			traceID = uuid.New().String()
		}

		// 存入 Gin 上下文
		c.Set(contextKeyTraceID, traceID)

		// 返回给客户端
		c.Header(TraceIDHeader, traceID)

		// 设置请求超时
		if timeout > 0 {
			ctx, cancel := context.WithTimeout(c.Request.Context(), timeout)
			defer cancel()
			c.Request = c.Request.WithContext(ctx)
		}

		c.Next()
	}
}

// GetTraceID 从 Gin 上下文中提取 trace id。
//
// 参数：
//   - c: Gin 上下文
//
// 返回值：
//   - string: trace id（如果 TraceMiddleware 未启用则返回空字符串）
func GetTraceID(c *gin.Context) string {
	val, exists := c.Get(contextKeyTraceID)
	if !exists {
		return ""
	}
	traceID, ok := val.(string)
	if !ok {
		return ""
	}
	return traceID
}
