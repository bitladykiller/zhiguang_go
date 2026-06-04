package middleware

import (
	"time"

	"github.com/gin-gonic/gin"
	"go.uber.org/zap"
)

// LoggerMiddleware 返回一个基于 zap 的结构化日志 Gin 中间件。
//
// 记录的字段：
//   - method：HTTP 请求方法（GET/POST/PUT/DELETE 等）
//   - path：请求路径
//   - status：HTTP 响应状态码
//   - latency：请求总耗时（从接收到响应完成）
//   - client_ip：客户端 IP 地址
//   - user_agent：客户端 User-Agent
//   - body_size：响应体大小（字节）
//
// 日志级别策略：
//   - status >= 500：使用 logger.Error（服务端错误）
//   - status >= 400：使用 logger.Warn（客户端错误）
//   - status < 400：使用 logger.Info（正常请求）
//
// 噪声控制：跳过 /health 和 /metrics 等低价值接口的日志输出，
// 避免日志量过大影响问题排查。
func LoggerMiddleware(logger *zap.Logger) gin.HandlerFunc {
	return func(c *gin.Context) {
		// 跳过健康检查和指标接口
		path := c.Request.URL.Path
		if path == "/health" || path == "/metrics" {
			c.Next()
			return
		}

		start := time.Now()

		// 执行后续处理链
		c.Next()

		// 计算请求耗时
		latency := time.Since(start)

		// 组装日志字段
		fields := []zap.Field{
			zap.String("method", c.Request.Method),
			zap.String("path", path),
			zap.Int("status", c.Writer.Status()),
			zap.Duration("latency", latency),
			zap.String("client_ip", c.ClientIP()),
			zap.String("user_agent", c.Request.UserAgent()),
			zap.Int("body_size", c.Writer.Size()),
		}

		// 根据状态码选择日志级别：错误用 warn/error，成功用 info
		status := c.Writer.Status()
		if status >= 500 {
			logger.Error("request completed with server error", fields...)
		} else if status >= 400 {
			logger.Warn("request completed with client error", fields...)
		} else {
			logger.Info("request completed", fields...)
		}
	}
}
