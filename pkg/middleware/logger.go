package middleware

import (
	"time"

	"github.com/gin-gonic/gin"
	"go.uber.org/zap"
)

// LoggerMiddleware 返回一个基于 zap 的 Gin 日志中间件。
// 它会记录请求方法、路径、状态码、耗时、客户端 IP 和 User-Agent。
// 为减少噪声，会跳过健康检查等低价值接口。
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
