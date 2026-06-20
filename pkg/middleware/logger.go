package middleware

import (
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	"go.uber.org/zap"
)

// LoggerMiddleware 返回一个基于 zap 的结构化日志 Gin 中间件。
//
// 功能：
//   记录每个 HTTP 请求的详细信息，包括方法、路径、状态码、耗时、
//   客户端 IP、User-Agent 和响应体大小。
//
// 参数：
//   - logger: zap.Logger 实例，用于输出结构化日志
//
// 返回值：
//   - gin.HandlerFunc: 日志中间件函数
//
// 记录的字段：
//   - method：    HTTP 请求方法（GET/POST/PUT/DELETE 等）
//   - path：      请求路径
//   - status：    HTTP 响应状态码
//   - latency：   请求总耗时（从接收到响应完成）
//   - client_ip： 客户端 IP 地址
//   - user_agent：客户端 User-Agent
//   - body_size： 响应体大小（字节）
//
// 日志级别策略：
//   - status >= 500：使用 logger.Error（服务端错误）
//   - status >= 400：使用 logger.Warn（客户端错误）
//   - status < 400：使用 logger.Info（正常请求）
//
// 噪声控制：
//   跳过 /health 和 /metrics 等低价值接口的日志输出，
//   避免日志量过大影响问题排查。
//
// 函数调用说明：
//   - time.Now():   记录请求开始时间
//   - time.Since(start):  计算从 start 到当前时间的差值（请求耗时）
//   - c.Request.URL.Path:  Gin 中获取请求路径
//   - c.Request.Method:    Gin 中获取 HTTP 方法
//   - c.Writer.Status():   Gin 中获取已写入的响应状态码
//   - c.ClientIP():        Gin 中获取客户端 IP（考虑代理转发）
//   - c.Request.UserAgent(): 获取客户端 User-Agent
//   - c.Writer.Size():     获取已写入的响应体大小
//
// 设计决策：
//   - 跳过 /health 和 /metrics：
//     这些接口由 K8s 探针和 Prometheus 频繁轮询，记录日志会淹没有效日志。
//   - 使用 zap.Field 而非格式化字符串：
//     结构化日志便于日志中心（如 ELK/Loki）做索引和聚合查询。
func LoggerMiddleware(logger *zap.Logger) gin.HandlerFunc {
		return func(c *gin.Context) {
		// 跳过健康检查和指标接口
		path := c.Request.URL.Path
		if path == "/health" || path == "/ready" || path == "/metrics" || strings.HasPrefix(path, "/debug/pprof") {
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
