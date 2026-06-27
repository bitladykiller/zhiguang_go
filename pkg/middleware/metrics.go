package middleware

import (
	"strconv"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/zhiguang/app/pkg/metrics"
)

func MetricsMiddleware() gin.HandlerFunc {
	return func(c *gin.Context) {
		start := time.Now()
		path := c.Request.URL.Path

		c.Next()

		duration := time.Since(start).Seconds()
		status := strconv.Itoa(c.Writer.Status())

		metrics.HttpRequestTotal.WithLabelValues(c.Request.Method, path, status).Inc()
		metrics.HttpRequestDuration.WithLabelValues(c.Request.Method, path).Observe(duration)
	}
}