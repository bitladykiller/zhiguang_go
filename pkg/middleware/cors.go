package middleware

import (
	"time"

	"github.com/gin-contrib/cors"
	"github.com/gin-gonic/gin"
)

// CorsMiddleware 返回一个处理跨域资源共享的 Gin 中间件。
//
// 功能：
//   通过 gin-contrib/cors 库创建 CORS 中间件，配置允许的来源、方法、
//   请求头、暴露的响应头和预检缓存时间。
//
// 参数：
//   - origins: 允许的来源域名列表。为空时默认使用 ["*"]。
//
// 配置项说明：
//   - AllowMethods: 允许的 HTTP 方法，包含 REST API 常用方法
//   - AllowHeaders: 允许的自定义头部，包含登录场景必需的 Authorization
//   - MaxAge: 预检请求的缓存时间（12小时），减少 OPTIONS 请求次数
//
// 返回值：
//   - gin.HandlerFunc: CORS 中间件函数
func CorsMiddleware(origins []string) gin.HandlerFunc {
	if len(origins) == 0 {
		origins = []string{"*"}
	}

	allowCredentials := true
	for _, o := range origins {
		if o == "*" {
			allowCredentials = false
			break
		}
	}

	config := cors.Config{
		AllowMethods:     []string{"GET", "POST", "PUT", "PATCH", "DELETE", "OPTIONS"},
		AllowHeaders:     []string{"Origin", "Content-Type", "Accept", "Authorization", "X-Requested-With"},
		ExposeHeaders:    []string{"Content-Length", "X-RateLimit-Remaining"},
		AllowCredentials: allowCredentials,
		MaxAge:           12 * time.Hour,
	}
	if allowCredentials {
		config.AllowOrigins = origins
	} else {
		config.AllowAllOrigins = true
	}
	return cors.New(config)
}
