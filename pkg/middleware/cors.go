package middleware

import (
	"time"

	"github.com/gin-contrib/cors"
	"github.com/gin-gonic/gin"
)

// CorsMiddleware 返回一个处理跨域资源共享的 Gin 中间件。
//
// 当前配置对开发环境较宽松，生产环境应进一步收紧允许来源。
// 具体来说应考虑以下调整：
//   - AllowOrigins 从 "*" 改为实际前端域名列表
//   - AllowCredentials 在 AllowOrigins 不为 "*" 时才能正常工作
//
// 配置项说明：
//   - AllowOrigins: "*" 表示允许所有来源（开发阶段）
//   - AllowMethods: 允许的 HTTP 方法，包含 REST API 常用方法
//   - AllowHeaders: 允许的自定义头部，包含登录场景必需的 Authorization
//   - MaxAge: 预检请求的缓存时间（12小时），减少 OPTIONS 请求次数
func CorsMiddleware() gin.HandlerFunc {
	return cors.New(cors.Config{
		AllowOrigins:     []string{"*"},
		AllowMethods:     []string{"GET", "POST", "PUT", "PATCH", "DELETE", "OPTIONS"},
		AllowHeaders:     []string{"Origin", "Content-Type", "Accept", "Authorization", "X-Requested-With"},
		ExposeHeaders:    []string{"Content-Length", "Content-Type"},
		AllowCredentials: true,
		MaxAge:           12 * time.Hour,
	})
}
