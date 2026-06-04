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
//
// 参数：
//   - 无（当前使用硬编码配置）
//
// 返回值：
//   - gin.HandlerFunc: CORS 中间件函数
//
// 函数调用说明：
//   - cors.New(config):
//     gin-contrib/cors 库提供的 CORS 中间件工厂函数。
//     传入 cors.Config 结构体来配置策略。
//
// 边界情况 / 生产环境注意事项：
//   - AllowOrigins[]{"*"} 与 AllowCredentials(true) 不兼容：
//     浏览器在 AllowCredentials=true 时会忽略 AllowOrigins="*"。
//     生产环境建议改为显式的域名白名单：
//       AllowOrigins: []string{"https://your-app.com"}
//     或者使用 AllowedOriginFunc 做动态判断。
//   - AllowAllOrigins 为 true 时，实际不会发送 Access-Control-Allow-Origin: *
//     头，而是会回显请求的 Origin 值（配合 AllowCredentials）。
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
