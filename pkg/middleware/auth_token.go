package middleware

import (
	"strings"

	"github.com/gin-gonic/gin"
)

// extractBearerToken 从 Authorization 请求头中提取 Token。
//
// 功能：
//
//	解析 HTTP 请求头 "Authorization: Bearer <token>"，
//	返回 token 部分（去掉 "Bearer " 前缀）。
//
// 参数：
//   - c: Gin 上下文
//
// 返回值：
//   - string: 裸 token 字符串。如果请求头缺失或格式不合法则返回空字符串。
//
// 函数调用说明：
//   - c.GetHeader("Authorization"):
//     Gin 的方法，从 HTTP 请求中获取指定头部字段的值。
//     不区分大小写（Gin/Go 的 HTTP 库标准化头部名称）。
//   - strings.SplitN(header, " ", 2):
//     标准库字符串分割函数。按空格分割为最多 2 部分：
//     parts[0] 是类型（"Bearer"），parts[1] 是 token 值。
//   - strings.EqualFold(a, b):
//     不区分大小写的字符串比较。确保 "bearer"、"Bearer"、"BEARER" 都匹配。
//   - strings.TrimSpace(s):
//     去除字符串首尾空白字符。
//
// 边界情况：
//   - Authorization 头缺失 → 返回 ""
//   - Authorization 头只有 "Bearer" 没有 token → 返回 ""
//   - 非 Bearer 类型（如 "Basic xxx"）→ 返回 ""
//   - Token 值两侧有空格 → TrimSpace 处理
func extractBearerToken(c *gin.Context) string {
	header := c.GetHeader("Authorization")
	if header == "" {
		return ""
	}

	parts := strings.SplitN(header, " ", 2)
	if len(parts) != 2 || !strings.EqualFold(parts[0], "Bearer") {
		return ""
	}

	return strings.TrimSpace(parts[1])
}
