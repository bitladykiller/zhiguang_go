package auth

import (
	"github.com/gin-gonic/gin"
	"github.com/zhiguang/app/pkg/errcode"
	"github.com/zhiguang/app/pkg/middleware"
	"github.com/zhiguang/app/pkg/response"
)

func bindJSON(c *gin.Context, target interface{}) bool {
	if err := c.ShouldBindJSON(target); err != nil {
		response.Fail(c, 400, "invalid request: "+err.Error())
		return false
	}
	return true
}

func currentUserID(c *gin.Context) (uint64, bool) {
	userID, ok := middleware.GetUserID(c)
	if !ok {
		response.Error(c, errcode.ErrUnauthorized)
		return 0, false
	}
	return userID, true
}

// extractClientInfo 从 Gin 上下文中提取客户端网络信息（IP 和 User-Agent）。
//
// 提取的字段说明：
//
//	c.ClientIP()：
//	  自动处理 X-Forwarded-For、X-Real-IP 等代理转发头。
//	  如果请求经过反向代理（Nginx、ELB、Kong 等），返回的是原始客户端 IP 而非代理 IP。
//	  Gin 通过 TrustedProxies 配置控制信任的代理范围（默认信任全部内网代理）。
//
//	c.GetHeader("User-Agent")：
//	  获取 HTTP 请求头 User-Agent 的原始值。
//	  如果请求未携带此头，返回空字符串而非 error。
//
// 参数:
//   - c: Gin 上下文
//
// 返回值:
//   - ClientInfo: 包含 IP 和 UserAgent 字段的结构体，用于审计日志（RecordLoginLog）和业务处理
//
// 边界情况:
//   - 请求未携带 User-Agent 头时，该字段为空字符串，不影响流程正常执行
//   - 客户端 IP 可能为空（极罕见情况，如通过 Unix Socket 发起的请求）
//   - 对于 WebSocket 升级请求，c.ClientIP() 仍能正确提取
func extractClientInfo(c *gin.Context) ClientInfo {
	return ClientInfo{
		IP:        c.ClientIP(),
		UserAgent: c.GetHeader("User-Agent"),
	}
}
