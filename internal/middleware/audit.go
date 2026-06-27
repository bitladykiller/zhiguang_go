package middleware

import (
	"github.com/gin-gonic/gin"
	"github.com/zhiguang/app/pkg/audit"
	pkgmw "github.com/zhiguang/app/pkg/middleware"
)

type AuditMiddleware struct {
	auditLogger *audit.AuditLogger
}

func NewAuditMiddleware(auditLogger *audit.AuditLogger) *AuditMiddleware {
	return &AuditMiddleware{auditLogger: auditLogger}
}

func (m *AuditMiddleware) Audit(action audit.Action, resourceType string) gin.HandlerFunc {
	return func(c *gin.Context) {
		c.Next()

		userID, exists := pkgmw.GetUserID(c)
		if !exists {
			return
		}

		resourceID := c.Param("id")
		if resourceID == "" {
			resourceID = c.Query("id")
		}

		m.auditLogger.LogAction(c.Request.Context(), action, int64(userID), resourceType, resourceID, c.Request.Method+" "+c.Request.URL.Path)
	}
}