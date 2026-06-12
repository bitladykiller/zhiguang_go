package counter

import (
	"strings"

	"github.com/gin-gonic/gin"
	"github.com/zhiguang/app/pkg/errcode"
	"github.com/zhiguang/app/pkg/middleware"
	"github.com/zhiguang/app/pkg/response"
)

func requireUserID(c *gin.Context) (uint64, bool) {
	userID, ok := middleware.GetUserID(c)
	if !ok {
		response.Error(c, errcode.ErrUnauthorized)
		return 0, false
	}
	return userID, true
}

func bindToggleRequest(c *gin.Context) (*ToggleRequest, bool) {
	var req ToggleRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		response.Fail(c, 400, "invalid request")
		return nil, false
	}
	return &req, true
}

func entityQuery(c *gin.Context) (string, string, bool) {
	entityType := c.Query("entity_type")
	entityID := c.Query("entity_id")
	if entityType == "" || entityID == "" {
		response.Fail(c, 400, "entity_type and entity_id are required")
		return "", "", false
	}
	return entityType, entityID, true
}

func metricsQuery(c *gin.Context) []string {
	return strings.Split(c.DefaultQuery("metrics", "like,fav"), ",")
}
