package relation

import (
	"strconv"

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

func bindFollowRequest(c *gin.Context) (*FollowRequest, bool) {
	var req FollowRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		response.Fail(c, 400, "invalid request")
		return nil, false
	}
	return &req, true
}

func otherIDQuery(c *gin.Context) (uint64, bool) {
	otherID, err := strconv.ParseUint(c.Query("other_id"), 10, 64)
	if err != nil {
		response.Fail(c, 400, "invalid other_id")
		return 0, false
	}
	return otherID, true
}

// queryInt 从查询参数中解析整数，缺失或非法时返回默认值。
//
// 功能：与 knowpost/handler.go 中的 queryInt 功能相同，但额外校验返回值 > 0。
func queryInt(c *gin.Context, key string, def int) int {
	s := c.Query(key)
	if s == "" {
		return def
	}
	v, _ := strconv.Atoi(s)
	if v <= 0 {
		return def
	}
	return v
}

// queryInt64 从查询参数中解析 int64 值，缺失或非法时返回 0。
//
// 功能：用于解析游标值。游标是 int64 类型的毫秒时间戳。
func queryInt64(c *gin.Context, key string) int64 {
	s := c.Query(key)
	if s == "" {
		return 0
	}
	v, _ := strconv.ParseInt(s, 10, 64)
	return v
}

// queryUint64 从查询参数中解析 uint64 值，缺失或非法时返回 0。
//
// 功能：用于解析查询参数中的 user_id。
// 与 queryInt64 的区别在于返回值是无符号整型。
func queryUint64(c *gin.Context, key string) uint64 {
	s := c.Query(key)
	if s == "" {
		return 0
	}
	v, _ := strconv.ParseUint(s, 10, 64)
	return v
}
