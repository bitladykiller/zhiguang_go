package knowpost

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

func optionalUserID(c *gin.Context) *uint64 {
	if userID, ok := middleware.GetUserID(c); ok {
		return &userID
	}
	return nil
}

func parseKnowPostID(c *gin.Context) (uint64, bool) {
	id, err := strconv.ParseUint(c.Param("id"), 10, 64)
	if err != nil {
		response.Fail(c, 400, "invalid id")
		return 0, false
	}
	return id, true
}

// queryInt 从请求查询参数中解析整数值，解析失败时返回默认值。
//
// 功能：安全地从查询字符串中读取整数参数。
// 如果参数缺失或无法解析为整数，返回给定的默认值。
//
// 参数：
//   - c: *gin.Context，当前请求上下文。
//   - key: string，查询参数名。
//   - defaultVal: int，解析失败时的默认值。
//
// 返回值：
//   - int: 解析成功返回整数，失败返回 defaultVal。
func queryInt(c *gin.Context, key string, defaultVal int) int {
	s := c.Query(key)
	if s == "" {
		return defaultVal
	}
	v, err := strconv.Atoi(s)
	if err != nil {
		return defaultVal
	}
	return v
}

// toAppErr 将任意 error 转换为 *errcode.AppError。
//
// 功能：如果原始错误已经是 AppError 类型，直接原样返回。
// 如果是其他类型的 error（如数据库查询错误），包装为 ErrInternal。
//
// 这样设计的原因：
//
//	服务层的业务逻辑可能返回 *errcode.AppError（如 ErrNotFound、ErrForbidden），
//	也可能返回普通的 error（如数据库连接错误）。在转换成 HTTP 响应时，
//	handler 通过 toAppErr 统一处理，确保非业务错误不会泄露内部细节。
//
// 参数：
//   - err: error，原始错误。
//
// 返回值：*errcode.AppError，始终非 nil。
func toAppErr(err error) *errcode.AppError {
	if appErr, ok := err.(*errcode.AppError); ok {
		return appErr
	}
	return errcode.ErrInternal.WithMsg(err.Error())
}
