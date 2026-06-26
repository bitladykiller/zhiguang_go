// Package middleware 提供一组 Gin 中间件组件。
package middleware

import (
	"github.com/gin-gonic/gin"
	"github.com/zhiguang/app/pkg/errcode"
	"go.uber.org/zap"
)

// ErrorLogMiddleware 记录 handler 中通过 c.Error(err) 报告的错误。
//
// 使用方式：
//
//	r.Use(middleware.ErrorLogMiddleware(logger))
//
// Handler 中用法：
//
//	if err != nil {
//	    _ = c.Error(err)  // 将错误传递给日志中间件
//	    response.Error(c, errcode.ErrInternal)
//	    return
//	}
func ErrorLogMiddleware(logger *zap.Logger) gin.HandlerFunc {
	return func(c *gin.Context) {
		c.Next()

		// 从 Gin 的错误列表中提取并记录
		errs := c.Errors
		if len(errs) == 0 {
			return
		}

		for _, e := range errs {
			if e.Err != nil {
				logger.Error("handler error",
					zap.Error(e.Err),
				)
			}
		}
	}
}

// RecordError 将原始 error 存入 Gin 上下文，供 ErrorLogMiddleware 记录。
//
// 用法：
//
//	if err != nil {
//	    middleware.RecordError(c, err)
//	    response.Error(c, errcode.ErrInternal)
//	    return
//	}
//
// WHY 用这个而不是直接调用 c.Error()：
//   - 封装 Gin 的 c.Error 调用，避免每个 handler 都记住 Gin 的错误收集 API。
func RecordError(c *gin.Context, err error) {
	if err != nil {
		_ = c.Error(err)
	}
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
//	handler 通过 ToAppErr 统一处理，确保非业务错误不会泄露内部细节。
func ToAppErr(err error) *errcode.AppError {
	if err == nil {
		return errcode.ErrInternal.WithMsg("nil error")
	}
	if appErr, ok := err.(*errcode.AppError); ok {
		return appErr
	}
	return errcode.ErrInternal.WithMsg(err.Error())
}
