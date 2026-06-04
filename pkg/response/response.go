// Package response 为 Gin 处理器提供统一的 JSON 响应辅助函数。
// 这里使用 Go 泛型来生成类型安全、结构统一的 API 响应包裹体：
// { "code": 0, "message": "success", "data": T }
package response

import (
	"net/http"

	"github.com/gin-gonic/gin"
	"github.com/zhiguang/app/pkg/errcode"
)

// ApiResponse 是所有 API 接口统一使用的 JSON 响应结构。
// 当 Code == 0 时表示请求成功；非 0 则表示发生错误。
type ApiResponse[T any] struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
	Data    T      `json:"data,omitempty"`
}

// Success 写入一个 HTTP 200 响应，响应码固定为 code=0，并携带业务数据。
func Success[T any](c *gin.Context, data T) {
	c.JSON(http.StatusOK, ApiResponse[T]{
		Code:    0,
		Message: "success",
		Data:    data,
	})
}

// Created 写入一个 HTTP 201 响应，并返回给定的数据。
func Created[T any](c *gin.Context, data T) {
	c.JSON(http.StatusCreated, ApiResponse[T]{
		Code:    0,
		Message: "created",
		Data:    data,
	})
}

// NoContent 写入一个 HTTP 204 响应，不返回响应体。
func NoContent(c *gin.Context) {
	c.Status(http.StatusNoContent)
}

// Error 根据 AppError 中的业务错误码推导合适的 HTTP 状态码并返回错误响应。
// 响应体中只包含错误码和错误信息，不包含 data 字段。
func Error(c *gin.Context, appErr *errcode.AppError) {
	httpStatus := httpStatusFromCode(appErr.Code)
	c.AbortWithStatusJSON(httpStatus, ApiResponse[any]{
		Code:    int(appErr.Code),
		Message: appErr.Message,
	})
}

// Fail 使用显式指定的 HTTP 状态码和错误消息返回失败响应。
// 适用于参数校验失败等无法直接映射到预定义 AppError 的场景。
func Fail(c *gin.Context, httpStatus int, msg string) {
	c.AbortWithStatusJSON(httpStatus, ApiResponse[any]{
		Code:    httpStatus,
		Message: msg,
	})
}

// httpStatusFromCode 将 AppError 的业务错误码映射为最合适的 HTTP 状态码。
// WHY：错误码的号段（4xxxx 与 5xxxx）决定了 HTTP 状态类别，
// 这样客户端才能区分可自行修复的请求错误与需要重试的服务端错误。
func httpStatusFromCode(code errcode.ErrorCode) int {
	if code >= 1000 {
		code = code / 100
	}

	switch {
	case code == errcode.CodeSuccess:
		return http.StatusOK
	case code == errcode.CodeUnauthorized:
		return http.StatusUnauthorized
	case code == errcode.CodeForbidden:
		return http.StatusForbidden
	case code == errcode.CodeNotFound:
		return http.StatusNotFound
	case code == errcode.CodeConflict:
		return http.StatusConflict
	case code == errcode.CodeTooManyRequests:
		return http.StatusTooManyRequests
	case code >= 500:
		return http.StatusInternalServerError
	case code >= 400:
		return http.StatusBadRequest
	default:
		return http.StatusInternalServerError
	}
}
