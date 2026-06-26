// Package response 为 Gin 处理器提供统一的 JSON 响应辅助函数。
//
// 设计决策：
//   - 使用 Go 泛型来生成类型安全的 API 响应包裹体，避免每次调用都做类型断言。
//   - 所有 API 接口统一使用 { "code": 0, "message": "success", "data": T } 格式，
//     其中 code == 0 表示成功，非 0 表示错误。
//   - Error() 函数会从业务错误码自动推导合适的 HTTP 状态码，
//     避免每个 handler 都写一次状态码映射逻辑。
//
// 响应码与 HTTP 状态码的映射规则：
//   0       → 200 OK
//   4xx     → 对应的 4xx 状态码
//   5xxxx   → 500 Internal Server Error（业务层 5 位错误码，从 50000 开始）
//   5xx     → 500 Internal Server Error（通用服务端错误）
package response

import (
	"net/http"

	"github.com/gin-gonic/gin"
	"github.com/zhiguang/app/pkg/errcode"
)

const (
	ResponseMessageSuccess = "success"
	ResponseMessageCreated = "created"
)

// ApiResponse 是所有 API 接口统一使用的 JSON 响应结构。
// 当 Code == 0 时表示请求成功；非 0 则表示发生错误。
type ApiResponse[T any] struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
	Data    T      `json:"data,omitempty"`
}

// Success 写入 HTTP 200 OK 响应，响应码固定为 0，并携带业务数据。
//
// 参数:
//   - c: Gin 上下文
//   - data: 任意类型的业务数据，通过泛型 T 确保类型安全
//
// 响应体格式:
//   { "code": 0, "message": "success", "data": T }
//
// 使用说明:
//   Go 1.18+ 泛型使调用方无需做类型断言：
//   response.Success(c, user)           // data 为 *auth.User
//   response.Success(c, gin.H{...})     // data 为 map
//   response.Success(c, []string{...})  // data 为 slice
//
// 边界情况:
//   - data 为 nil 时 JSON 序列化为 null（而非缺失字段）
//   - 如果 data 实现了 json.Marshaler 接口，使用自定义序列化逻辑
func Success[T any](c *gin.Context, data T) {
	c.JSON(http.StatusOK, ApiResponse[T]{
		Code:    0,
		Message: ResponseMessageSuccess,
		Data:    data,
	})
}

// Created 写入 HTTP 201 Created 响应，用于资源创建成功后的响应。
//
// 参数:
//   - c: Gin 上下文
//   - data: 任意类型的业务数据（如新创建的资源 ID 或完整资源信息）
//
// 响应体格式:
//   { "code": 0, "message": "created", "data": T }
//
// 使用场景:
//   - 用户注册成功
//   - 知文发布成功
//   - 预签名 URL 生成成功（storage handler）
//
// 与 Success 的区别:
//   HTTP 状态码为 201 而非 200，语义上表示"资源已创建"而非"请求已处理"。
func Created[T any](c *gin.Context, data T) {
	c.JSON(http.StatusCreated, ApiResponse[T]{
		Code:    0,
		Message: ResponseMessageCreated,
		Data:    data,
	})
}

// NoContent 写入 HTTP 204 No Content 响应，不返回响应体。
//
// 参数:
//   - c: Gin 上下文
//
// 使用场景:
//   - 删除操作成功（DELETE 方法）
//   - 某些更新操作不需要返回数据
//
// 说明:
//   使用 c.Status() 而非 c.JSON()，确保不输出响应体。
//   HTTP 204 规范要求响应体必须为空。
func NoContent(c *gin.Context) {
	c.Status(http.StatusNoContent)
}

// Error 根据 AppError 中的业务错误码自动推导合适的 HTTP 状态码并返回错误响应。
//
// 参数:
//   - c: Gin 上下文
//   - appErr: 包含业务错误码和错误消息的 AppError 实例
//
// 响应体格式:
//   { "code": appErr.Code, "message": appErr.Message }
//   注意不包含 data 字段（错误场景下无业务数据）
//
// 状态码推导:
//   通过 httpStatusFromCode 函数自动映射：
//   - 0       → 200 OK（理论上不会走入 Error 分支）
//   - 4xx     → 对应 HTTP 4xx
//   - 5xxxx   → 先归一为前 3 位再按 3 位码映射
//   - 5xx     → 500 Internal Server Error
//
// 设计说明:
//   使用 c.AbortWithStatusJSON 而非 c.JSON，确保后续中间件（如日志记录）
//   能感知到请求已被中止（Aborted 状态），不会继续执行后续 handler 链。
func Error(c *gin.Context, appErr *errcode.AppError) {
	httpStatus := errcode.HTTPStatusFromCode(appErr.Code)
	c.AbortWithStatusJSON(httpStatus, ApiResponse[any]{
		Code:    int(appErr.Code),
		Message: appErr.Message,
	})
}

// Fail 使用显式指定的 HTTP 状态码和错误消息返回失败响应。
//
// 参数:
//   - c: Gin 上下文
//   - httpStatus: 显式指定的 HTTP 状态码（如 400、503 等）
//   - msg: 错误描述信息
//
// 响应体格式:
//   { "code": httpStatus, "message": msg }
//
// 与 Error 的区别:
//   - Fail: 手动指定 HTTP 状态码和消息字符串，适合无法映射到预定义 AppError 的场景
//   - Error: 根据 AppError 自动推导状态码，适合已定义业务错误码的场景
//
// 使用场景:
//   - handler 层参数校验失败（400）
//   - 可选服务未初始化（503）
//   - 请求体 JSON 解析失败（400）
//
// 注意:
//   虽然 code 字段的值等于 HTTP 状态码，但客户端应使用 code 而非 HTTP 状态码来判断业务结果。
func Fail(c *gin.Context, httpStatus int, msg string) {
	c.AbortWithStatusJSON(httpStatus, ApiResponse[any]{
		Code:    httpStatus,
		Message: msg,
	})
}