// Package errcode 定义统一的错误码以及 AppError 类型，
// 供各个服务层复用，以保证错误处理方式一致。
// 它与 zhiguang_be 中的 Java ErrorCode 枚举保持对应。
package errcode

import "fmt"

// ErrorCode 表示 API 响应中返回的数字错误码。
type ErrorCode int

const (
	// Success：表示无错误。
	CodeSuccess ErrorCode = 0

	// --- 4xx 客户端错误 ---

	// CodeBadRequest 表示请求参数格式错误或缺少必填字段。
	CodeBadRequest ErrorCode = 400
	// CodeUnauthorized 表示缺少访问令牌或令牌无效。
	CodeUnauthorized ErrorCode = 401
	// CodeForbidden 表示权限不足。
	CodeForbidden ErrorCode = 403
	// CodeNotFound 表示请求的资源不存在。
	CodeNotFound ErrorCode = 404
	// CodeConflict 表示资源冲突，例如标识已存在。
	CodeConflict ErrorCode = 409
	// CodeTooManyRequests 表示触发了限流。
	CodeTooManyRequests ErrorCode = 429

	// --- 鉴权相关业务错误（映射到 HTTP 状态码） ---

	// ErrCodeIdentifierExists：标识（手机号/邮箱）已被注册。
	ErrCodeIdentifierExists ErrorCode = 40901
	// ErrCodeIdentifierNotFound：登录时未找到对应标识。
	ErrCodeIdentifierNotFound ErrorCode = 40401
	// ErrCodeInvalidCredentials：密码不匹配或验证码错误。
	ErrCodeInvalidCredentials ErrorCode = 40101
	// ErrCodeRefreshTokenInvalid：刷新令牌已过期、被撤销或格式非法。
	ErrCodeRefreshTokenInvalid ErrorCode = 40102
	// ErrCodeTermsNotAccepted：用户注册时未同意条款。
	ErrCodeTermsNotAccepted ErrorCode = 40001
	// ErrCodeVerificationNotFound：验证码未发送过或已过期。
	ErrCodeVerificationNotFound ErrorCode = 40402
	// ErrCodeVerificationMismatch：验证码不匹配。
	ErrCodeVerificationMismatch ErrorCode = 40002
	// ErrCodeVerificationTooManyAttempts：触发了验证码暴力尝试保护。
	ErrCodeVerificationTooManyAttempts ErrorCode = 42901
	// ErrCodePasswordPolicyViolation：密码未满足强度要求。
	ErrCodePasswordPolicyViolation ErrorCode = 40003

	// --- 5xx 服务端错误 ---

	// CodeInternalError 表示未预期的内部错误。
	CodeInternalError ErrorCode = 500
)

// AppError 是统一错误类型，同时包含业务错误码和可读错误信息。
// 它实现了 error 接口，并被 response.Error() 用来构造统一格式的 JSON 错误响应。
type AppError struct {
	Code    ErrorCode `json:"code"`
	Message string    `json:"message"`
}

// Error 实现标准 error 接口。
func (e *AppError) Error() string {
	return fmt.Sprintf("[%d] %s", e.Code, e.Message)
}

// WithMsg 返回一个携带新错误消息的 AppError 副本。
// 适合在预定义错误码基础上追加具体上下文。
func (e *AppError) WithMsg(msg string) *AppError {
	return &AppError{Code: e.Code, Message: msg}
}

// NewAppError 根据给定错误码和错误信息创建新的 AppError。
func NewAppError(code ErrorCode, msg string) *AppError {
	return &AppError{Code: code, Message: msg}
}

// --- 预定义错误实例 ---
// 这些是常见错误场景下可直接复用的单例。
// 如需补充单次请求上下文，请使用 .WithMsg()，不要直接修改原对象。

var (
	ErrBadRequest      = &AppError{Code: CodeBadRequest, Message: "bad request"}
	ErrUnauthorized    = &AppError{Code: CodeUnauthorized, Message: "unauthorized"}
	ErrForbidden       = &AppError{Code: CodeForbidden, Message: "forbidden"}
	ErrNotFound        = &AppError{Code: CodeNotFound, Message: "not found"}
	ErrInternal        = &AppError{Code: CodeInternalError, Message: "internal server error"}
	ErrTooManyRequests = &AppError{Code: CodeTooManyRequests, Message: "too many requests"}

	ErrIdentifierExists            = &AppError{Code: ErrCodeIdentifierExists, Message: "identifier already exists"}
	ErrIdentifierNotFound          = &AppError{Code: ErrCodeIdentifierNotFound, Message: "identifier not found"}
	ErrInvalidCredentials          = &AppError{Code: ErrCodeInvalidCredentials, Message: "invalid credentials"}
	ErrRefreshTokenInvalid         = &AppError{Code: ErrCodeRefreshTokenInvalid, Message: "invalid refresh token"}
	ErrTermsNotAccepted            = &AppError{Code: ErrCodeTermsNotAccepted, Message: "terms not accepted"}
	ErrVerificationNotFound        = &AppError{Code: ErrCodeVerificationNotFound, Message: "verification code not found"}
	ErrVerificationMismatch        = &AppError{Code: ErrCodeVerificationMismatch, Message: "verification code mismatch"}
	ErrVerificationTooManyAttempts = &AppError{Code: ErrCodeVerificationTooManyAttempts, Message: "too many verification attempts"}
	ErrPasswordPolicyViolation     = &AppError{Code: ErrCodePasswordPolicyViolation, Message: "password policy violation"}
)
