package errcode

import "fmt"

// ErrorCode 定义业务错误码类型，底层为 int。
// 0 表示成功，4xx/5xx 系列映射到对应 HTTP 状态码。
type ErrorCode int

const (
	CodeSuccess                ErrorCode = 0
	CodeBadRequest             ErrorCode = 400
	CodeUnauthorized           ErrorCode = 401
	CodeForbidden              ErrorCode = 403
	CodeNotFound               ErrorCode = 404
	CodeConflict               ErrorCode = 409
	CodeTooManyRequests        ErrorCode = 429
	CodeInternalError          ErrorCode = 500

	ErrCodeIdentifierExists            ErrorCode = 40901
	ErrCodeIdentifierNotFound          ErrorCode = 40401
	ErrCodeInvalidCredentials          ErrorCode = 40101
	ErrCodeRefreshTokenInvalid         ErrorCode = 40102
	ErrCodeTermsNotAccepted            ErrorCode = 40001
	ErrCodeVerificationNotFound        ErrorCode = 40402
	ErrCodeVerificationMismatch        ErrorCode = 40002
	ErrCodeVerificationTooManyAttempts ErrorCode = 42901
	ErrCodeServiceUnavailable          ErrorCode = 503
)

// AppError 是统一的业务错误类型，包含错误码和消息。
// 实现 error 接口，便于在业务层直接返回与 HTTP 层统一处理。
type AppError struct {
	Code    ErrorCode `json:"code"`
	Message string    `json:"message"`
}

func (e *AppError) Error() string {
	return fmt.Sprintf("[%d] %s", e.Code, e.Message)
}

func (e *AppError) WithMsg(msg string) *AppError {
	return &AppError{Code: e.Code, Message: msg}
}

var (
	ErrBadRequest      = &AppError{Code: CodeBadRequest, Message: "bad request"}
	ErrUnauthorized    = &AppError{Code: CodeUnauthorized, Message: "unauthorized"}
	ErrForbidden       = &AppError{Code: CodeForbidden, Message: "forbidden"}
	ErrNotFound        = &AppError{Code: CodeNotFound, Message: "not found"}
	ErrInternal        = &AppError{Code: CodeInternalError, Message: "internal server error"}
	ErrConflict        = &AppError{Code: CodeConflict, Message: "conflict"}
	ErrTooManyRequests = &AppError{Code: CodeTooManyRequests, Message: "too many requests"}
	ErrServiceUnavailable = &AppError{Code: ErrCodeServiceUnavailable, Message: "service unavailable"}

	ErrIdentifierExists            = &AppError{Code: ErrCodeIdentifierExists, Message: "identifier already exists"}
	ErrIdentifierNotFound          = &AppError{Code: ErrCodeIdentifierNotFound, Message: "identifier not found"}
	ErrInvalidCredentials          = &AppError{Code: ErrCodeInvalidCredentials, Message: "invalid credentials"}
	ErrRefreshTokenInvalid         = &AppError{Code: ErrCodeRefreshTokenInvalid, Message: "invalid refresh token"}
	ErrTermsNotAccepted            = &AppError{Code: ErrCodeTermsNotAccepted, Message: "terms not accepted"}
	ErrVerificationNotFound        = &AppError{Code: ErrCodeVerificationNotFound, Message: "verification code not found"}
	ErrVerificationMismatch        = &AppError{Code: ErrCodeVerificationMismatch, Message: "verification code mismatch"}
	ErrVerificationTooManyAttempts = &AppError{Code: ErrCodeVerificationTooManyAttempts, Message: "too many verification attempts"}
)

// HTTPStatusFromCode 根据错误码获取对应的 HTTP 状态码。
//
// 映射规则:
//   - 0（CodeSuccess）→ 200
//   - 自定义错误码 >= 1000 时先除 100 取整
//   - 然后按 401/403/404/409/429/5xx/4xx 区间匹配
//   - 未能匹配时默认返回 500
//
// 参数:
//   - code: ErrorCode，业务错误码
//
// 返回值:
//   - int: HTTP 状态码
func HTTPStatusFromCode(code ErrorCode) int {
	if code >= 1000 {
		code = code / 100
	}
	switch {
	case code == CodeSuccess:
		return 200
	case code == CodeUnauthorized:
		return 401
	case code == CodeForbidden:
		return 403
	case code == CodeNotFound:
		return 404
	case code == CodeConflict:
		return 409
	case code == CodeTooManyRequests:
		return 429
	case code >= 500:
		return 500
	case code >= 400:
		return 400
	default:
		return 500
	}
}