package errcode

import "fmt"

// ErrorCode 定义业务错误码类型，底层为 int。
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

// codeMessages 存储错误码对应的中文消息。
var codeMessages = map[ErrorCode]string{
	CodeSuccess:                "成功",
	CodeBadRequest:             "请求参数错误",
	CodeUnauthorized:           "未授权",
	CodeForbidden:              "无权操作",
	CodeNotFound:               "资源不存在",
	CodeConflict:               "资源冲突",
	CodeTooManyRequests:        "请求过于频繁",
	CodeInternalError:          "服务器内部错误",
	ErrCodeIdentifierExists:    "标识已存在",
	ErrCodeIdentifierNotFound:  "标识不存在",
	ErrCodeInvalidCredentials:  "凭证无效",
	ErrCodeRefreshTokenInvalid: "刷新令牌无效",
	ErrCodeTermsNotAccepted:    "未接受条款",
	ErrCodeVerificationNotFound:        "验证码不存在",
	ErrCodeVerificationMismatch:        "验证码不匹配",
	ErrCodeVerificationTooManyAttempts: "验证尝试次数过多",
	ErrCodeServiceUnavailable:          "服务暂不可用",
}

// AppError 是统一的业务错误类型，包含错误码和消息。
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

// Msg 返回错误码对应的中文消息。
func (e *AppError) Msg() string {
	if msg, ok := codeMessages[e.Code]; ok {
		return msg
	}
	return "未知错误"
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
func HTTPStatusFromCode(code ErrorCode) int {
	if code >= 1000 {
		code = code / 100
	}
	switch {
	case code == CodeSuccess:
		return 200
	case code == CodeBadRequest:
		return 400
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
	case code == ErrCodeServiceUnavailable:
		return 503
	case code >= 500:
		return 500
	default:
		return 500
	}
}