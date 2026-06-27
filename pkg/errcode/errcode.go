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

// ============================================================================
// 任务 D / F：结构化错误响应 + 错误嵌套
// ============================================================================

type Code string

const (
	CodeSuccessStr        Code = "SUCCESS"
	CodeInternalStr       Code = "INTERNAL_ERROR"
	CodeBadRequestStr     Code = "BAD_REQUEST"
	CodeNotFoundStr       Code = "NOT_FOUND"
	CodeUnauthorizedStr   Code = "UNAUTHORIZED"
	CodeKnowPostNotFound  Code = "KNOWPOST_NOT_FOUND"
	CodeKnowPostForbidden Code = "KNOWPOST_FORBIDDEN"
	CodeCounterInvalid    Code = "COUNTER_INVALID_PARAM"
)

var codeMessages = map[Code]string{
	CodeSuccessStr:        "成功",
	CodeInternalStr:       "服务器内部错误",
	CodeBadRequestStr:     "请求参数错误",
	CodeNotFoundStr:       "资源不存在",
	CodeUnauthorizedStr:   "未授权",
	CodeKnowPostNotFound:  "知文不存在或已删除",
	CodeKnowPostForbidden: "无权操作该知文",
	CodeCounterInvalid:    "计数器参数无效",
}

func CodeMsg(c Code) string {
	if msg, ok := codeMessages[c]; ok {
		return msg
	}
	return "未知错误"
}

type StrAppError struct {
	Code    Code   `json:"code"`
	Message string `json:"message"`
	Detail  string `json:"detail,omitempty"`
	Err     error  `json:"-"`
}

func (e *StrAppError) Error() string {
	return fmt.Sprintf("[%s] %s", e.Code, e.Message)
}

func (e *StrAppError) Unwrap() error { return e.Err }

func NewStrAppError(code Code, err error) *StrAppError {
	return &StrAppError{Code: code, Message: CodeMsg(code), Err: err}
}

func NewStrAppErrorWithDetail(code Code, detail string) *StrAppError {
	return &StrAppError{Code: code, Message: CodeMsg(code), Detail: detail}
}

func Wrap(err error, code Code) *StrAppError {
	return &StrAppError{Code: code, Message: CodeMsg(code), Err: err}
}

func (e *StrAppError) WithDetail(detail string) *StrAppError {
	e.Detail = detail
	return e
}

func HTTPStatusFromStrCode(code Code) int {
	switch code {
	case CodeBadRequestStr, CodeCounterInvalid:
		return 400
	case CodeUnauthorizedStr:
		return 401
	case CodeKnowPostForbidden:
		return 403
	case CodeNotFoundStr, CodeKnowPostNotFound:
		return 404
	default:
		return 500
	}
}