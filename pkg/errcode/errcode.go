package errcode

import "fmt"

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