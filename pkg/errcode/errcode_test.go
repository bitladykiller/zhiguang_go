package errcode

import (
	"net/http"
	"testing"
)

func TestAppError_Error(t *testing.T) {
	err := &AppError{Code: CodeNotFound, Message: "resource not found"}
	want := "[404] resource not found"
	if got := err.Error(); got != want {
		t.Errorf("Error() = %q, want %q", got, want)
	}
}

func TestAppError_WithMsg(t *testing.T) {
	err := ErrNotFound.WithMsg("custom message")
	if err.Code != CodeNotFound {
		t.Errorf("Code = %d, want %d", err.Code, CodeNotFound)
	}
	if err.Message != "custom message" {
		t.Errorf("Message = %q, want %q", err.Message, "custom message")
	}
}

func TestWithMsg_OriginalUnchanged(t *testing.T) {
	msg := ErrNotFound.Message
	_ = ErrNotFound.WithMsg("changed")
	if ErrNotFound.Message != msg {
		t.Error("WithMsg should not mutate original")
	}
}

func TestHTTPStatusFromCode(t *testing.T) {
	tests := []struct {
		code ErrorCode
		want int
	}{
		{CodeSuccess, http.StatusOK},
		{CodeBadRequest, http.StatusBadRequest},
		{CodeUnauthorized, http.StatusUnauthorized},
		{CodeForbidden, http.StatusForbidden},
		{CodeNotFound, http.StatusNotFound},
		{CodeConflict, http.StatusConflict},
		{CodeTooManyRequests, http.StatusTooManyRequests},
		{CodeInternalError, http.StatusInternalServerError},
		{ErrCodeIdentifierExists, http.StatusConflict},
		{ErrCodeIdentifierNotFound, http.StatusNotFound},
		{ErrCodeInvalidCredentials, http.StatusUnauthorized},
		{ErrCodeRefreshTokenInvalid, http.StatusUnauthorized},
		{ErrCodeTermsNotAccepted, http.StatusBadRequest},
		{ErrCodeVerificationNotFound, http.StatusNotFound},
		{ErrCodeVerificationMismatch, http.StatusBadRequest},
		{ErrCodeVerificationTooManyAttempts, http.StatusTooManyRequests},
	}
	for _, tt := range tests {
		got := HTTPStatusFromCode(tt.code)
		if got != tt.want {
			t.Errorf("HTTPStatusFromCode(%d) = %d, want %d", tt.code, got, tt.want)
		}
	}
}

func TestHTTPStatusFromCode_ServiceUnavailable(t *testing.T) {
	got := HTTPStatusFromCode(ErrCodeServiceUnavailable)
	if got != http.StatusInternalServerError {
		t.Errorf("HTTPStatusFromCode(%d) = %d, want %d", ErrCodeServiceUnavailable, got, http.StatusInternalServerError)
	}
}

func TestHTTPStatusFromCode_Unknown(t *testing.T) {
	got := HTTPStatusFromCode(ErrorCode(999))
	if got != http.StatusInternalServerError {
		t.Errorf("got %d, want 500", got)
	}
}

func TestPredefinedErrors_NonNil(t *testing.T) {
	errs := []*AppError{
		ErrBadRequest, ErrUnauthorized, ErrForbidden, ErrNotFound,
		ErrInternal, ErrConflict, ErrTooManyRequests, ErrServiceUnavailable,
		ErrIdentifierExists, ErrIdentifierNotFound, ErrInvalidCredentials,
		ErrRefreshTokenInvalid, ErrTermsNotAccepted, ErrVerificationNotFound,
		ErrVerificationMismatch, ErrVerificationTooManyAttempts,
	}
	for _, e := range errs {
		if e == nil {
			t.Fatal("predefined error should not be nil")
		}
		if e.Code == 0 && e.Message == "" {
			t.Fatal("predefined error should have code and message")
		}
	}
}