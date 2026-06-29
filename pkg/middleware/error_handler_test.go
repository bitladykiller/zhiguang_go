package middleware

import (
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/zhiguang/app/pkg/errcode"
	"go.uber.org/zap"
)

func TestErrorLogMiddleware_NoErrors(t *testing.T) {
	logger, _ := zap.NewDevelopment()
	r := setupTest()
	r.Use(ErrorLogMiddleware(logger))
	r.GET("/ok", func(c *gin.Context) {
		c.Status(http.StatusOK)
	})

	w := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/ok", nil)
	r.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", w.Code)
	}
}

func TestRecordError(t *testing.T) {
	r := setupTest()
	r.Use(ErrorLogMiddleware(zap.NewNop()))
	r.GET("/err", func(c *gin.Context) {
		RecordError(c, errors.New("test error"))
		c.Status(http.StatusInternalServerError)
	})

	w := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/err", nil)
	r.ServeHTTP(w, req)

	if w.Code != http.StatusInternalServerError {
		t.Fatalf("status = %d, want 500", w.Code)
	}
}

func TestRecordError_Nil(t *testing.T) {
	RecordError(nil, nil)
}

func TestToAppErr_AppError(t *testing.T) {
	appErr := errcode.ErrNotFound.WithMsg("custom")
	result := ToAppErr(appErr)
	if result != appErr {
		t.Error("should return same pointer")
	}
}

func TestToAppErr_PlainError(t *testing.T) {
	result := ToAppErr(errors.New("db error"))
	if result.Code != errcode.CodeInternalError {
		t.Errorf("code = %d, want %d", result.Code, errcode.CodeInternalError)
	}
	if result.Message != "internal error" {
		t.Errorf("message = %q, want %q", result.Message, "internal error")
	}
}

func TestToAppErr_Nil(t *testing.T) {
	result := ToAppErr(nil)
	if result.Code != errcode.CodeInternalError {
		t.Errorf("code = %d, want %d", result.Code, errcode.CodeInternalError)
	}
}