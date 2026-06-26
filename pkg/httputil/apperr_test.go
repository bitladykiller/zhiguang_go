package httputil

import (
	"errors"
	"testing"

	"github.com/zhiguang/app/pkg/errcode"
)

func TestToAppError_AppError(t *testing.T) {
	orig := errcode.ErrNotFound.WithMsg("gone")
	got := ToAppError(orig)
	if got.Code != orig.Code || got.Message != orig.Message {
		t.Fatalf("got %+v want %+v", got, orig)
	}
}

func TestToAppError_WrapsGeneric(t *testing.T) {
	got := ToAppError(errors.New("redis down"))
	if got.Code != errcode.CodeInternalError {
		t.Fatalf("code = %d", got.Code)
	}
	if got.Message != "redis down" {
		t.Fatalf("message = %q", got.Message)
	}
}