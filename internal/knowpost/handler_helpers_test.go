package knowpost

import (
	"errors"
	"net/http/httptest"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/zhiguang/app/pkg/errcode"
)

func TestQueryInt(t *testing.T) {
	gin.SetMode(gin.TestMode)

	recorder := httptest.NewRecorder()
	ctx, _ := gin.CreateTestContext(recorder)
	ctx.Request = httptest.NewRequest("GET", "/?page=3&size=bad", nil)

	if got := queryInt(ctx, "page", 1); got != 3 {
		t.Fatalf("queryInt(page) = %d, want 3", got)
	}
	if got := queryInt(ctx, "size", 20); got != 20 {
		t.Fatalf("queryInt(size) = %d, want 20", got)
	}
	if got := queryInt(ctx, "missing", 99); got != 99 {
		t.Fatalf("queryInt(missing) = %d, want 99", got)
	}
}

func TestToAppErr(t *testing.T) {
	appErr := errcode.ErrForbidden.WithMsg("forbidden")
	if got := toAppErr(appErr); got != appErr {
		t.Fatal("toAppErr should return original AppError instance")
	}

	got := toAppErr(errors.New("db down"))
	if got.Code != errcode.CodeInternalError {
		t.Fatalf("toAppErr(non-app error).Code = %d, want %d", got.Code, errcode.CodeInternalError)
	}
	if got.Message != "db down" {
		t.Fatalf("toAppErr(non-app error).Message = %q, want %q", got.Message, "db down")
	}
}

func TestOptionalUserID(t *testing.T) {
	gin.SetMode(gin.TestMode)

	recorder := httptest.NewRecorder()
	ctx, _ := gin.CreateTestContext(recorder)
	if got := optionalUserID(ctx); got != nil {
		t.Fatal("optionalUserID() should return nil when context has no user")
	}

	ctx.Set("user_id", uint64(42))
	got := optionalUserID(ctx)
	if got == nil || *got != 42 {
		t.Fatalf("optionalUserID() = %v, want pointer to 42", got)
	}
}
