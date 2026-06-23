package response

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/zhiguang/app/pkg/errcode"
)

func setupTest() *gin.Engine {
	gin.SetMode(gin.TestMode)
	return gin.New()
}

func TestSuccess(t *testing.T) {
	r := setupTest()
	r.GET("/test", func(c *gin.Context) {
		Success(c, map[string]string{"key": "value"})
	})

	w := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/test", nil)
	r.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", w.Code)
	}
	body := w.Body.String()
	if body != `{"code":0,"message":"success","data":{"key":"value"}}` {
		t.Errorf("body = %q", body)
	}
}

func TestSuccess_NilData(t *testing.T) {
	r := setupTest()
	r.GET("/nil", func(c *gin.Context) {
		Success[any](c, nil)
	})

	w := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/nil", nil)
	r.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", w.Code)
	}
}

func TestCreated(t *testing.T) {
	r := setupTest()
	r.POST("/create", func(c *gin.Context) {
		Created(c, gin.H{"id": "abc123"})
	})

	w := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/create", nil)
	r.ServeHTTP(w, req)

	if w.Code != http.StatusCreated {
		t.Fatalf("status = %d, want 201", w.Code)
	}
	if w.Body.String() != `{"code":0,"message":"created","data":{"id":"abc123"}}` {
		t.Errorf("body = %q", w.Body.String())
	}
}

func TestCreated_NilData(t *testing.T) {
	r := setupTest()
	r.POST("/create-nil", func(c *gin.Context) {
		Created[any](c, nil)
	})

	w := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/create-nil", nil)
	r.ServeHTTP(w, req)

	if w.Code != http.StatusCreated {
		t.Fatalf("status = %d, want 201", w.Code)
	}
}

func TestNoContent(t *testing.T) {
	r := setupTest()
	r.DELETE("/delete", func(c *gin.Context) {
		NoContent(c)
	})

	w := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodDelete, "/delete", nil)
	r.ServeHTTP(w, req)

	if w.Code != http.StatusNoContent {
		t.Fatalf("status = %d, want 204", w.Code)
	}
	if w.Body.Len() != 0 {
		t.Fatal("expected empty body for 204")
	}
}

func TestError(t *testing.T) {
	r := setupTest()
	r.GET("/error", func(c *gin.Context) {
		Error(c, errcode.ErrNotFound)
	})

	w := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/error", nil)
	r.ServeHTTP(w, req)

	if w.Code != http.StatusNotFound {
		t.Fatalf("status = %d, want 404", w.Code)
	}
	if w.Body.String() != `{"code":404,"message":"not found"}` {
		t.Errorf("body = %q", w.Body.String())
	}
}

func TestError_WithCustomMsg(t *testing.T) {
	r := setupTest()
	r.GET("/custom", func(c *gin.Context) {
		Error(c, errcode.ErrNotFound.WithMsg("custom not found"))
	})

	w := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/custom", nil)
	r.ServeHTTP(w, req)

	if w.Code != http.StatusNotFound {
		t.Fatalf("status = %d, want 404", w.Code)
	}
	if w.Body.String() != `{"code":404,"message":"custom not found"}` {
		t.Errorf("body = %q", w.Body.String())
	}
}

func TestError_InternalError(t *testing.T) {
	r := setupTest()
	r.GET("/internal", func(c *gin.Context) {
		Error(c, errcode.ErrInternal)
	})

	w := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/internal", nil)
	r.ServeHTTP(w, req)

	if w.Code != http.StatusInternalServerError {
		t.Fatalf("status = %d, want 500", w.Code)
	}
}

func TestFail(t *testing.T) {
	r := setupTest()
	r.GET("/fail", func(c *gin.Context) {
		Fail(c, http.StatusBadRequest, "bad input")
	})

	w := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/fail", nil)
	r.ServeHTTP(w, req)

	if w.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400", w.Code)
	}
	if w.Body.String() != `{"code":400,"message":"bad input"}` {
		t.Errorf("body = %q", w.Body.String())
	}
}

func TestFail_503(t *testing.T) {
	r := setupTest()
	r.GET("/unavailable", func(c *gin.Context) {
		Fail(c, http.StatusServiceUnavailable, "service not ready")
	})

	w := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/unavailable", nil)
	r.ServeHTTP(w, req)

	if w.Code != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want 503", w.Code)
	}
}