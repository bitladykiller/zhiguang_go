package httputil

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/gin-gonic/gin"
)

func setupTest() *gin.Engine {
	gin.SetMode(gin.TestMode)
	return gin.New()
}

func TestQueryInt_Default(t *testing.T) {
	r := setupTest()
	r.GET("/test", func(c *gin.Context) {
		v := QueryInt(c, "page", 1)
		c.String(http.StatusOK, "%d", v)
	})

	w := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/test", nil)
	r.ServeHTTP(w, req)

	if w.Body.String() != "1" {
		t.Errorf("body = %q, want 1", w.Body.String())
	}
}

func TestQueryInt_Valid(t *testing.T) {
	r := setupTest()
	r.GET("/test", func(c *gin.Context) {
		v := QueryInt(c, "page", 1)
		c.String(http.StatusOK, "%d", v)
	})

	w := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/test?page=5", nil)
	r.ServeHTTP(w, req)

	if w.Body.String() != "5" {
		t.Errorf("body = %q, want 5", w.Body.String())
	}
}

func TestQueryInt_InvalidString(t *testing.T) {
	r := setupTest()
	r.GET("/test", func(c *gin.Context) {
		v := QueryInt(c, "page", 10)
		c.String(http.StatusOK, "%d", v)
	})

	w := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/test?page=abc", nil)
	r.ServeHTTP(w, req)

	if w.Body.String() != "10" {
		t.Errorf("body = %q, want 10", w.Body.String())
	}
}

func TestQueryInt_ZeroValue(t *testing.T) {
	r := setupTest()
	r.GET("/test", func(c *gin.Context) {
		v := QueryInt(c, "page", 42)
		c.String(http.StatusOK, "%d", v)
	})

	w := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/test?page=0", nil)
	r.ServeHTTP(w, req)

	if w.Body.String() != "42" {
		t.Errorf("body = %q, want 42", w.Body.String())
	}
}

func TestQueryInt_NegativeValue(t *testing.T) {
	r := setupTest()
	r.GET("/test", func(c *gin.Context) {
		v := QueryInt(c, "page", 1)
		c.String(http.StatusOK, "%d", v)
	})

	w := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/test?page=-3", nil)
	r.ServeHTTP(w, req)

	if w.Body.String() != "1" {
		t.Errorf("body = %q, want 1", w.Body.String())
	}
}

func TestQueryInt_EmptyString(t *testing.T) {
	r := setupTest()
	r.GET("/test", func(c *gin.Context) {
		v := QueryInt(c, "page", 99)
		c.String(http.StatusOK, "%d", v)
	})

	w := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/test?page=", nil)
	r.ServeHTTP(w, req)

	if w.Body.String() != "99" {
		t.Errorf("body = %q, want 99", w.Body.String())
	}
}

func TestQueryInt_MultipleParams(t *testing.T) {
	r := setupTest()
	r.GET("/test", func(c *gin.Context) {
		page := QueryInt(c, "page", 1)
		size := QueryInt(c, "size", 20)
		c.String(http.StatusOK, "%d:%d", page, size)
	})

	w := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/test?page=3&size=50", nil)
	r.ServeHTTP(w, req)

	if w.Body.String() != "3:50" {
		t.Errorf("body = %q, want 3:50", w.Body.String())
	}
}