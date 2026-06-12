package middleware

import (
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/gin-gonic/gin"
)

type stubClaims struct {
	userID    uint64
	tokenType string
}

func (s stubClaims) UserID() uint64    { return s.userID }
func (s stubClaims) TokenType() string { return s.tokenType }

type stubValidator struct {
	claims TokenClaims
	err    error
}

func (s stubValidator) ValidateToken(tokenStr string) (TokenClaims, error) {
	if s.err != nil {
		return nil, s.err
	}
	return s.claims, nil
}

func TestExtractBearerToken(t *testing.T) {
	gin.SetMode(gin.TestMode)

	tests := []struct {
		name   string
		header string
		want   string
	}{
		{name: "missing", header: "", want: ""},
		{name: "valid", header: "Bearer abc", want: "abc"},
		{name: "case insensitive", header: "bearer xyz", want: "xyz"},
		{name: "trim token", header: "Bearer   token-with-space  ", want: "token-with-space"},
		{name: "wrong scheme", header: "Basic abc", want: ""},
		{name: "missing token", header: "Bearer", want: ""},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			recorder := httptest.NewRecorder()
			ctx, _ := gin.CreateTestContext(recorder)
			req := httptest.NewRequest(http.MethodGet, "/", nil)
			if tc.header != "" {
				req.Header.Set("Authorization", tc.header)
			}
			ctx.Request = req

			if got := extractBearerToken(ctx); got != tc.want {
				t.Fatalf("extractBearerToken() = %q, want %q", got, tc.want)
			}
		})
	}
}

func TestGetUserID(t *testing.T) {
	gin.SetMode(gin.TestMode)

	tests := []struct {
		name  string
		value interface{}
		want  uint64
		ok    bool
	}{
		{name: "missing", value: nil, want: 0, ok: false},
		{name: "uint64", value: uint64(7), want: 7, ok: true},
		{name: "float64", value: float64(8), want: 8, ok: true},
		{name: "int64", value: int64(9), want: 9, ok: true},
		{name: "int", value: int(10), want: 10, ok: true},
		{name: "unsupported", value: "11", want: 0, ok: false},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			recorder := httptest.NewRecorder()
			ctx, _ := gin.CreateTestContext(recorder)
			if tc.value != nil {
				ctx.Set(string(ctxUserID), tc.value)
			}

			got, ok := GetUserID(ctx)
			if got != tc.want || ok != tc.ok {
				t.Fatalf("GetUserID() = (%d, %v), want (%d, %v)", got, ok, tc.want, tc.ok)
			}
		})
	}
}

func TestAuthMiddleware(t *testing.T) {
	gin.SetMode(gin.TestMode)

	t.Run("missing header", func(t *testing.T) {
		router := gin.New()
		router.GET("/", AuthMiddleware(stubValidator{}), func(c *gin.Context) {
			c.Status(http.StatusOK)
		})

		req := httptest.NewRequest(http.MethodGet, "/", nil)
		resp := httptest.NewRecorder()
		router.ServeHTTP(resp, req)

		if resp.Code != http.StatusUnauthorized {
			t.Fatalf("status = %d, want %d", resp.Code, http.StatusUnauthorized)
		}
	})

	t.Run("invalid token", func(t *testing.T) {
		router := gin.New()
		router.GET("/", AuthMiddleware(stubValidator{err: errors.New("bad token")}), func(c *gin.Context) {
			c.Status(http.StatusOK)
		})

		req := httptest.NewRequest(http.MethodGet, "/", nil)
		req.Header.Set("Authorization", "Bearer bad")
		resp := httptest.NewRecorder()
		router.ServeHTTP(resp, req)

		if resp.Code != http.StatusUnauthorized {
			t.Fatalf("status = %d, want %d", resp.Code, http.StatusUnauthorized)
		}
	})

	t.Run("valid token", func(t *testing.T) {
		router := gin.New()
		router.GET("/", AuthMiddleware(stubValidator{claims: stubClaims{userID: 42, tokenType: "access"}}), func(c *gin.Context) {
			userID, ok := GetUserID(c)
			if !ok || userID != 42 {
				t.Fatalf("GetUserID() = (%d, %v), want (42, true)", userID, ok)
			}
			c.Status(http.StatusNoContent)
		})

		req := httptest.NewRequest(http.MethodGet, "/", nil)
		req.Header.Set("Authorization", "Bearer good")
		resp := httptest.NewRecorder()
		router.ServeHTTP(resp, req)

		if resp.Code != http.StatusNoContent {
			t.Fatalf("status = %d, want %d", resp.Code, http.StatusNoContent)
		}
	})
}

func TestOptionalAuthMiddleware(t *testing.T) {
	gin.SetMode(gin.TestMode)

	router := gin.New()
	router.GET("/", OptionalAuthMiddleware(stubValidator{claims: stubClaims{userID: 88, tokenType: "access"}}), func(c *gin.Context) {
		userID, ok := GetUserID(c)
		if userID == 88 && ok {
			c.Status(http.StatusCreated)
			return
		}
		c.Status(http.StatusOK)
	})

	t.Run("anonymous passes through", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/", nil)
		resp := httptest.NewRecorder()
		router.ServeHTTP(resp, req)

		if resp.Code != http.StatusOK {
			t.Fatalf("status = %d, want %d", resp.Code, http.StatusOK)
		}
	})

	t.Run("valid token enriches context", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/", nil)
		req.Header.Set("Authorization", "Bearer good")
		resp := httptest.NewRecorder()
		router.ServeHTTP(resp, req)

		if resp.Code != http.StatusCreated {
			t.Fatalf("status = %d, want %d", resp.Code, http.StatusCreated)
		}
	})
}
