package middleware

import (
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/gin-gonic/gin"
)

type stubTokenClaims struct {
	uid       uint64
	tokenType string
}

func (c stubTokenClaims) UserID() uint64    { return c.uid }
func (c stubTokenClaims) TokenType() string { return c.tokenType }

type stubValidator struct {
	claims TokenClaims
	err    error
}

func (v stubValidator) ValidateToken(tokenStr string) (TokenClaims, error) {
	if v.err != nil {
		return nil, v.err
	}
	return v.claims, nil
}

func setupTest() *gin.Engine {
	gin.SetMode(gin.TestMode)
	return gin.New()
}

func TestAuthMiddleware_MissingHeader(t *testing.T) {
	r := setupTest()
	r.GET("/protected", AuthMiddleware(stubValidator{claims: stubTokenClaims{uid: 1}}))

	w := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/protected", nil)
	r.ServeHTTP(w, req)

	if w.Code != http.StatusUnauthorized {
		t.Fatalf("expected 401, got %d", w.Code)
	}
}

func TestAuthMiddleware_InvalidToken(t *testing.T) {
	r := setupTest()
	r.GET("/protected", AuthMiddleware(stubValidator{err: errors.New("invalid")}))

	w := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/protected", nil)
	req.Header.Set("Authorization", "Bearer invalidtoken")
	r.ServeHTTP(w, req)

	if w.Code != http.StatusUnauthorized {
		t.Fatalf("expected 401, got %d", w.Code)
	}
}

func TestAuthMiddleware_Success(t *testing.T) {
	r := setupTest()
	r.GET("/protected", AuthMiddleware(stubValidator{claims: stubTokenClaims{uid: 42, tokenType: "access"}}), func(c *gin.Context) {
		uid, ok := GetUserID(c)
		if !ok || uid != 42 {
			t.Fatalf("expected user_id=42, got %v, ok=%v", uid, ok)
		}
		c.Status(http.StatusOK)
	})

	w := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/protected", nil)
	req.Header.Set("Authorization", "Bearer validtoken")
	r.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", w.Code)
	}
}

func TestOptionalAuthMiddleware_NoToken(t *testing.T) {
	r := setupTest()
	r.GET("/optional", OptionalAuthMiddleware(stubValidator{claims: stubTokenClaims{uid: 1}}), func(c *gin.Context) {
		_, ok := GetUserID(c)
		if ok {
			t.Fatal("expected no user_id for anonymous request")
		}
		c.Status(http.StatusOK)
	})

	w := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/optional", nil)
	r.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", w.Code)
	}
}

func TestOptionalAuthMiddleware_WithValidToken(t *testing.T) {
	r := setupTest()
	r.GET("/optional", OptionalAuthMiddleware(stubValidator{claims: stubTokenClaims{uid: 99, tokenType: "access"}}), func(c *gin.Context) {
		uid, ok := GetUserID(c)
		if !ok || uid != 99 {
			t.Fatalf("expected user_id=99, got %v, ok=%v", uid, ok)
		}
		c.Status(http.StatusOK)
	})

	w := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/optional", nil)
	req.Header.Set("Authorization", "Bearer validtoken")
	r.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", w.Code)
	}
}

func TestOptionalAuthMiddleware_WithInvalidToken(t *testing.T) {
	r := setupTest()
	r.GET("/optional", OptionalAuthMiddleware(stubValidator{err: errors.New("invalid")}), func(c *gin.Context) {
		_, ok := GetUserID(c)
		if ok {
			t.Fatal("expected no user_id when token is invalid")
		}
		c.Status(http.StatusOK)
	})

	w := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/optional", nil)
	req.Header.Set("Authorization", "Bearer invalidtoken")
	r.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", w.Code)
	}
}

func TestGetUserID_NotSet(t *testing.T) {
	c := &gin.Context{}
	_, ok := GetUserID(c)
	if ok {
		t.Fatal("expected false when user_id not set")
	}
}

func TestGetUserID_TypeConversion(t *testing.T) {
	c := &gin.Context{}
	c.Set(string(ctxUserID), float64(123))
	uid, ok := GetUserID(c)
	if !ok || uid != 123 {
		t.Fatalf("expected 123, got %v", uid)
	}
}