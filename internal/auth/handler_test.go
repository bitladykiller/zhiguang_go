package auth

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"encoding/json"
	"encoding/pem"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/zhiguang/app/pkg/config"
	"github.com/zhiguang/app/pkg/errcode"
)

type mockAuthService struct {
	sendCodeFn         func(ctx context.Context, req *SendCodeRequest) (SendCodeResponse, *errcode.AppError)
	registerFn         func(ctx context.Context, req *RegisterRequest, clientInfo ClientInfo) (AuthResponse, *errcode.AppError)
	loginFn            func(ctx context.Context, req *LoginRequest, clientInfo ClientInfo) (AuthResponse, *errcode.AppError)
	refreshFn          func(ctx context.Context, req *TokenRefreshRequest) (AuthResponse, *errcode.AppError)
	logoutFn           func(ctx context.Context, req *TokenRefreshRequest)
	resetPasswordFn    func(ctx context.Context, req *PasswordResetRequest) *errcode.AppError
	currentUserFn      func(ctx context.Context, userID uint64) (AuthUserResponse, *errcode.AppError)
}

func (m *mockAuthService) SendCode(ctx context.Context, req *SendCodeRequest) (SendCodeResponse, *errcode.AppError) {
	return m.sendCodeFn(ctx, req)
}
func (m *mockAuthService) Register(ctx context.Context, req *RegisterRequest, clientInfo ClientInfo) (AuthResponse, *errcode.AppError) {
	return m.registerFn(ctx, req, clientInfo)
}
func (m *mockAuthService) Login(ctx context.Context, req *LoginRequest, clientInfo ClientInfo) (AuthResponse, *errcode.AppError) {
	return m.loginFn(ctx, req, clientInfo)
}
func (m *mockAuthService) Refresh(ctx context.Context, req *TokenRefreshRequest) (AuthResponse, *errcode.AppError) {
	return m.refreshFn(ctx, req)
}
func (m *mockAuthService) Logout(ctx context.Context, req *TokenRefreshRequest) {
	m.logoutFn(ctx, req)
}
func (m *mockAuthService) ResetPassword(ctx context.Context, req *PasswordResetRequest) *errcode.AppError {
	return m.resetPasswordFn(ctx, req)
}
func (m *mockAuthService) CurrentUser(ctx context.Context, userID uint64) (AuthUserResponse, *errcode.AppError) {
	return m.currentUserFn(ctx, userID)
}

func setupHandlerTest(tb testing.TB) (*gin.Engine, *mockAuthService, *JwtService) {
	tb.Helper()
	gin.SetMode(gin.TestMode)
	mock := &mockAuthService{}
	jwtSvc, err := newTestJwtService()
	if err != nil {
		tb.Fatalf("failed to create test jwt service: %v", err)
	}
	h := NewAuthHandler(mock, jwtSvc)
	r := gin.New()
	h.RegisterRoutes(r.Group("/api"))
	return r, mock, jwtSvc
}

func TestHandler_SendCode(t *testing.T) {
	r, mock, _ := setupHandlerTest(t)

	t.Run("success", func(t *testing.T) {
		mock.sendCodeFn = func(ctx context.Context, req *SendCodeRequest) (SendCodeResponse, *errcode.AppError) {
			return SendCodeResponse{Identifier: req.Identifier, Scene: req.Scene, ExpireSeconds: 300}, nil
		}
		body := `{"identifier":"13800138000","identifier_type":"PHONE","scene":"REGISTER"}`
		w := httptest.NewRecorder()
		req, _ := http.NewRequest("POST", "/api/auth/send-code", strings.NewReader(body))
		req.Header.Set("Content-Type", "application/json")
		r.ServeHTTP(w, req)
		if w.Code != 200 {
			t.Fatalf("expected 200, got %d: %s", w.Code, w.Body.String())
		}
		var resp struct {
			Code int `json:"code"`
			Data struct {
				Identifier    string `json:"identifier"`
				Scene         string `json:"scene"`
				ExpireSeconds int    `json:"expire_seconds"`
			} `json:"data"`
		}
		if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
			t.Fatalf("unmarshal: %v", err)
		}
		if resp.Code != 0 || resp.Data.Identifier != "13800138000" {
			t.Fatalf("unexpected response: %+v", resp)
		}
	})

	t.Run("bind error", func(t *testing.T) {
		w := httptest.NewRecorder()
		req, _ := http.NewRequest("POST", "/api/auth/send-code", strings.NewReader(`{invalid}`))
		req.Header.Set("Content-Type", "application/json")
		r.ServeHTTP(w, req)
		if w.Code != 400 {
			t.Fatalf("expected 400, got %d", w.Code)
		}
	})
}

func TestHandler_Register(t *testing.T) {
	r, mock, _ := setupHandlerTest(t)

	t.Run("success", func(t *testing.T) {
		mock.registerFn = func(ctx context.Context, req *RegisterRequest, ci ClientInfo) (AuthResponse, *errcode.AppError) {
			return AuthResponse{
				User:  AuthUserResponse{ID: 1, Nickname: "test"},
				Token: TokenResponse{AccessToken: "at", RefreshToken: "rt"},
			}, nil
		}
		body := `{"identifier":"13800138000","identifier_type":"PHONE","code":"123456","agree_terms":true}`
		w := httptest.NewRecorder()
		req, _ := http.NewRequest("POST", "/api/auth/register", strings.NewReader(body))
		req.Header.Set("Content-Type", "application/json")
		r.ServeHTTP(w, req)
		if w.Code != 201 {
			t.Fatalf("expected 201, got %d: %s", w.Code, w.Body.String())
		}
	})

	t.Run("bind error", func(t *testing.T) {
		w := httptest.NewRecorder()
		req, _ := http.NewRequest("POST", "/api/auth/register", strings.NewReader(`{invalid}`))
		req.Header.Set("Content-Type", "application/json")
		r.ServeHTTP(w, req)
		if w.Code != 400 {
			t.Fatalf("expected 400, got %d", w.Code)
		}
	})
}

func TestHandler_Login(t *testing.T) {
	r, mock, _ := setupHandlerTest(t)

	t.Run("success", func(t *testing.T) {
		mock.loginFn = func(ctx context.Context, req *LoginRequest, ci ClientInfo) (AuthResponse, *errcode.AppError) {
			return AuthResponse{
				User:  AuthUserResponse{ID: 1, Nickname: "test"},
				Token: TokenResponse{AccessToken: "at", RefreshToken: "rt"},
			}, nil
		}
		body := `{"identifier":"13800138000","identifier_type":"PHONE","password":"abc123"}`
		w := httptest.NewRecorder()
		req, _ := http.NewRequest("POST", "/api/auth/login", strings.NewReader(body))
		req.Header.Set("Content-Type", "application/json")
		r.ServeHTTP(w, req)
		if w.Code != 200 {
			t.Fatalf("expected 200, got %d: %s", w.Code, w.Body.String())
		}
	})

	t.Run("bind error", func(t *testing.T) {
		w := httptest.NewRecorder()
		req, _ := http.NewRequest("POST", "/api/auth/login", strings.NewReader(`{invalid}`))
		req.Header.Set("Content-Type", "application/json")
		r.ServeHTTP(w, req)
		if w.Code != 400 {
			t.Fatalf("expected 400, got %d", w.Code)
		}
	})
}

func TestHandler_Refresh(t *testing.T) {
	r, mock, _ := setupHandlerTest(t)

	t.Run("success", func(t *testing.T) {
		mock.refreshFn = func(ctx context.Context, req *TokenRefreshRequest) (AuthResponse, *errcode.AppError) {
			return AuthResponse{
				User:  AuthUserResponse{ID: 1, Nickname: "test"},
				Token: TokenResponse{AccessToken: "new_at", RefreshToken: "new_rt"},
			}, nil
		}
		body := `{"refresh_token":"some_token"}`
		w := httptest.NewRecorder()
		req, _ := http.NewRequest("POST", "/api/auth/refresh", strings.NewReader(body))
		req.Header.Set("Content-Type", "application/json")
		r.ServeHTTP(w, req)
		if w.Code != 200 {
			t.Fatalf("expected 200, got %d: %s", w.Code, w.Body.String())
		}
	})

	t.Run("bind error", func(t *testing.T) {
		w := httptest.NewRecorder()
		req, _ := http.NewRequest("POST", "/api/auth/refresh", strings.NewReader(`{invalid}`))
		req.Header.Set("Content-Type", "application/json")
		r.ServeHTTP(w, req)
		if w.Code != 400 {
			t.Fatalf("expected 400, got %d", w.Code)
		}
	})
}

func TestHandler_Logout(t *testing.T) {
	r, mock, _ := setupHandlerTest(t)

	t.Run("success", func(t *testing.T) {
		mock.logoutFn = func(ctx context.Context, req *TokenRefreshRequest) {}
		body := `{"refresh_token":"some_token"}`
		w := httptest.NewRecorder()
		req, _ := http.NewRequest("POST", "/api/auth/logout", strings.NewReader(body))
		req.Header.Set("Content-Type", "application/json")
		r.ServeHTTP(w, req)
		if w.Code != 200 {
			t.Fatalf("expected 200, got %d: %s", w.Code, w.Body.String())
		}
	})

	t.Run("bind error", func(t *testing.T) {
		w := httptest.NewRecorder()
		req, _ := http.NewRequest("POST", "/api/auth/logout", strings.NewReader(`{invalid}`))
		req.Header.Set("Content-Type", "application/json")
		r.ServeHTTP(w, req)
		if w.Code != 400 {
			t.Fatalf("expected 400, got %d", w.Code)
		}
	})
}

func TestHandler_ResetPassword(t *testing.T) {
	r, mock, _ := setupHandlerTest(t)

	t.Run("success", func(t *testing.T) {
		mock.resetPasswordFn = func(ctx context.Context, req *PasswordResetRequest) *errcode.AppError {
			return nil
		}
		body := `{"identifier":"13800138000","identifier_type":"PHONE","code":"123456","new_password":"abc123"}`
		w := httptest.NewRecorder()
		req, _ := http.NewRequest("POST", "/api/auth/reset-password", strings.NewReader(body))
		req.Header.Set("Content-Type", "application/json")
		r.ServeHTTP(w, req)
		if w.Code != 200 {
			t.Fatalf("expected 200, got %d: %s", w.Code, w.Body.String())
		}
	})

	t.Run("bind error", func(t *testing.T) {
		w := httptest.NewRecorder()
		req, _ := http.NewRequest("POST", "/api/auth/reset-password", strings.NewReader(`{invalid}`))
		req.Header.Set("Content-Type", "application/json")
		r.ServeHTTP(w, req)
		if w.Code != 400 {
			t.Fatalf("expected 400, got %d", w.Code)
		}
	})
}

func TestHandler_Me(t *testing.T) {
	r, mock, jwtSvc := setupHandlerTest(t)

	t.Run("success", func(t *testing.T) {
		user := &User{ID: 42, Nickname: "testuser"}
		tokenPair, err := jwtSvc.IssueTokenPair(user)
		if err != nil {
			t.Fatalf("issue token: %v", err)
		}
		mock.currentUserFn = func(ctx context.Context, userID uint64) (AuthUserResponse, *errcode.AppError) {
			if userID != 42 {
				t.Fatalf("expected userID 42, got %d", userID)
			}
			return AuthUserResponse{ID: 42, Nickname: "testuser"}, nil
		}
		w := httptest.NewRecorder()
		req, _ := http.NewRequest("GET", "/api/auth/me", nil)
		req.Header.Set("Authorization", "Bearer "+tokenPair.AccessToken)
		r.ServeHTTP(w, req)
		if w.Code != 200 {
			t.Fatalf("expected 200, got %d: %s", w.Code, w.Body.String())
		}
		var resp struct {
			Code int `json:"code"`
			Data struct {
				ID       uint64 `json:"id"`
				Nickname string `json:"nickname"`
			} `json:"data"`
		}
		if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
			t.Fatalf("unmarshal: %v", err)
		}
		if resp.Data.ID != 42 || resp.Data.Nickname != "testuser" {
			t.Fatalf("unexpected user: %+v", resp.Data)
		}
	})

	t.Run("get user id failed - no token", func(t *testing.T) {
		w := httptest.NewRecorder()
		req, _ := http.NewRequest("GET", "/api/auth/me", nil)
		r.ServeHTTP(w, req)
		if w.Code != 401 {
			t.Fatalf("expected 401, got %d: %s", w.Code, w.Body.String())
		}
	})

	t.Run("get user id failed - service error", func(t *testing.T) {
		user := &User{ID: 99, Nickname: "fail"}
		tokenPair, err := jwtSvc.IssueTokenPair(user)
		if err != nil {
			t.Fatalf("issue token: %v", err)
		}
		mock.currentUserFn = func(ctx context.Context, userID uint64) (AuthUserResponse, *errcode.AppError) {
			return AuthUserResponse{}, errcode.ErrIdentifierNotFound
		}
		w := httptest.NewRecorder()
		req, _ := http.NewRequest("GET", "/api/auth/me", nil)
		req.Header.Set("Authorization", "Bearer "+tokenPair.AccessToken)
		r.ServeHTTP(w, req)
		if w.Code != 404 {
			t.Fatalf("expected 404, got %d: %s", w.Code, w.Body.String())
		}
	})
}

func TestExtractClientInfo(t *testing.T) {
	gin.SetMode(gin.TestMode)

	t.Run("with IP and User-Agent", func(t *testing.T) {
		w := httptest.NewRecorder()
		c, _ := gin.CreateTestContext(w)
		c.Request, _ = http.NewRequest("GET", "/", nil)
		c.Request.Header.Set("User-Agent", "test-agent")
		c.Request.RemoteAddr = "192.168.1.1:12345"
		ci := extractClientInfo(c)
		if ci.UserAgent != "test-agent" {
			t.Fatalf("expected UserAgent 'test-agent', got '%s'", ci.UserAgent)
		}
	})

	t.Run("without User-Agent", func(t *testing.T) {
		w := httptest.NewRecorder()
		c, _ := gin.CreateTestContext(w)
		c.Request, _ = http.NewRequest("GET", "/", nil)
		ci := extractClientInfo(c)
		if ci.UserAgent != "" {
			t.Fatalf("expected empty UserAgent, got '%s'", ci.UserAgent)
		}
	})
}

func newTestJwtService() (*JwtService, error) {
	_, privPath, pubPath := createTempKeyPair()
	return NewJwtService(&config.JwtConfig{
		Issuer:          "test",
		KeyID:           "test-key",
		PrivateKeyPath:  privPath,
		PublicKeyPath:   pubPath,
		AccessTokenTTL:  time.Hour,
		RefreshTokenTTL: 24 * time.Hour,
	})
}

func createTempKeyPair() (string, string, string) {
	dir, _ := os.MkdirTemp("", "jwt-test-*")
	privKey, _ := rsa.GenerateKey(rand.Reader, 2048)
	privBlock := &pem.Block{Type: "RSA PRIVATE KEY", Bytes: x509.MarshalPKCS1PrivateKey(privKey)}
	privPath := filepath.Join(dir, "private.pem")
	f, _ := os.Create(privPath)
	pem.Encode(f, privBlock)
	f.Close()

	pubBytes, _ := x509.MarshalPKIXPublicKey(&privKey.PublicKey)
	pubBlock := &pem.Block{Type: "PUBLIC KEY", Bytes: pubBytes}
	pubPath := filepath.Join(dir, "public.pem")
	f2, _ := os.Create(pubPath)
	pem.Encode(f2, pubBlock)
	f2.Close()
	return dir, privPath, pubPath
}

// ============================================================================
// Benchmarks
// ============================================================================

func BenchmarkRegisterHandler(b *testing.B) {
	r, mock, _ := setupHandlerTest(b)
	mock.registerFn = func(ctx context.Context, req *RegisterRequest, ci ClientInfo) (AuthResponse, *errcode.AppError) {
		return AuthResponse{
			User:  AuthUserResponse{ID: 1, Nickname: "test"},
			Token: TokenResponse{AccessToken: "at", RefreshToken: "rt"},
		}, nil
	}

	body := `{"identifier":"13800138000","identifier_type":"PHONE","code":"123456","agree_terms":true}`
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		w := httptest.NewRecorder()
		req, _ := http.NewRequest("POST", "/api/auth/register", strings.NewReader(body))
		req.Header.Set("Content-Type", "application/json")
		r.ServeHTTP(w, req)
	}
}

func BenchmarkLoginHandler(b *testing.B) {
	r, mock, _ := setupHandlerTest(b)
	mock.loginFn = func(ctx context.Context, req *LoginRequest, ci ClientInfo) (AuthResponse, *errcode.AppError) {
		return AuthResponse{
			User:  AuthUserResponse{ID: 1, Nickname: "test"},
			Token: TokenResponse{AccessToken: "at", RefreshToken: "rt"},
		}, nil
	}

	body := `{"identifier":"13800138000","identifier_type":"PHONE","password":"abc123"}`
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		w := httptest.NewRecorder()
		req, _ := http.NewRequest("POST", "/api/auth/login", strings.NewReader(body))
		req.Header.Set("Content-Type", "application/json")
		r.ServeHTTP(w, req)
	}
}