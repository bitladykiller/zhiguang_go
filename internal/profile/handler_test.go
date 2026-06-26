package profile

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/zhiguang/app/pkg/errcode"
)

// --- mock service ---

type mockProfileSvc struct {
	getUser   *UserProfile
	getErr    *errcode.AppError
	updateOK  bool
	updateErr *errcode.AppError
}

func (m *mockProfileSvc) GetProfile(ctx context.Context, id uint64) (*UserProfile, *errcode.AppError) {
	if m.getErr != nil {
		return nil, m.getErr
	}
	return m.getUser, nil
}

func (m *mockProfileSvc) UpdateProfile(ctx context.Context, callerID, targetID uint64, req *ProfilePatchRequest) *errcode.AppError {
	if m.updateErr != nil {
		return m.updateErr
	}
	return nil
}

// --- helpers ---

func setupRouter(svc ProfileServicer) *gin.Engine {
	gin.SetMode(gin.TestMode)
	r := gin.New()
	h := NewProfileHandler(svc)
	h.RegisterRoutes(r.Group("/api/v1"))
	return r
}

func setUserID(c *gin.Context, uid uint64) {
	c.Set("user_id", uid)
}

// --- GetProfile ---

func TestGetProfile_Success(t *testing.T) {
	svc := &mockProfileSvc{
		getUser: &UserProfile{ID: 1, Nickname: "alice"},
	}
	r := setupRouter(svc)

	w := httptest.NewRecorder()
	req, _ := http.NewRequest("GET", "/api/v1/profiles/1", nil)
	r.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", w.Code)
	}

	var resp struct {
		Code int             `json:"code"`
		Data json.RawMessage `json:"data"`
	}
	json.Unmarshal(w.Body.Bytes(), &resp)
	if resp.Code != 0 {
		t.Errorf("code = %d, want 0", resp.Code)
	}
}

func TestGetProfile_InvalidID(t *testing.T) {
	svc := &mockProfileSvc{}
	r := setupRouter(svc)

	w := httptest.NewRecorder()
	req, _ := http.NewRequest("GET", "/api/v1/profiles/abc", nil)
	r.ServeHTTP(w, req)

	if w.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400", w.Code)
	}
}

func TestGetProfile_NotFound(t *testing.T) {
	svc := &mockProfileSvc{
		getErr: errcode.ErrNotFound,
	}
	r := setupRouter(svc)

	w := httptest.NewRecorder()
	req, _ := http.NewRequest("GET", "/api/v1/profiles/999", nil)
	r.ServeHTTP(w, req)

	if w.Code != http.StatusNotFound {
		t.Fatalf("status = %d, want 404", w.Code)
	}
}

func TestGetProfile_NegativeID(t *testing.T) {
	svc := &mockProfileSvc{}
	r := setupRouter(svc)

	w := httptest.NewRecorder()
	req, _ := http.NewRequest("GET", "/api/v1/profiles/-1", nil)
	r.ServeHTTP(w, req)

	if w.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400", w.Code)
	}
}

func TestGetProfile_ZeroID(t *testing.T) {
	svc := &mockProfileSvc{
		getUser: &UserProfile{ID: 0},
	}
	r := setupRouter(svc)

	w := httptest.NewRecorder()
	req, _ := http.NewRequest("GET", "/api/v1/profiles/0", nil)
	r.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", w.Code)
	}
}

// --- UpdateProfile ---

func TestUpdateProfile_Success(t *testing.T) {
	svc := &mockProfileSvc{}
	r := setupRouter(svc)

	w := httptest.NewRecorder()
	body := `{"nickname":"new-name"}`
	req, _ := http.NewRequest("PATCH", "/api/v1/profiles/1", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	r.ServeHTTP(w, req)

	// handler uses middleware.GetUserID which reads from Gin context.
	// Without middleware, userID is 0 so it returns 401.
	if w.Code != http.StatusUnauthorized {
		t.Fatalf("status = %d, want 401 (no auth)", w.Code)
	}
}

func TestUpdateProfile_WithAuth(t *testing.T) {
	svc := &mockProfileSvc{}
	gin.SetMode(gin.TestMode)
	r := gin.New()
	h := NewProfileHandler(svc)

	r.PATCH("/api/v1/profiles/:id", func(c *gin.Context) {
		setUserID(c, 1)
		h.UpdateProfile(c)
	})

	w := httptest.NewRecorder()
	body := `{"nickname":"new-name"}`
	req, _ := http.NewRequest("PATCH", "/api/v1/profiles/1", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	r.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200, body: %s", w.Code, w.Body.String())
	}

	var resp struct {
		Code int `json:"code"`
	}
	json.Unmarshal(w.Body.Bytes(), &resp)
	if resp.Code != 0 {
		t.Errorf("code = %d, want 0", resp.Code)
	}
}

func TestUpdateProfile_Forbidden(t *testing.T) {
	svc := &mockProfileSvc{}
	gin.SetMode(gin.TestMode)
	r := gin.New()
	h := NewProfileHandler(svc)

	r.PATCH("/api/v1/profiles/:id", func(c *gin.Context) {
		setUserID(c, 1)
		h.UpdateProfile(c)
	})

	w := httptest.NewRecorder()
	body := `{"nickname":"hack"}`
	req, _ := http.NewRequest("PATCH", "/api/v1/profiles/2", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	r.ServeHTTP(w, req)

	if w.Code != http.StatusForbidden {
		t.Fatalf("status = %d, want 403", w.Code)
	}
}

func TestUpdateProfile_InvalidID(t *testing.T) {
	svc := &mockProfileSvc{}
	gin.SetMode(gin.TestMode)
	r := gin.New()
	h := NewProfileHandler(svc)

	r.PATCH("/api/v1/profiles/:id", func(c *gin.Context) {
		setUserID(c, 1)
		h.UpdateProfile(c)
	})

	w := httptest.NewRecorder()
	body := `{"nickname":"test"}`
	req, _ := http.NewRequest("PATCH", "/api/v1/profiles/abc", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	r.ServeHTTP(w, req)

	if w.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400", w.Code)
	}
}

func TestUpdateProfile_InvalidBody(t *testing.T) {
	svc := &mockProfileSvc{}
	gin.SetMode(gin.TestMode)
	r := gin.New()
	h := NewProfileHandler(svc)

	r.PATCH("/api/v1/profiles/:id", func(c *gin.Context) {
		setUserID(c, 1)
		h.UpdateProfile(c)
	})

	w := httptest.NewRecorder()
	req, _ := http.NewRequest("PATCH", "/api/v1/profiles/1", strings.NewReader(`not json`))
	req.Header.Set("Content-Type", "application/json")
	r.ServeHTTP(w, req)

	if w.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400", w.Code)
	}
}

func TestUpdateProfile_ServiceError(t *testing.T) {
	svc := &mockProfileSvc{
		updateErr: errcode.ErrInternal.WithMsg("update failed"),
	}
	gin.SetMode(gin.TestMode)
	r := gin.New()
	h := NewProfileHandler(svc)

	r.PATCH("/api/v1/profiles/:id", func(c *gin.Context) {
		setUserID(c, 1)
		h.UpdateProfile(c)
	})

	w := httptest.NewRecorder()
	body := `{"nickname":"test"}`
	req, _ := http.NewRequest("PATCH", "/api/v1/profiles/1", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	r.ServeHTTP(w, req)

	if w.Code != http.StatusInternalServerError {
		t.Fatalf("status = %d, want 500", w.Code)
	}
}

func TestUpdateProfile_EmptyBody(t *testing.T) {
	svc := &mockProfileSvc{}
	gin.SetMode(gin.TestMode)
	r := gin.New()
	h := NewProfileHandler(svc)

	r.PATCH("/api/v1/profiles/:id", func(c *gin.Context) {
		setUserID(c, 1)
		h.UpdateProfile(c)
	})

	w := httptest.NewRecorder()
	req, _ := http.NewRequest("PATCH", "/api/v1/profiles/1", strings.NewReader(``))
	req.Header.Set("Content-Type", "application/json")
	r.ServeHTTP(w, req)

	if w.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400, body: %s", w.Code, w.Body.String())
	}
}

func TestUpdateProfile_NoAuth(t *testing.T) {
	svc := &mockProfileSvc{}
	r := setupRouter(svc)

	w := httptest.NewRecorder()
	req, _ := http.NewRequest("PATCH", "/api/v1/profiles/1", strings.NewReader(`{"nickname":"test"}`))
	req.Header.Set("Content-Type", "application/json")
	r.ServeHTTP(w, req)

	if w.Code != http.StatusUnauthorized {
		t.Fatalf("status = %d, want 401", w.Code)
	}
}

// --- NewProfileHandler ---

func TestNewProfileHandler(t *testing.T) {
	h := NewProfileHandler(nil)
	if h == nil {
		t.Fatal("expected non-nil handler")
	}
}

func TestRegisterRoutes(t *testing.T) {
	h := NewProfileHandler(nil)
	r := gin.New()
	group := r.Group("/api/v1")
	h.RegisterRoutes(group)

	routes := r.Routes()
	if len(routes) == 0 {
		t.Error("expected routes to be registered")
	}
}
