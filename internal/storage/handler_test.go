package storage

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/zhiguang/app/pkg/errcode"
)

// --- mock storage service ---

type mockStorageSvc struct {
	presignURL string
	objectKey  string
	publicURL  string
	expiry     time.Duration
	err        error
}

func (m *mockStorageSvc) GeneratePresignedPutURL(objectKey string, expiry time.Duration) (string, error) {
	if m.err != nil {
		return "", m.err
	}
	return m.presignURL, nil
}

func (m *mockStorageSvc) GeneratePresignedGetURL(objectKey string, expiry time.Duration) (string, error) {
	return m.presignURL, nil
}

func (m *mockStorageSvc) GenerateObjectKey(folder, fileName string) string {
	return m.objectKey
}

func (m *mockStorageSvc) PublicURL(objectKey string) string {
	return m.publicURL
}

func (m *mockStorageSvc) PresignExpiry() time.Duration {
	return m.expiry
}

// --- helpers ---

func setupStorageRouter(svc StorageServiceInterface) *gin.Engine {
	gin.SetMode(gin.TestMode)
	r := gin.New()
	h := NewStorageHandler(svc)
	h.RegisterRoutes(r.Group("/api/v1"))
	return r
}

func setUserID(c *gin.Context, uid uint64) {
	c.Set("user_id", uid)
}

// --- Presign ---

func TestPresign_Success(t *testing.T) {
	svc := &mockStorageSvc{
		presignURL: "https://presign.example.com/upload",
		objectKey:  "images/abc123_test.jpg",
		publicURL:  "https://cdn.example.com/images/abc123_test.jpg",
		expiry:     10 * time.Minute,
	}
	gin.SetMode(gin.TestMode)
	r := gin.New()
	h := NewStorageHandler(svc)
	r.POST("/api/v1/storage/presign", func(c *gin.Context) {
		setUserID(c, 1)
		h.Presign(c)
	})

	w := httptest.NewRecorder()
	body := `{"file_name":"test.jpg","content_type":"image/jpeg","folder":"images"}`
	req, _ := http.NewRequest("POST", "/api/v1/storage/presign", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	r.ServeHTTP(w, req)

	if w.Code != http.StatusCreated {
		t.Fatalf("status = %d, want 201, body: %s", w.Code, w.Body.String())
	}

	var resp struct {
		Code int `json:"code"`
		Data struct {
			UploadURL string `json:"upload_url"`
			ObjectKey string `json:"object_key"`
			PublicURL string `json:"public_url"`
			ExpireAt  string `json:"expire_at"`
		} `json:"data"`
	}
	if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
		t.Fatalf("unmarshal error: %v", err)
	}
	if resp.Code != 0 {
		t.Errorf("code = %d, want 0", resp.Code)
	}
	if resp.Data.UploadURL != "https://presign.example.com/upload" {
		t.Errorf("UploadURL = %q", resp.Data.UploadURL)
	}
	if resp.Data.ObjectKey != "images/abc123_test.jpg" {
		t.Errorf("ObjectKey = %q", resp.Data.ObjectKey)
	}
	if resp.Data.PublicURL != "https://cdn.example.com/images/abc123_test.jpg" {
		t.Errorf("PublicURL = %q", resp.Data.PublicURL)
	}
	if resp.Data.ExpireAt == "" {
		t.Error("ExpireAt should not be empty")
	}
}

func TestPresign_NoAuth(t *testing.T) {
	svc := &mockStorageSvc{}
	r := setupStorageRouter(svc)

	w := httptest.NewRecorder()
	body := `{"file_name":"test.jpg","content_type":"image/jpeg"}`
	req, _ := http.NewRequest("POST", "/api/v1/storage/presign", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	r.ServeHTTP(w, req)

	if w.Code != http.StatusUnauthorized {
		t.Fatalf("status = %d, want 401", w.Code)
	}
}

func TestPresign_NilService(t *testing.T) {
	gin.SetMode(gin.TestMode)
	r := gin.New()
	h := NewStorageHandler(nil)
	r.POST("/api/v1/storage/presign", func(c *gin.Context) {
		setUserID(c, 1)
		h.Presign(c)
	})

	w := httptest.NewRecorder()
	body := `{"file_name":"test.jpg","content_type":"image/jpeg"}`
	req, _ := http.NewRequest("POST", "/api/v1/storage/presign", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	r.ServeHTTP(w, req)

	if w.Code != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want 503", w.Code)
	}
}

func TestPresign_InvalidBody(t *testing.T) {
	svc := &mockStorageSvc{}
	gin.SetMode(gin.TestMode)
	r := gin.New()
	h := NewStorageHandler(svc)
	r.POST("/api/v1/storage/presign", func(c *gin.Context) {
		setUserID(c, 1)
		h.Presign(c)
	})

	w := httptest.NewRecorder()
	req, _ := http.NewRequest("POST", "/api/v1/storage/presign", strings.NewReader(`not json`))
	req.Header.Set("Content-Type", "application/json")
	r.ServeHTTP(w, req)

	if w.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400", w.Code)
	}
}

func TestPresign_MissingFileName(t *testing.T) {
	svc := &mockStorageSvc{}
	gin.SetMode(gin.TestMode)
	r := gin.New()
	h := NewStorageHandler(svc)
	r.POST("/api/v1/storage/presign", func(c *gin.Context) {
		setUserID(c, 1)
		h.Presign(c)
	})

	w := httptest.NewRecorder()
	body := `{"content_type":"image/jpeg"}`
	req, _ := http.NewRequest("POST", "/api/v1/storage/presign", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	r.ServeHTTP(w, req)

	if w.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400 (missing required fields)", w.Code)
	}
}

func TestPresign_MissingContentType(t *testing.T) {
	svc := &mockStorageSvc{}
	gin.SetMode(gin.TestMode)
	r := gin.New()
	h := NewStorageHandler(svc)
	r.POST("/api/v1/storage/presign", func(c *gin.Context) {
		setUserID(c, 1)
		h.Presign(c)
	})

	w := httptest.NewRecorder()
	body := `{"file_name":"test.jpg"}`
	req, _ := http.NewRequest("POST", "/api/v1/storage/presign", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	r.ServeHTTP(w, req)

	if w.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400 (missing required fields)", w.Code)
	}
}

func TestPresign_GenerateError(t *testing.T) {
	svc := &mockStorageSvc{
		err: errcode.ErrInternal,
	}
	gin.SetMode(gin.TestMode)
	r := gin.New()
	h := NewStorageHandler(svc)
	r.POST("/api/v1/storage/presign", func(c *gin.Context) {
		setUserID(c, 1)
		h.Presign(c)
	})

	w := httptest.NewRecorder()
	body := `{"file_name":"test.jpg","content_type":"image/jpeg"}`
	req, _ := http.NewRequest("POST", "/api/v1/storage/presign", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	r.ServeHTTP(w, req)

	if w.Code != http.StatusInternalServerError {
		t.Fatalf("status = %d, want 500", w.Code)
	}
}

func TestPresign_FolderOptional(t *testing.T) {
	svc := &mockStorageSvc{
		presignURL: "https://presign.example.com/upload",
		objectKey:  "uploads/uuid_test.jpg",
		publicURL:  "https://cdn.example.com/uploads/uuid_test.jpg",
		expiry:     10 * time.Minute,
	}
	gin.SetMode(gin.TestMode)
	r := gin.New()
	h := NewStorageHandler(svc)
	r.POST("/api/v1/storage/presign", func(c *gin.Context) {
		setUserID(c, 1)
		h.Presign(c)
	})

	w := httptest.NewRecorder()
	body := `{"file_name":"test.jpg","content_type":"image/jpeg"}`
	req, _ := http.NewRequest("POST", "/api/v1/storage/presign", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	r.ServeHTTP(w, req)

	if w.Code != http.StatusCreated {
		t.Fatalf("status = %d, want 201", w.Code)
	}
}

// --- NewStorageHandler ---

func TestNewStorageHandler(t *testing.T) {
	h := NewStorageHandler(nil)
	if h == nil {
		t.Fatal("expected non-nil handler")
	}
	if h.svc != nil {
		t.Error("expected nil svc")
	}
}

func TestNewStorageHandler_WithService(t *testing.T) {
	svc := &mockStorageSvc{}
	h := NewStorageHandler(svc)
	if h.svc != svc {
		t.Error("svc not set")
	}
}

func TestRegisterRoutes(t *testing.T) {
	h := NewStorageHandler(nil)
	r := gin.New()
	group := r.Group("/api/v1")
	h.RegisterRoutes(group)

	routes := r.Routes()
	found := false
	for _, route := range routes {
		if route.Path == "/api/v1/storage/presign" && route.Method == "POST" {
			found = true
			break
		}
	}
	if !found {
		t.Error("POST /storage/presign route not registered")
	}
}
