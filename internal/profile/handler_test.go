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

func setupRouter(svc ProfileServiceInterface) *gin.Engine {
	gin.SetMode(gin.TestMode)
	r := gin.New()
	h := NewProfileHandler(svc)
	h.RegisterRoutes(r.Group("/api/v1"))
	return r
}

func setUserID(c *gin.Context, uid uint64) {
	c.Set("user_id", uid)
}

// setupRouterWithAuth 创建一个带 userID 的 Gin 路由，用于测试需要认证的接口。
func setupRouterWithAuth(svc ProfileServiceInterface, uid uint64) *gin.Engine {
	gin.SetMode(gin.TestMode)
	r := gin.New()
	h := NewProfileHandler(svc)
	// 使用中间件设置 userID，模拟 middleware.GetUserID 的行为。
	// 注意：UpdateProfile handler 内部调用 middleware.GetUserID(c)，
	// 这里必须使用 Set("user_id", uid) 才能匹配。
	r.Use(func(c *gin.Context) {
		setUserID(c, uid)
	})
	h.RegisterRoutes(r.Group("/api/v1"))
	return r
}

// ============================================================================
// GetProfile handler tests (table-driven)
// ============================================================================

// getProfileTestCase 定义 GetProfile 测试的通用结构。
type getProfileTestCase struct {
	name       string
	svc        *mockProfileSvc
	path       string
	wantStatus int
	wantCode   int // 响应 JSON 中的 code 字段期望值
}

func TestGetProfile(t *testing.T) {
	tests := []getProfileTestCase{
		{
			name:       "success",
			svc:        &mockProfileSvc{getUser: &UserProfile{ID: 1, Nickname: "alice"}},
			path:       "/api/v1/profiles/1",
			wantStatus: http.StatusOK,
			wantCode:   0,
		},
		{
			name:       "zero_id",
			svc:        &mockProfileSvc{getUser: &UserProfile{ID: 0}},
			path:       "/api/v1/profiles/0",
			wantStatus: http.StatusOK,
			wantCode:   0,
		},
		{
			name:       "invalid_id",
			svc:        &mockProfileSvc{},
			path:       "/api/v1/profiles/abc",
			wantStatus: http.StatusBadRequest,
			wantCode:   -1, // 不校验 code 字段
		},
		{
			name:       "negative_id",
			svc:        &mockProfileSvc{},
			path:       "/api/v1/profiles/-1",
			wantStatus: http.StatusBadRequest,
			wantCode:   -1,
		},
		{
			name:       "not_found",
			svc:        &mockProfileSvc{getErr: errcode.ErrNotFound},
			path:       "/api/v1/profiles/999",
			wantStatus: http.StatusNotFound,
			wantCode:   -1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := setupRouter(tt.svc)
			w := httptest.NewRecorder()
			req, _ := http.NewRequest("GET", tt.path, nil)
			r.ServeHTTP(w, req)
			if w.Code != tt.wantStatus {
				t.Fatalf("status = %d, want %d", w.Code, tt.wantStatus)
			}
			if tt.wantCode >= 0 {
				var resp struct {
					Code int `json:"code"`
				}
				json.Unmarshal(w.Body.Bytes(), &resp)
				if resp.Code != tt.wantCode {
					t.Errorf("code = %d, want %d", resp.Code, tt.wantCode)
				}
			}
		})
	}
}

// ============================================================================
// UpdateProfile handler tests (table-driven)
// ============================================================================

// updateProfileTestCase 定义 UpdateProfile 测试的通用结构。
type updateProfileTestCase struct {
	name       string
	svc        *mockProfileSvc
	uid        uint64 // caller userID (0 表示不设置 userID)
	path       string // 请求路径，含 :id 路径参数
	body       string // JSON 请求体
	wantStatus int
	wantCode   int // 响应 JSON 中的 code 字段期望值 (-1 表示不校验)
}

func TestUpdateProfile(t *testing.T) {
	tests := []updateProfileTestCase{
		{
			name:       "no_auth",
			svc:        &mockProfileSvc{},
			uid:        0,
			path:       "/api/v1/profiles/1",
			body:       `{"nickname":"test"}`,
			wantStatus: http.StatusUnauthorized,
			wantCode:   -1,
		},
		{
			name:       "with_auth",
			svc:        &mockProfileSvc{},
			uid:        1,
			path:       "/api/v1/profiles/1",
			body:       `{"nickname":"new-name"}`,
			wantStatus: http.StatusOK,
			wantCode:   0,
		},
		{
			name:       "forbidden",
			svc:        &mockProfileSvc{},
			uid:        1,
			path:       "/api/v1/profiles/2",
			body:       `{"nickname":"hack"}`,
			wantStatus: http.StatusForbidden,
			wantCode:   -1,
		},
		{
			name:       "invalid_id",
			svc:        &mockProfileSvc{},
			uid:        1,
			path:       "/api/v1/profiles/abc",
			body:       `{"nickname":"test"}`,
			wantStatus: http.StatusBadRequest,
			wantCode:   -1,
		},
		{
			name:       "invalid_body",
			svc:        &mockProfileSvc{},
			uid:        1,
			path:       "/api/v1/profiles/1",
			body:       `not json`,
			wantStatus: http.StatusBadRequest,
			wantCode:   -1,
		},
		{
			name:       "empty_body",
			svc:        &mockProfileSvc{},
			uid:        1,
			path:       "/api/v1/profiles/1",
			body:       ``,
			wantStatus: http.StatusBadRequest,
			wantCode:   -1,
		},
		{
			name:       "service_error",
			svc:        &mockProfileSvc{updateErr: errcode.ErrInternal.WithMsg("update failed")},
			uid:        1,
			path:       "/api/v1/profiles/1",
			body:       `{"nickname":"test"}`,
			wantStatus: http.StatusInternalServerError,
			wantCode:   -1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var r *gin.Engine
			if tt.uid > 0 {
				r = setupRouterWithAuth(tt.svc, tt.uid)
			} else {
				// 无认证时使用不注入 userID 的路由
				r = setupRouter(tt.svc)
			}

			w := httptest.NewRecorder()
			bodyReader := strings.NewReader(tt.body)
			req, _ := http.NewRequest("PATCH", tt.path, bodyReader)
			req.Header.Set("Content-Type", "application/json")
			r.ServeHTTP(w, req)

			if w.Code != tt.wantStatus {
				t.Fatalf("status = %d, want %d, body: %s", w.Code, tt.wantStatus, w.Body.String())
			}
			if tt.wantCode >= 0 {
				var resp struct {
					Code int `json:"code"`
				}
				json.Unmarshal(w.Body.Bytes(), &resp)
				if resp.Code != tt.wantCode {
					t.Errorf("code = %d, want %d", resp.Code, tt.wantCode)
				}
			}
		})
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
