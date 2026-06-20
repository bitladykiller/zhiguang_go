package knowpost

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/zhiguang/app/pkg/errcode"
)

// ============================================================================
// Stubs for KnowPostWriteService, KnowPostReadService, KnowPostFeedServiceInterface
// ============================================================================

type stubWriteSvc struct {
	createDraftFn    func(ctx context.Context, creatorID uint64) (uint64, error)
	confirmContentFn func(ctx context.Context, creatorID, id uint64, objectKey, etag, sha256 string, size uint64) error
	updateMetadataFn func(ctx context.Context, creatorID, id uint64, req *KnowPostPatchRequest) error
	publishFn        func(ctx context.Context, creatorID, id uint64) error
	updateTopFn      func(ctx context.Context, creatorID, id uint64, isTop bool) error
	updateVisFn      func(ctx context.Context, creatorID, id uint64, visible KnowPostVisibility) error
	deleteFn         func(ctx context.Context, creatorID, id uint64) error
}

func (s *stubWriteSvc) CreateDraft(ctx context.Context, creatorID uint64) (uint64, error) {
	if s.createDraftFn == nil {
		return 0, nil
	}
	return s.createDraftFn(ctx, creatorID)
}
func (s *stubWriteSvc) ConfirmContent(ctx context.Context, creatorID, id uint64, objectKey, etag, sha256 string, size uint64) error {
	if s.confirmContentFn == nil {
		return nil
	}
	return s.confirmContentFn(ctx, creatorID, id, objectKey, etag, sha256, size)
}
func (s *stubWriteSvc) UpdateMetadata(ctx context.Context, creatorID, id uint64, req *KnowPostPatchRequest) error {
	if s.updateMetadataFn == nil {
		return nil
	}
	return s.updateMetadataFn(ctx, creatorID, id, req)
}
func (s *stubWriteSvc) Publish(ctx context.Context, creatorID, id uint64) error {
	if s.publishFn == nil {
		return nil
	}
	return s.publishFn(ctx, creatorID, id)
}
func (s *stubWriteSvc) UpdateTop(ctx context.Context, creatorID, id uint64, isTop bool) error {
	if s.updateTopFn == nil {
		return nil
	}
	return s.updateTopFn(ctx, creatorID, id, isTop)
}
func (s *stubWriteSvc) UpdateVisibility(ctx context.Context, creatorID, id uint64, visible KnowPostVisibility) error {
	if s.updateVisFn == nil {
		return nil
	}
	return s.updateVisFn(ctx, creatorID, id, visible)
}
func (s *stubWriteSvc) Delete(ctx context.Context, creatorID, id uint64) error {
	if s.deleteFn == nil {
		return nil
	}
	return s.deleteFn(ctx, creatorID, id)
}

type stubReadSvc struct {
	getDetailFn func(ctx context.Context, id uint64, currentUserID *uint64) (*KnowPostDetailResponse, error)
}

func (s *stubReadSvc) GetDetail(ctx context.Context, id uint64, currentUserID *uint64) (*KnowPostDetailResponse, error) {
	if s.getDetailFn == nil {
		return nil, nil
	}
	return s.getDetailFn(ctx, id, currentUserID)
}

type stubFeedSvc struct {
	getPublicFeedFn func(ctx context.Context, page, size int, currentUserID *uint64) (*FeedPageResponse, error)
	getMineFeedFn   func(ctx context.Context, userID uint64, page, size int) (*FeedPageResponse, error)
}

func (s *stubFeedSvc) GetPublicFeed(ctx context.Context, page, size int, currentUserID *uint64) (*FeedPageResponse, error) {
	if s.getPublicFeedFn == nil {
		return nil, nil
	}
	return s.getPublicFeedFn(ctx, page, size, currentUserID)
}
func (s *stubFeedSvc) GetMyPublished(ctx context.Context, userID uint64, page, size int) (*FeedPageResponse, error) {
	return s.GetMineFeed(ctx, userID, page, size)
}
func (s *stubFeedSvc) GetMineFeed(ctx context.Context, userID uint64, page, size int) (*FeedPageResponse, error) {
	if s.getMineFeedFn == nil {
		return nil, nil
	}
	return s.getMineFeedFn(ctx, userID, page, size)
}

// ============================================================================
// Test helpers
// ============================================================================

func newKnowPostHandler(writeSvc KnowPostWriteService, readSvc KnowPostReadService, feedSvc KnowPostFeedServiceInterface) *KnowPostHandler {
	return NewKnowPostHandler(writeSvc, readSvc, feedSvc)
}

func setupKnowPostRouter(h *KnowPostHandler, auth bool) *gin.Engine {
	gin.SetMode(gin.TestMode)
	r := gin.New()
	if auth {
		r.Use(func(c *gin.Context) {
			c.Set("user_id", uint64(42))
		})
	}
	h.RegisterRoutes(r.Group("/api/v1"))
	return r
}

func performJSON(r *gin.Engine, method, path string, body any) *httptest.ResponseRecorder {
	w := httptest.NewRecorder()
	var req *http.Request
	if body != nil {
		b, _ := json.Marshal(body)
		req = httptest.NewRequest(method, path, bytes.NewReader(b))
		req.Header.Set("Content-Type", "application/json")
	} else {
		req = httptest.NewRequest(method, path, nil)
	}
	r.ServeHTTP(w, req)
	return w
}

// ============================================================================
// CreateDraft tests
// ============================================================================

func TestCreateDraft_Success(t *testing.T) {
	writeSvc := &stubWriteSvc{
		createDraftFn: func(_ context.Context, creatorID uint64) (uint64, error) {
			if creatorID != 42 {
				t.Errorf("creatorID = %d, want 42", creatorID)
			}
			return 1001, nil
		},
	}
	h := newKnowPostHandler(writeSvc, &stubReadSvc{}, &stubFeedSvc{})
	r := setupKnowPostRouter(h, true)
	w := performJSON(r, "POST", "/api/v1/knowposts/draft", nil)

	if w.Code != http.StatusCreated {
		t.Fatalf("status = %d, want 201, body: %s", w.Code, w.Body.String())
	}
	var resp struct {
		Code int `json:"code"`
		Data struct {
			ID string `json:"id"`
		} `json:"data"`
	}
	if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if resp.Code != 0 {
		t.Errorf("code = %d, want 0", resp.Code)
	}
	if resp.Data.ID != "1001" {
		t.Errorf("id = %q, want 1001", resp.Data.ID)
	}
}

func TestCreateDraft_Unauthorized(t *testing.T) {
	h := newKnowPostHandler(&stubWriteSvc{}, &stubReadSvc{}, &stubFeedSvc{})
	r := setupKnowPostRouter(h, false)
	w := performJSON(r, "POST", "/api/v1/knowposts/draft", nil)

	if w.Code != http.StatusUnauthorized {
		t.Fatalf("status = %d, want 401", w.Code)
	}
}

func TestCreateDraft_ServiceError(t *testing.T) {
	writeSvc := &stubWriteSvc{
		createDraftFn: func(_ context.Context, _ uint64) (uint64, error) {
			return 0, errors.New("db error")
		},
	}
	h := newKnowPostHandler(writeSvc, &stubReadSvc{}, &stubFeedSvc{})
	r := setupKnowPostRouter(h, true)
	w := performJSON(r, "POST", "/api/v1/knowposts/draft", nil)

	if w.Code != http.StatusInternalServerError {
		t.Fatalf("status = %d, want 500", w.Code)
	}
}

// ============================================================================
// ConfirmContent tests
// ============================================================================

func TestConfirmContent_Success(t *testing.T) {
	writeSvc := &stubWriteSvc{
		confirmContentFn: func(_ context.Context, creatorID, id uint64, objectKey, etag, sha256 string, size uint64) error {
			if creatorID != 42 || id != 123 {
				t.Errorf("unexpected args: creatorID=%d id=%d", creatorID, id)
			}
			return nil
		},
	}
	h := newKnowPostHandler(writeSvc, &stubReadSvc{}, &stubFeedSvc{})
	r := setupKnowPostRouter(h, true)
	body := map[string]any{
		"object_key": "posts/abc",
		"etag":       "etag123",
		"sha256":     "sha256val",
		"size":       1024,
	}
	w := performJSON(r, "PUT", "/api/v1/knowposts/123/content", body)

	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", w.Code)
	}
}

func TestConfirmContent_Unauthorized(t *testing.T) {
	h := newKnowPostHandler(&stubWriteSvc{}, &stubReadSvc{}, &stubFeedSvc{})
	r := setupKnowPostRouter(h, false)
	w := performJSON(r, "PUT", "/api/v1/knowposts/123/content", map[string]string{"object_key": "x"})

	if w.Code != http.StatusUnauthorized {
		t.Fatalf("status = %d, want 401", w.Code)
	}
}

func TestConfirmContent_InvalidID(t *testing.T) {
	h := newKnowPostHandler(&stubWriteSvc{}, &stubReadSvc{}, &stubFeedSvc{})
	r := setupKnowPostRouter(h, true)
	w := performJSON(r, "PUT", "/api/v1/knowposts/abc/content", map[string]string{"object_key": "x"})

	if w.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400", w.Code)
	}
}

func TestConfirmContent_InvalidBody(t *testing.T) {
	h := newKnowPostHandler(&stubWriteSvc{}, &stubReadSvc{}, &stubFeedSvc{})
	r := setupKnowPostRouter(h, true)
	w := performJSON(r, "PUT", "/api/v1/knowposts/123/content", map[string]string{"wrong": "field"})

	if w.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400", w.Code)
	}
}

func TestConfirmContent_ServiceError(t *testing.T) {
	writeSvc := &stubWriteSvc{
		confirmContentFn: func(_ context.Context, _, _ uint64, _, _, _ string, _ uint64) error {
			return errors.New("storage error")
		},
	}
	h := newKnowPostHandler(writeSvc, &stubReadSvc{}, &stubFeedSvc{})
	r := setupKnowPostRouter(h, true)
	body := map[string]any{
		"object_key": "posts/abc",
		"etag":       "e",
		"sha256":     "s",
		"size":       10,
	}
	w := performJSON(r, "PUT", "/api/v1/knowposts/123/content", body)

	if w.Code != http.StatusInternalServerError {
		t.Fatalf("status = %d, want 500", w.Code)
	}
}

// ============================================================================
// UpdateMetadata tests
// ============================================================================

func TestUpdateMetadata_Success(t *testing.T) {
	writeSvc := &stubWriteSvc{
		updateMetadataFn: func(_ context.Context, creatorID, id uint64, req *KnowPostPatchRequest) error {
			if creatorID != 42 || id != 123 {
				t.Errorf("unexpected args: creatorID=%d id=%d", creatorID, id)
			}
			return nil
		},
	}
	h := newKnowPostHandler(writeSvc, &stubReadSvc{}, &stubFeedSvc{})
	r := setupKnowPostRouter(h, true)
	w := performJSON(r, "PUT", "/api/v1/knowposts/123/metadata", map[string]string{"title": "New Title"})

	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", w.Code)
	}
}

func TestUpdateMetadata_Unauthorized(t *testing.T) {
	h := newKnowPostHandler(&stubWriteSvc{}, &stubReadSvc{}, &stubFeedSvc{})
	r := setupKnowPostRouter(h, false)
	w := performJSON(r, "PUT", "/api/v1/knowposts/123/metadata", map[string]string{"title": "x"})

	if w.Code != http.StatusUnauthorized {
		t.Fatalf("status = %d, want 401", w.Code)
	}
}

func TestUpdateMetadata_InvalidID(t *testing.T) {
	h := newKnowPostHandler(&stubWriteSvc{}, &stubReadSvc{}, &stubFeedSvc{})
	r := setupKnowPostRouter(h, true)
	w := performJSON(r, "PUT", "/api/v1/knowposts/abc/metadata", map[string]string{"title": "x"})

	if w.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400", w.Code)
	}
}

func TestUpdateMetadata_ServiceError(t *testing.T) {
	writeSvc := &stubWriteSvc{
		updateMetadataFn: func(_ context.Context, _, _ uint64, _ *KnowPostPatchRequest) error {
			return errors.New("db error")
		},
	}
	h := newKnowPostHandler(writeSvc, &stubReadSvc{}, &stubFeedSvc{})
	r := setupKnowPostRouter(h, true)
	w := performJSON(r, "PUT", "/api/v1/knowposts/123/metadata", map[string]string{"title": "x"})

	if w.Code != http.StatusInternalServerError {
		t.Fatalf("status = %d, want 500", w.Code)
	}
}

// ============================================================================
// Publish tests
// ============================================================================

func TestPublish_Success(t *testing.T) {
	writeSvc := &stubWriteSvc{
		publishFn: func(_ context.Context, creatorID, id uint64) error {
			if creatorID != 42 || id != 123 {
				t.Errorf("unexpected args: creatorID=%d id=%d", creatorID, id)
			}
			return nil
		},
	}
	h := newKnowPostHandler(writeSvc, &stubReadSvc{}, &stubFeedSvc{})
	r := setupKnowPostRouter(h, true)
	w := performJSON(r, "POST", "/api/v1/knowposts/123/publish", nil)

	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", w.Code)
	}
}

func TestPublish_Unauthorized(t *testing.T) {
	h := newKnowPostHandler(&stubWriteSvc{}, &stubReadSvc{}, &stubFeedSvc{})
	r := setupKnowPostRouter(h, false)
	w := performJSON(r, "POST", "/api/v1/knowposts/123/publish", nil)

	if w.Code != http.StatusUnauthorized {
		t.Fatalf("status = %d, want 401", w.Code)
	}
}

func TestPublish_InvalidID(t *testing.T) {
	h := newKnowPostHandler(&stubWriteSvc{}, &stubReadSvc{}, &stubFeedSvc{})
	r := setupKnowPostRouter(h, true)
	w := performJSON(r, "POST", "/api/v1/knowposts/abc/publish", nil)

	if w.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400", w.Code)
	}
}

func TestPublish_ServiceError(t *testing.T) {
	writeSvc := &stubWriteSvc{
		publishFn: func(_ context.Context, _, _ uint64) error {
			return errors.New("publish error")
		},
	}
	h := newKnowPostHandler(writeSvc, &stubReadSvc{}, &stubFeedSvc{})
	r := setupKnowPostRouter(h, true)
	w := performJSON(r, "POST", "/api/v1/knowposts/123/publish", nil)

	if w.Code != http.StatusInternalServerError {
		t.Fatalf("status = %d, want 500", w.Code)
	}
}

// ============================================================================
// UpdateTop tests
// ============================================================================

func TestUpdateTop_Success(t *testing.T) {
	writeSvc := &stubWriteSvc{
		updateTopFn: func(_ context.Context, creatorID, id uint64, isTop bool) error {
			if creatorID != 42 || id != 123 || !isTop {
				t.Errorf("unexpected args: creatorID=%d id=%d isTop=%v", creatorID, id, isTop)
			}
			return nil
		},
	}
	h := newKnowPostHandler(writeSvc, &stubReadSvc{}, &stubFeedSvc{})
	r := setupKnowPostRouter(h, true)
	w := performJSON(r, "PUT", "/api/v1/knowposts/123/top", map[string]bool{"is_top": true})

	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", w.Code)
	}
}

func TestUpdateTop_Unauthorized(t *testing.T) {
	h := newKnowPostHandler(&stubWriteSvc{}, &stubReadSvc{}, &stubFeedSvc{})
	r := setupKnowPostRouter(h, false)
	w := performJSON(r, "PUT", "/api/v1/knowposts/123/top", map[string]bool{"is_top": true})

	if w.Code != http.StatusUnauthorized {
		t.Fatalf("status = %d, want 401", w.Code)
	}
}

func TestUpdateTop_InvalidID(t *testing.T) {
	h := newKnowPostHandler(&stubWriteSvc{}, &stubReadSvc{}, &stubFeedSvc{})
	r := setupKnowPostRouter(h, true)
	w := performJSON(r, "PUT", "/api/v1/knowposts/abc/top", map[string]bool{"is_top": true})

	if w.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400", w.Code)
	}
}

func TestUpdateTop_ServiceError(t *testing.T) {
	writeSvc := &stubWriteSvc{
		updateTopFn: func(_ context.Context, _, _ uint64, _ bool) error {
			return errors.New("db error")
		},
	}
	h := newKnowPostHandler(writeSvc, &stubReadSvc{}, &stubFeedSvc{})
	r := setupKnowPostRouter(h, true)
	w := performJSON(r, "PUT", "/api/v1/knowposts/123/top", map[string]bool{"is_top": true})

	if w.Code != http.StatusInternalServerError {
		t.Fatalf("status = %d, want 500", w.Code)
	}
}

// ============================================================================
// UpdateVisibility tests
// ============================================================================

func TestUpdateVisibility_Success(t *testing.T) {
	writeSvc := &stubWriteSvc{
		updateVisFn: func(_ context.Context, creatorID, id uint64, visible KnowPostVisibility) error {
			if creatorID != 42 || id != 123 || visible != KnowPostVisibilityPublic {
				t.Errorf("unexpected args: creatorID=%d id=%d visible=%q", creatorID, id, visible)
			}
			return nil
		},
	}
	h := newKnowPostHandler(writeSvc, &stubReadSvc{}, &stubFeedSvc{})
	r := setupKnowPostRouter(h, true)
	w := performJSON(r, "PUT", "/api/v1/knowposts/123/visibility", map[string]string{"visible": "public"})

	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", w.Code)
	}
}

func TestUpdateVisibility_Unauthorized(t *testing.T) {
	h := newKnowPostHandler(&stubWriteSvc{}, &stubReadSvc{}, &stubFeedSvc{})
	r := setupKnowPostRouter(h, false)
	w := performJSON(r, "PUT", "/api/v1/knowposts/123/visibility", map[string]string{"visible": "public"})

	if w.Code != http.StatusUnauthorized {
		t.Fatalf("status = %d, want 401", w.Code)
	}
}

func TestUpdateVisibility_InvalidID(t *testing.T) {
	h := newKnowPostHandler(&stubWriteSvc{}, &stubReadSvc{}, &stubFeedSvc{})
	r := setupKnowPostRouter(h, true)
	w := performJSON(r, "PUT", "/api/v1/knowposts/abc/visibility", map[string]string{"visible": "public"})

	if w.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400", w.Code)
	}
}

func TestUpdateVisibility_MissingField(t *testing.T) {
	h := newKnowPostHandler(&stubWriteSvc{}, &stubReadSvc{}, &stubFeedSvc{})
	r := setupKnowPostRouter(h, true)
	w := performJSON(r, "PUT", "/api/v1/knowposts/123/visibility", map[string]string{"wrong": "field"})

	if w.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400 (missing required 'visible' field)", w.Code)
	}
}

func TestUpdateVisibility_ServiceError(t *testing.T) {
	writeSvc := &stubWriteSvc{
		updateVisFn: func(_ context.Context, _, _ uint64, _ KnowPostVisibility) error {
			return errors.New("db error")
		},
	}
	h := newKnowPostHandler(writeSvc, &stubReadSvc{}, &stubFeedSvc{})
	r := setupKnowPostRouter(h, true)
	w := performJSON(r, "PUT", "/api/v1/knowposts/123/visibility", map[string]string{"visible": "public"})

	if w.Code != http.StatusInternalServerError {
		t.Fatalf("status = %d, want 500", w.Code)
	}
}

// ============================================================================
// Delete tests
// ============================================================================

func TestDelete_Success(t *testing.T) {
	writeSvc := &stubWriteSvc{
		deleteFn: func(_ context.Context, creatorID, id uint64) error {
			if creatorID != 42 || id != 123 {
				t.Errorf("unexpected args: creatorID=%d id=%d", creatorID, id)
			}
			return nil
		},
	}
	h := newKnowPostHandler(writeSvc, &stubReadSvc{}, &stubFeedSvc{})
	r := setupKnowPostRouter(h, true)
	w := performJSON(r, "DELETE", "/api/v1/knowposts/123", nil)

	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", w.Code)
	}
}

func TestDelete_Unauthorized(t *testing.T) {
	h := newKnowPostHandler(&stubWriteSvc{}, &stubReadSvc{}, &stubFeedSvc{})
	r := setupKnowPostRouter(h, false)
	w := performJSON(r, "DELETE", "/api/v1/knowposts/123", nil)

	if w.Code != http.StatusUnauthorized {
		t.Fatalf("status = %d, want 401", w.Code)
	}
}

func TestDelete_InvalidID(t *testing.T) {
	h := newKnowPostHandler(&stubWriteSvc{}, &stubReadSvc{}, &stubFeedSvc{})
	r := setupKnowPostRouter(h, true)
	w := performJSON(r, "DELETE", "/api/v1/knowposts/abc", nil)

	if w.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400", w.Code)
	}
}

func TestDelete_ServiceError(t *testing.T) {
	writeSvc := &stubWriteSvc{
		deleteFn: func(_ context.Context, _, _ uint64) error {
			return errors.New("db error")
		},
	}
	h := newKnowPostHandler(writeSvc, &stubReadSvc{}, &stubFeedSvc{})
	r := setupKnowPostRouter(h, true)
	w := performJSON(r, "DELETE", "/api/v1/knowposts/123", nil)

	if w.Code != http.StatusInternalServerError {
		t.Fatalf("status = %d, want 500", w.Code)
	}
}

// ============================================================================
// GetDetail tests
// ============================================================================

func TestGetDetail_Success(t *testing.T) {
	readSvc := &stubReadSvc{
		getDetailFn: func(_ context.Context, id uint64, currentUserID *uint64) (*KnowPostDetailResponse, error) {
			if id != 123 {
				t.Errorf("id = %d, want 123", id)
			}
			return &KnowPostDetailResponse{ID: "123", Title: strPtr("Test Title"), AuthorNickname: "Alice"}, nil
		},
	}
	h := newKnowPostHandler(&stubWriteSvc{}, readSvc, &stubFeedSvc{})
	r := setupKnowPostRouter(h, false)
	w := performJSON(r, "GET", "/api/v1/knowposts/123", nil)

	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", w.Code)
	}
	var resp struct {
		Code int                      `json:"code"`
		Data KnowPostDetailResponse `json:"data"`
	}
	if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if resp.Code != 0 {
		t.Errorf("code = %d, want 0", resp.Code)
	}
	if resp.Data.ID != "123" {
		t.Errorf("id = %q, want 123", resp.Data.ID)
	}
}

func TestGetDetail_InvalidID(t *testing.T) {
	h := newKnowPostHandler(&stubWriteSvc{}, &stubReadSvc{}, &stubFeedSvc{})
	r := setupKnowPostRouter(h, false)
	w := performJSON(r, "GET", "/api/v1/knowposts/abc", nil)

	if w.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400", w.Code)
	}
}

func TestHandler_GetDetail_NotFound(t *testing.T) {
	readSvc := &stubReadSvc{
		getDetailFn: func(_ context.Context, _ uint64, _ *uint64) (*KnowPostDetailResponse, error) {
			return nil, errcode.ErrNotFound.WithMsg("post not found")
		},
	}
	h := newKnowPostHandler(&stubWriteSvc{}, readSvc, &stubFeedSvc{})
	r := setupKnowPostRouter(h, false)
	w := performJSON(r, "GET", "/api/v1/knowposts/999", nil)

	if w.Code != http.StatusNotFound {
		t.Fatalf("status = %d, want 404", w.Code)
	}
}

func TestGetDetail_ServiceError(t *testing.T) {
	readSvc := &stubReadSvc{
		getDetailFn: func(_ context.Context, _ uint64, _ *uint64) (*KnowPostDetailResponse, error) {
			return nil, errors.New("db error")
		},
	}
	h := newKnowPostHandler(&stubWriteSvc{}, readSvc, &stubFeedSvc{})
	r := setupKnowPostRouter(h, false)
	w := performJSON(r, "GET", "/api/v1/knowposts/123", nil)

	if w.Code != http.StatusInternalServerError {
		t.Fatalf("status = %d, want 500", w.Code)
	}
}

func TestGetDetail_WithAuth(t *testing.T) {
	readSvc := &stubReadSvc{
		getDetailFn: func(_ context.Context, _ uint64, currentUserID *uint64) (*KnowPostDetailResponse, error) {
			if currentUserID == nil || *currentUserID != 42 {
				t.Errorf("currentUserID = %v, want &42", currentUserID)
			}
			return &KnowPostDetailResponse{ID: "123"}, nil
		},
	}
	h := newKnowPostHandler(&stubWriteSvc{}, readSvc, &stubFeedSvc{})
	r := setupKnowPostRouter(h, true)
	w := performJSON(r, "GET", "/api/v1/knowposts/123", nil)

	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", w.Code)
	}
}

// ============================================================================
// GetPublicFeed tests
// ============================================================================

func TestGetPublicFeed_Success(t *testing.T) {
	feedSvc := &stubFeedSvc{
		getPublicFeedFn: func(_ context.Context, page, size int, currentUserID *uint64) (*FeedPageResponse, error) {
			if page != 1 || size != 20 {
				t.Errorf("unexpected page/size: %d/%d", page, size)
			}
			return &FeedPageResponse{Items: []FeedItemResponse{}, Page: 1, Size: 20, HasMore: false}, nil
		},
	}
	h := newKnowPostHandler(&stubWriteSvc{}, &stubReadSvc{}, feedSvc)
	r := setupKnowPostRouter(h, false)
	w := performJSON(r, "GET", "/api/v1/knowposts/feed/public", nil)

	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", w.Code)
	}
}

func TestGetPublicFeed_WithPagination(t *testing.T) {
	var capturedPage, capturedSize int
	feedSvc := &stubFeedSvc{
		getPublicFeedFn: func(_ context.Context, page, size int, _ *uint64) (*FeedPageResponse, error) {
			capturedPage = page
			capturedSize = size
			return &FeedPageResponse{}, nil
		},
	}
	h := newKnowPostHandler(&stubWriteSvc{}, &stubReadSvc{}, feedSvc)
	r := setupKnowPostRouter(h, false)
	w := performJSON(r, "GET", "/api/v1/knowposts/feed/public?page=3&size=10", nil)

	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", w.Code)
	}
	if capturedPage != 3 {
		t.Errorf("page = %d, want 3", capturedPage)
	}
	if capturedSize != 10 {
		t.Errorf("size = %d, want 10", capturedSize)
	}
}

func TestGetPublicFeed_ServiceError(t *testing.T) {
	feedSvc := &stubFeedSvc{
		getPublicFeedFn: func(_ context.Context, _, _ int, _ *uint64) (*FeedPageResponse, error) {
			return nil, errors.New("feed error")
		},
	}
	h := newKnowPostHandler(&stubWriteSvc{}, &stubReadSvc{}, feedSvc)
	r := setupKnowPostRouter(h, false)
	w := performJSON(r, "GET", "/api/v1/knowposts/feed/public", nil)

	if w.Code != http.StatusInternalServerError {
		t.Fatalf("status = %d, want 500", w.Code)
	}
}

// ============================================================================
// GetMyPublished tests
// ============================================================================

func TestGetMyPublished_Success(t *testing.T) {
	feedSvc := &stubFeedSvc{
		getMineFeedFn: func(_ context.Context, userID uint64, page, size int) (*FeedPageResponse, error) {
			if userID != 42 {
				t.Errorf("userID = %d, want 42", userID)
			}
			return &FeedPageResponse{Items: []FeedItemResponse{}, Page: 1, Size: 20, HasMore: false}, nil
		},
	}
	h := newKnowPostHandler(&stubWriteSvc{}, &stubReadSvc{}, feedSvc)
	r := setupKnowPostRouter(h, true)
	w := performJSON(r, "GET", "/api/v1/knowposts/feed/mine", nil)

	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", w.Code)
	}
}

func TestGetMyPublished_Unauthorized(t *testing.T) {
	h := newKnowPostHandler(&stubWriteSvc{}, &stubReadSvc{}, &stubFeedSvc{})
	r := setupKnowPostRouter(h, false)
	w := performJSON(r, "GET", "/api/v1/knowposts/feed/mine", nil)

	if w.Code != http.StatusUnauthorized {
		t.Fatalf("status = %d, want 401", w.Code)
	}
}

func TestGetMyPublished_ServiceError(t *testing.T) {
	feedSvc := &stubFeedSvc{
		getMineFeedFn: func(_ context.Context, _ uint64, _, _ int) (*FeedPageResponse, error) {
			return nil, errors.New("feed error")
		},
	}
	h := newKnowPostHandler(&stubWriteSvc{}, &stubReadSvc{}, feedSvc)
	r := setupKnowPostRouter(h, true)
	w := performJSON(r, "GET", "/api/v1/knowposts/feed/mine", nil)

	if w.Code != http.StatusInternalServerError {
		t.Fatalf("status = %d, want 500", w.Code)
	}
}

// ============================================================================
// NewKnowPostHandler
// ============================================================================

func TestNewKnowPostHandler(t *testing.T) {
	h := NewKnowPostHandler(nil, nil, nil)
	if h == nil {
		t.Fatal("expected non-nil handler")
	}
}

// ============================================================================
// Benchmarks
// ============================================================================

func BenchmarkFeedHandler(b *testing.B) {
	writeSvc := &stubWriteSvc{}
	readSvc := &stubReadSvc{}
	title := "test"
	feedSvc := &stubFeedSvc{
		getPublicFeedFn: func(ctx context.Context, page, size int, currentUserID *uint64) (*FeedPageResponse, error) {
			return &FeedPageResponse{
				Items: []FeedItemResponse{
					{ID: "1", Title: &title, Description: nil},
				},
				Page:    1,
				Size:    20,
				HasMore: false,
			}, nil
		},
	}
	h := NewKnowPostHandler(writeSvc, readSvc, feedSvc)
	r := setupKnowPostRouter(h, true)

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		w := httptest.NewRecorder()
		req := httptest.NewRequest("GET", "/api/v1/knowposts/feed/public?page=1&size=20", nil)
		r.ServeHTTP(w, req)
	}
}

func BenchmarkDetailHandler(b *testing.B) {
	writeSvc := &stubWriteSvc{}
	title := "test"
	readSvc := &stubReadSvc{
		getDetailFn: func(ctx context.Context, id uint64, currentUserID *uint64) (*KnowPostDetailResponse, error) {
			return &KnowPostDetailResponse{
				ID:    "1",
				Title: &title,
			}, nil
		},
	}
	feedSvc := &stubFeedSvc{}
	h := NewKnowPostHandler(writeSvc, readSvc, feedSvc)
	gin.SetMode(gin.TestMode)
	r := gin.New()
	r.Use(func(c *gin.Context) {
		c.Set("user_id", uint64(42))
	})
	h.RegisterRoutes(r.Group("/api/v1"))

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		w := httptest.NewRecorder()
		req := httptest.NewRequest("GET", "/api/v1/knowposts/1", nil)
		r.ServeHTTP(w, req)
	}
}
