package counter

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/gin-gonic/gin"
)

func TestBindToggleRequest(t *testing.T) {
	gin.SetMode(gin.TestMode)

	t.Run("valid", func(t *testing.T) {
		recorder := httptest.NewRecorder()
		ctx, _ := gin.CreateTestContext(recorder)
		ctx.Request = httptest.NewRequest(http.MethodPost, "/", strings.NewReader(`{"entity_type":"post","entity_id":"1"}`))
		ctx.Request.Header.Set("Content-Type", "application/json")

		req, ok := bindToggleRequest(ctx)
		if !ok {
			t.Fatal("bindToggleRequest(valid) should succeed")
		}
		if req.EntityType != "post" || req.EntityID != "1" {
			t.Fatalf("bindToggleRequest(valid) = %+v", req)
		}
	})

	t.Run("invalid", func(t *testing.T) {
		recorder := httptest.NewRecorder()
		ctx, _ := gin.CreateTestContext(recorder)
		ctx.Request = httptest.NewRequest(http.MethodPost, "/", strings.NewReader(`{}`))
		ctx.Request.Header.Set("Content-Type", "application/json")

		if _, ok := bindToggleRequest(ctx); ok {
			t.Fatal("bindToggleRequest(invalid) should fail")
		}
		if recorder.Code != http.StatusBadRequest {
			t.Fatalf("status = %d, want %d", recorder.Code, http.StatusBadRequest)
		}
	})
}

func TestEntityQuery(t *testing.T) {
	gin.SetMode(gin.TestMode)

	t.Run("valid", func(t *testing.T) {
		recorder := httptest.NewRecorder()
		ctx, _ := gin.CreateTestContext(recorder)
		ctx.Request = httptest.NewRequest(http.MethodGet, "/?entity_type=post&entity_id=1", nil)

		entityType, entityID, ok := entityQuery(ctx)
		if !ok {
			t.Fatal("entityQuery(valid) should succeed")
		}
		if entityType != "post" || entityID != "1" {
			t.Fatalf("entityQuery(valid) = (%q, %q)", entityType, entityID)
		}
	})

	t.Run("missing", func(t *testing.T) {
		recorder := httptest.NewRecorder()
		ctx, _ := gin.CreateTestContext(recorder)
		ctx.Request = httptest.NewRequest(http.MethodGet, "/?entity_type=post", nil)

		if _, _, ok := entityQuery(ctx); ok {
			t.Fatal("entityQuery(missing) should fail")
		}
		if recorder.Code != http.StatusBadRequest {
			t.Fatalf("status = %d, want %d", recorder.Code, http.StatusBadRequest)
		}
	})
}

func TestMetricsQuery(t *testing.T) {
	gin.SetMode(gin.TestMode)

	recorder := httptest.NewRecorder()
	ctx, _ := gin.CreateTestContext(recorder)
	ctx.Request = httptest.NewRequest(http.MethodGet, "/?metrics=like,fav,view", nil)

	metrics := metricsQuery(ctx)
	if len(metrics) != 3 {
		t.Fatalf("len(metrics) = %d, want 3", len(metrics))
	}
	if metrics[0] != "like" || metrics[1] != "fav" || metrics[2] != "view" {
		t.Fatalf("metrics = %#v", metrics)
	}
}
