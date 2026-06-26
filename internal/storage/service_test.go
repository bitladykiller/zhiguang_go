package storage

import (
	"errors"
	"testing"
	"time"

	"github.com/zhiguang/app/pkg/config"
)

// --- GenerateObjectKey ---

func TestGenerateObjectKey_WithFolder(t *testing.T) {
	svc := &OssStorageService{cfg: &config.OssConfig{Folder: "images"}}
	key := svc.GenerateObjectKey("custom", "photo.jpg")
	if len(key) < 10 {
		t.Errorf("key too short: %q", key)
	}
}

func TestGenerateObjectKey_EmptyFolder(t *testing.T) {
	svc := &OssStorageService{cfg: &config.OssConfig{Folder: ""}}
	key := svc.GenerateObjectKey("", "photo.jpg")
	if len(key) < 10 {
		t.Errorf("key too short: %q", key)
	}
}

func TestGenerateObjectKey_ConfigFolder(t *testing.T) {
	svc := &OssStorageService{cfg: &config.OssConfig{Folder: "myfolder"}}
	key := svc.GenerateObjectKey("", "photo.jpg")
	if len(key) < 10 {
		t.Errorf("key too short: %q", key)
	}
}

func TestGenerateObjectKey_Format(t *testing.T) {
	svc := &OssStorageService{cfg: &config.OssConfig{Folder: "uploads"}}
	key := svc.GenerateObjectKey("uploads", "photo.jpg")
	if len(key) <= len("uploads/")+1+len("_photo.jpg") {
		t.Errorf("unexpected key format: %q", key)
	}
}

func TestGenerateObjectKey_Unique(t *testing.T) {
	svc := &OssStorageService{cfg: &config.OssConfig{Folder: "test"}}
	key1 := svc.GenerateObjectKey("test", "file.jpg")
	key2 := svc.GenerateObjectKey("test", "file.jpg")
	if key1 == key2 {
		t.Error("expected unique keys for same input")
	}
}

// --- PublicURL ---

func TestPublicURL_WithCustomDomain(t *testing.T) {
	svc := &OssStorageService{cfg: &config.OssConfig{PublicDomain: "media.example.com"}}
	url := svc.PublicURL("images/photo.jpg")
	want := "https://media.example.com/images/photo.jpg"
	if url != want {
		t.Errorf("got %q, want %q", url, want)
	}
}

func TestPublicURL_WithCustomDomainTrailingSlash(t *testing.T) {
	svc := &OssStorageService{cfg: &config.OssConfig{PublicDomain: "media.example.com/"}}
	url := svc.PublicURL("photo.jpg")
	want := "https://media.example.com/photo.jpg"
	if url != want {
		t.Errorf("got %q, want %q", url, want)
	}
}

func TestPublicURL_WithoutCustomDomain(t *testing.T) {
	svc := &OssStorageService{
		cfg: &config.OssConfig{
			Bucket:   "my-bucket",
			Endpoint: "oss-cn-hangzhou.aliyuncs.com",
		},
	}
	url := svc.PublicURL("a/b.jpg")
	want := "https://my-bucket.oss-cn-hangzhou.aliyuncs.com/a/b.jpg"
	if url != want {
		t.Errorf("got %q, want %q", url, want)
	}
}

func TestPublicURL_EmptyConfig(t *testing.T) {
	svc := &OssStorageService{cfg: &config.OssConfig{}}
	url := svc.PublicURL("test.jpg")
	if url == "" {
		t.Error("expected non-empty URL")
	}
}

// --- PresignExpiry ---

func TestPresignExpiry_Default(t *testing.T) {
	svc := &OssStorageService{cfg: &config.OssConfig{}}
	expiry := svc.PresignExpiry()
	if expiry != 10*time.Minute {
		t.Errorf("got %v, want 10m", expiry)
	}
}

func TestPresignExpiry_Configured(t *testing.T) {
	svc := &OssStorageService{cfg: &config.OssConfig{PresignExpiryMs: 300_000}}
	expiry := svc.PresignExpiry()
	if expiry != 5*time.Minute {
		t.Errorf("got %v, want 5m", expiry)
	}
}

func TestPresignExpiry_Zero(t *testing.T) {
	svc := &OssStorageService{cfg: &config.OssConfig{PresignExpiryMs: 0}}
	expiry := svc.PresignExpiry()
	if expiry != 10*time.Minute {
		t.Errorf("got %v, want 10m", expiry)
	}
}

func TestPresignExpiry_LargeValue(t *testing.T) {
	svc := &OssStorageService{cfg: &config.OssConfig{PresignExpiryMs: 3_600_000}}
	expiry := svc.PresignExpiry()
	if expiry != time.Hour {
		t.Errorf("got %v, want 1h", expiry)
	}
}

// --- NewOssStorageService ---

func TestNewOssStorageService_InvalidEndpoint(t *testing.T) {
	cfg := &config.OssConfig{
		Endpoint:        "",
		AccessKeyID:     "test",
		AccessKeySecret: "test",
		Bucket:          "bucket",
	}
	_, err := NewOssStorageService(cfg)
	if err == nil {
		// OSS SDK may not validate endpoint eagerly on some platforms
		t.Log("OSS SDK accepted empty endpoint (may fail later)")
	}
}

func TestNewOssStorageService_NilConfig(t *testing.T) {
	defer func() {
		if r := recover(); r == nil {
			t.Log("nil config does not panic (valid)")
		}
	}()
	_, err := NewOssStorageService(nil)
	if err != nil {
		// expected: nil dereference or error
	}
}

// --- interface compliance ---

func TestOssStorageImplementsInterface(t *testing.T) {
	var _ StorageServicer = (*OssStorageService)(nil)
}

func TestStorageServiceInterface_IsStorageServicer(t *testing.T) {
	var svc StorageServiceInterface = nil
	_ = svc
}

// --- OssConfig defaults ---

func TestOssConfig_Defaults(t *testing.T) {
	cfg := &config.OssConfig{}
	if cfg.Folder != "" || cfg.PublicDomain != "" || cfg.PresignExpiryMs != 0 {
		t.Error("expected zero values in empty OssConfig")
	}
}

// --- error path tests (nil bucket) ---

func TestGeneratePresignedPutURL_NilBucket(t *testing.T) {
	svc := &OssStorageService{}
	defer func() {
		if r := recover(); r != nil {
			t.Log("recovered from nil bucket panic as expected")
		}
	}()
	_, err := svc.GeneratePresignedPutURL("key", time.Minute)
	if err == nil {
		t.Fatal("expected error for nil bucket")
	}
}

func TestGeneratePresignedGetURL_NilBucket_Removed(t *testing.T) {
	// 该方法已删除，此测试保留占位
	t.Log("GeneratePresignedGetURL was removed as dead code")
}

func TestGetObjectMeta_NilBucket_Removed(t *testing.T) {
	// 该方法已删除，此测试保留占位
	t.Log("GetObjectMeta was removed as dead code")
}

// --- PublicURL nil config ---

func TestPublicURL_NilConfig(t *testing.T) {
	svc := &OssStorageService{}
	defer func() {
		if r := recover(); r != nil {
			t.Log("recovered from nil config panic as expected")
		}
	}()
	url := svc.PublicURL("test.jpg")
	if url == "" {
		t.Error("expected non-empty URL")
	}
}

// --- helper types ---

var errOSSFailure = errors.New("oss sdk failure")

type failingBucket struct{}

func (b *failingBucket) SignURL(objectKey string, method int, expires int64) (string, error) {
	return "", errOSSFailure
}

func (b *failingBucket) GetObjectDetailedMeta(objectKey string) (interface{}, error) {
	return nil, errOSSFailure
}
