package bootstrap

import (
	"context"
	"testing"

	"github.com/zhiguang/app/internal/cache"
	"github.com/zhiguang/app/pkg/config"
)

func TestNewFreeCacheWithConfig_Default(t *testing.T) {
	cfg := &config.Config{}
	cache := newFreeCacheWithConfig(cfg)
	if cache == nil {
		t.Fatal("expected non-nil cache")
	}
}

func TestNewFreeCacheWithConfig_CustomSize(t *testing.T) {
	cfg := &config.Config{}
	cfg.Cache.L2.PublicCfg.MaxSize = 16
	cfg.Cache.L2.MineCfg.MaxSize = 16
	cache := newFreeCacheWithConfig(cfg)
	if cache == nil {
		t.Fatal("expected non-nil cache")
	}
}

func TestNewFreeCacheWithConfig_ZeroSize(t *testing.T) {
	cfg := &config.Config{}
	cfg.Cache.L2.PublicCfg.MaxSize = 0
	cfg.Cache.L2.MineCfg.MaxSize = 0
	cache := newFreeCacheWithConfig(cfg)
	if cache == nil {
		t.Fatal("expected non-nil cache")
	}
}

func TestHotKeyRunner_Start(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	detector := cache.NewHotKeyDetector(&config.HotKeyConfig{
		BucketSizeSeconds:    6,
		BucketCount:          10,
		FlushIntervalSeconds: 6,
		StatTTLSeconds:       120,
		LevelLow:             5,
		LevelMedium:          20,
		LevelHigh:            50,
		ExtendLowSeconds:     20,
		ExtendMediumSeconds:  60,
		ExtendHighSeconds:    120,
		HotMarkTTLSeconds:    60,
	}, nil)
	r := &hotKeyRunner{d: detector}
	r.Start(ctx)
}

func TestInitializeApp_InvalidConfigPath(t *testing.T) {
	_, err := InitializeApp("nonexistent.yaml")
	if err == nil {
		t.Fatal("expected error for nonexistent config")
	}
}