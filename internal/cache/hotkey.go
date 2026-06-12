// Package cache 提供热点键识别（HotKeyDetector）能力。
package cache

import (
	"sync"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/zhiguang/app/pkg/config"
)

const hotwinKeyPrefix = "hotwin:"
const hotkeyActivePrefix = "hotkey:active:"

// HotKeyLevel 表示键的热度等级。
type HotKeyLevel int

const (
	LevelCold   HotKeyLevel = 0
	LevelLow    HotKeyLevel = 1
	LevelMedium HotKeyLevel = 2
	LevelHigh   HotKeyLevel = 3
)

// HotKeyDetector 使用本地 map + Redis Hash 检测跨实例热点键。
//
// 当前按职责拆分为：
//   - hotkey.go: 结构体、构造、采样入口
//   - hotkey_flush.go: 本地缓冲 flush 到 Redis 的逻辑
//   - hotkey_level.go: 热度计算、TTL 推导、字符串表示
//
// 这样做的目的是把“采样”“跨实例聚合”“热度决策”拆开维护，
// 避免基础组件继续堆在一个大文件里。
type HotKeyDetector struct {
	config *config.HotKeyConfig
	redis  *redis.Client

	mu  sync.Mutex
	buf map[string]map[int64]int64

	levelMu sync.RWMutex
	levels  map[string]HotKeyLevel

	bucketSize    time.Duration
	flushInterval time.Duration
	statTTL       time.Duration
	markTTL       time.Duration

	startOnce sync.Once
}

// NewHotKeyDetector 根据配置和 Redis 客户端创建跨实例热点键探测器。
func NewHotKeyDetector(cfg *config.HotKeyConfig, redisClient *redis.Client) *HotKeyDetector {
	return &HotKeyDetector{
		config:        cfg,
		redis:         redisClient,
		buf:           make(map[string]map[int64]int64),
		levels:        make(map[string]HotKeyLevel),
		bucketSize:    time.Duration(cfg.BucketSizeSeconds) * time.Second,
		flushInterval: time.Duration(cfg.FlushIntervalSeconds) * time.Second,
		statTTL:       time.Duration(cfg.StatTTLSeconds) * time.Second,
		markTTL:       time.Duration(cfg.HotMarkTTLSeconds) * time.Second,
	}
}

// Record 为指定键在当前时间窗口内增加一次命中计数。
func (d *HotKeyDetector) Record(key string) {
	d.startOnce.Do(func() {
		go d.flushLoop()
	})

	bucket := d.currentBucket()

	d.mu.Lock()
	defer d.mu.Unlock()

	if d.buf[key] == nil {
		d.buf[key] = make(map[int64]int64)
	}
	d.buf[key][bucket]++
}

// currentBucket 返回当前时间对应的桶编号。
func (d *HotKeyDetector) currentBucket() int64 {
	return time.Now().Unix() / int64(d.bucketSize.Seconds())
}
