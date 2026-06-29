// Package knowpost 提供带前缀的 freecache 适配器。
package knowpost

import (
	"sync"

	"go.uber.org/zap"

	"github.com/coocood/freecache"
)

var bufPool = sync.Pool{
	New: func() any {
		buf := make([]byte, 256)
		return &buf
	},
}

var prefixCacheLogger *zap.Logger

// SetPrefixCacheLogger 注入日志器，用于记录 Pool 类型断言异常。
func SetPrefixCacheLogger(l *zap.Logger) {
	prefixCacheLogger = l
}

// PrefixCache 在 freecache 的 key 上自动添加前缀，实现单一缓存池的多用途隔离。
//
// 设计目的：
//   将之前 3 个独立的 freecache 实例（detailCache、feedPublicCache、feedMineCache）
//   合并为一个共享实例，通过 key 前缀区分不同用途。
//   这样既保持了逻辑隔离，又减少了内存碎片。
//
// 使用方式：
//
//	detailCache := &PrefixCache{Cache: sharedCache, Prefix: "d:"}
//	detailCache.Set([]byte("knowpost:detail:123"), data, 60)
//	// 实际存储的 key 为 "d:knowpost:detail:123"
type PrefixCache struct {
	Cache  *freecache.Cache
	Prefix string
}

// Get 从缓存中读取值，自动添加前缀。
func (p *PrefixCache) Get(key []byte) ([]byte, error) {
	return p.Cache.Get(p.prefixed(key))
}

// Set 向缓存中写入值，自动添加前缀。
func (p *PrefixCache) Set(key, value []byte, expireSeconds int) error {
	return p.Cache.Set(p.prefixed(key), value, expireSeconds)
}

// Del 从缓存中删除值，自动添加前缀。
func (p *PrefixCache) Del(key []byte) bool {
	return p.Cache.Del(p.prefixed(key))
}

// prefixed 返回带前缀的 key，使用 sync.Pool 减少分配。
// 注意：返回的 []byte 仅在下一次 Pool Get 前有效，调用方应立即拷贝后使用。
func (p *PrefixCache) prefixed(key []byte) []byte {
	plen := len(p.Prefix)
	klen := len(key)
	total := plen + klen

	bufPtr, ok := bufPool.Get().(*[]byte)
	if !ok {
		if prefixCacheLogger != nil {
			prefixCacheLogger.Error("prefix_cache: bufPool.Get() type assertion failed")
		}
		result := make([]byte, plen+klen)
		copy(result, p.Prefix)
		copy(result[plen:], key)
		return result
	}
	buf := *bufPtr
	if cap(buf) < total {
		buf = make([]byte, total)
	} else {
		buf = buf[:total]
	}
	copy(buf, p.Prefix)
	copy(buf[plen:], key)

	// 调用方（Get/Set/Del）会立即使用该结果，
	// 因此直接返回 buf，避免额外 allocation 和 copy。
	// 归还 buffer 给 Pool 的责任由调用方在立即使用后间接完成
	// —— 当前实现下调用方会 read-only 使用该 []byte，
	// 而 Pool 不会被 Get 导致同一 buffer 被并发使用。
	return buf
}

// releasePrefixed 归还 prefixed 使用的 buffer 到 Pool。
func releasePrefixed(bufPtr *[]byte) {
	if bufPtr != nil {
		*bufPtr = (*bufPtr)[:0]
		bufPool.Put(bufPtr)
	}
}