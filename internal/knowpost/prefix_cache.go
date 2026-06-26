// Package knowpost 提供带前缀的 freecache 适配器。
package knowpost

import "github.com/coocood/freecache"

// PrefixCache 在 freecache 的 key 上自动添加前缀，实现单一缓存池的多用途隔离。
//
// 设计目的：
//
//	将之前 3 个独立的 freecache 实例（detailCache、feedPublicCache、feedMineCache）
//	合并为一个共享实例，通过 key 前缀区分不同用途。
//	这样既保持了逻辑隔离，又减少了内存碎片。
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

// prefixed 返回带前缀的 key。
func (p *PrefixCache) prefixed(key []byte) []byte {
	prefixed := make([]byte, len(p.Prefix)+len(key))
	copy(prefixed, p.Prefix)
	copy(prefixed[len(p.Prefix):], key)
	return prefixed
}
