package cache

import (
	"github.com/coocood/freecache"
	"go.uber.org/zap"
)

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

// prefixed 返回「前缀 + key」拼接后的新切片。
//
// WHY 这里不使用 sync.Pool：
//
//	本函数曾用一个包级 sync.Pool 缓存拼接用的字节切片，但缓冲区**只 Get 从不 Put**
//	（配套的归还函数从未被任何调用方引用）。其净效果是：每次调用先做一次 Pool 查找，
//	未命中后由 Pool.New 分配一个固定 256 字节的切片——比直接按需 make 多付出了
//	Pool 查找与指针包装的开销，却完全没有拿到复用收益，是一次负优化。
//
//	BenchmarkPrefixCache_Get 实测（darwin/arm64，200000x×3）：
//	  旧（Pool）实现：~114 ns/op，288 B/op，3 allocs/op
//	  现（直接分配）：~ 90 ns/op， 56 B/op，2 allocs/op
//
//	要让 Pool 真正生效，缓冲区必须在同一次调用内取用并归还；
//	而本函数的返回值要交给 freecache 使用，生命周期跨出了函数边界，
//	归还时机无法在此安全判定。考虑到 freecache 随后还要做哈希与内存拷贝，
//	这一次小切片分配在整体开销中占比很低，直接分配才是更快也更好懂的选择。
//	如需彻底消除该分配，正确做法是让调用方持有缓冲区并改用
//	`append(buf[:0], ...)` 形式，而非在此处引入无法归还的 Pool。
func (p *PrefixCache) prefixed(key []byte) []byte {
	buf := make([]byte, len(p.Prefix)+len(key))
	n := copy(buf, p.Prefix)
	copy(buf[n:], key)
	return buf
}

// SetOrWarn 写入 L1 并在失败时告警，供业务侧统一使用。
//
// WHY 不允许忽略 Set 的返回值：
//
//	freecache 在「单条 value 超过缓存容量的 1/1024」时会拒绝写入并返回错误。
//	超限的条目会**每次请求都写失败**，表现为该 key 的 L1 永远不命中、
//	流量持续下沉到 L2/L3——而如果错误被丢弃，这一退化在监控上完全不可见。
//	因此这里统一记一条 Warn：不影响请求成功，但让容量配置问题可被发现。
func (p *PrefixCache) SetOrWarn(logger *zap.Logger, key, value []byte, expireSeconds int) {
	if err := p.Set(key, value, expireSeconds); err != nil && logger != nil {
		logger.Warn("l1 cache set failed",
			zap.String("prefix", p.Prefix),
			zap.ByteString("key", key),
			zap.Int("valueBytes", len(value)),
			zap.Error(err),
		)
	}
}
