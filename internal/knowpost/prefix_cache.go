package knowpost

import "github.com/zhiguang/app/internal/cache"

// PrefixCache 的实现已上收至 internal/cache（通用缓存工具的统一归属，
// 与 HotKeyDetector、RedisBloom、Tiered 同层）。此别名维持既有引用，逐步移除。
type PrefixCache = cache.PrefixCache
