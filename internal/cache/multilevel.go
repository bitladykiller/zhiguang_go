package cache

import (
	"context"
	"sync"
	"time"

	"github.com/coocood/freecache"
	"github.com/redis/go-redis/v9"
)

// MultiLevelCache 实现两级缓存（L1：freecache，L2：Redis），
// 并通过 singleflight 防止 L2 未命中后的缓存击穿。
//
// 缓存读取路径：
//
//	L1（freecache） -> 进程内极速缓存，约 50ns
//	L2（Redis） -> 分布式缓存，约 1ms
//	DB -> 权威数据源，只会在 singleflight 锁内回源
//
// WHY：当热门键同时在 L1 和 L2 失效时，大量并发请求可能会同时打到数据库。
// singleflight 能保证同一时刻只有一个 goroutine 真正回源，其余请求等待结果即可。
//
// 使用方式：
//
//	cache := NewMultiLevelCache(10*1024*1024, 60, redisClient)
//	val, err := cache.Get(ctx, "mykey", func() (string, error) {
//	    return loadFromDB()
//	})
type MultiLevelCache struct {
	l1      *freecache.Cache
	l2      *redis.Client
	ttl     time.Duration
	flights sync.Map // key → *call (singleflight 锁)
}

// call 表示一次正在进行中的缓存加载操作。
type call struct {
	wg  sync.WaitGroup
	val string
	err error
}

// NewMultiLevelCache 创建一个两级缓存实例。
//
// 功能：
//   初始化 L1（freecache）和 L2（Redis），设置统一的应用层 TTL。
//
// 参数：
//   - size:       freecache 容量（字节），例如 10MB = 10 * 1024 * 1024
//   - ttlSeconds: 缓存写入时的默认过期时间（秒）
//   - redisClient: 已配置的 Redis 客户端实例
//
// 返回值：
//   - *MultiLevelCache: 两级缓存实例
//
// 函数调用说明：
//   - freecache.NewCache(size):
//     freecache 是一个 Go 语言实现的进程内缓存库。
//     NewCache 创建指定大小的缓存实例。超出容量时会淘汰最旧的条目。
//     freecache 的 GC 友好：它内部使用环形缓冲区存储数据，不会产生 GC 压力。
//
// 设计决策：
//   使用 freecache 而非 go-cache 或 bigcache 是因为：
//   - freecache 对 GC 几乎零压力（零 GC 扫描）
//   - 支持过期时间（TTL）设置
//   - 实现简单，功能足以满足两级缓存的需求
func NewMultiLevelCache(size int, ttlSeconds int, redisClient *redis.Client) *MultiLevelCache {
	return &MultiLevelCache{
		l1:  freecache.NewCache(size),
		l2:  redisClient,
		ttl: time.Duration(ttlSeconds) * time.Second,
	}
}

// Get 根据键读取缓存值；未命中时调用 loader 回源并回填缓存。
//
// 功能（三级读取路径）：
//   Level 1: freecache 查询（进程内，~50ns）。
//     - 命中：返回 string(val)，不往下继续。
//     - 未命中：继续查 L2。
//   Level 2: Redis 查询（网络 IO，~1ms）。
//     - 命中：回填 L1（freecache），返回 val。
//     - 未命中：进入回源流程。
//   Level 3: singleflight 加锁回源。
//     - 同 key 同一时刻只有一个 goroutine 执行 loader。
//     - loader 执行完后回填 L1 + L2，返回结果。
//
// 参数：
//   - ctx:    上下文（传递给 L2 Redis 操作和 loader）
//   - key:    缓存键
//   - loader: 回源加载函数，L1 和 L2 均未命中时调用
//
// 返回值：
//   - string: 缓存值或 loader 加载的值
//   - error:  如果 L1/L2 均未命中且 loader 执行失败时返回
//
// 函数调用说明：
//   - c.l1.Get([]byte(key)):
//     freecache 的 Get 方法，接收 []byte 键，返回 ([]byte, error)。
//     未命中返回非 nil error（freecache.ErrNotFound）。
//   - c.l1.Set([]byte(key), []byte(val), ttl):
//     freecache 的 Set 方法，接收键、值（均为 []byte）和 TTL（秒）。
//     TTL <= 0 表示永不过期。
//
// 设计决策：
//   缓存路径按速度降序排列（L1 → L2 → DB），
//   确保最快路径优先命中。L2 命中后回填 L1，
//   这样后续同 key 的请求直接从 L1 获得服务。
func (c *MultiLevelCache) Get(ctx context.Context, key string, loader func() (string, error)) (string, error) {
	// L1：freecache（最快路径）
	if val, err := c.l1.Get([]byte(key)); err == nil {
		return string(val), nil
	}

	// L2：Redis
	val, err := c.l2.Get(ctx, key).Result()
	if err == nil {
		// 回填 L1
		c.l1.Set([]byte(key), []byte(val), int(c.ttl.Seconds()))
		return val, nil
	}

	// L2 未命中 -> 进入 singleflight -> 执行 loader
	return c.singleflightLoad(ctx, key, loader)
}

// Set 同时写入 L1（freecache）和 L2（Redis）。
//
// 功能：
//   双写策略：一次写入，两级缓存同时更新。
//   - L1 写入：freecache.Set()，TTL 以秒为单位。
//   - L2 写入：redis.Set()，TTL 以 time.Duration 为单位。
//
// 参数：
//   - key:   缓存键
//   - value: 缓存值
//   - ttl:   过期时间（time.Duration）
//
// 返回值：
//   - error: Redis 写入失败时返回（L1 写入 error 被忽略）
//
// 设计决策：
//   - L1 写入错误被忽略：freecache 在容量满时可能写入失败，
//     这不应该影响主流程（L2 已写入成功，下次读 L1 不命中会查 L2）。
//   - 显式指定 ttl：与 Get 中回填时使用默认 ttl 不同，
//     Set 调用方可以自定义过期时间。
func (c *MultiLevelCache) Set(ctx context.Context, key, value string, ttl time.Duration) error {
	// 写 L1
	c.l1.Set([]byte(key), []byte(value), int(ttl.Seconds()))
	// 写 L2
	return c.l2.Set(ctx, key, value, ttl).Err()
}

// Del 同时删除 L1 和 L2 中的同名缓存键。
//
// 功能：
//   双删策略：同时删除两级缓存中指定 key 的数据。
//   - L1 删除：freecache.Del()，不会返回错误。
//   - L2 删除：redis.Del()，通过 ctx 支持超时和取消。
//
// 参数：
//   - key: 待删除的缓存键
//
// 设计决策：
//   删除操作不返回 error，因为：
//   - L1 freecache 的 Del 不会失败（内部 map 删除）。
//   - L2 Redis 的 Del 即使失败，一致性也可以通过过期时间来保证。
//   如果调用方需要确保 L2 删除成功，应直接使用 GetL2().Del()。
func (c *MultiLevelCache) Del(ctx context.Context, key string) {
	c.l1.Del([]byte(key))
	c.l2.Del(ctx, key)
}

// GetL1 返回底层 L1 freecache 实例，供直接访问。
func (c *MultiLevelCache) GetL1() *freecache.Cache {
	return c.l1
}

// GetL2 返回底层 L2 Redis 客户端，供直接访问。
func (c *MultiLevelCache) GetL2() *redis.Client {
	return c.l2
}

// singleflightLoad 通过 sync.WaitGroup 实现缓存回源的 singleflight 机制。
//
// 功能：
//   保证同一个 key 在同一时刻只有一个 goroutine 会执行 loader 回源加载。
//   并发请求同一 key 的其他 goroutine 会等待第一个完成并复用其结果。
//
// 实现细节：
//   Step 1: 使用 c.flights 映射（sync.Map）检查是否已有同 key 正在加载。
//     - 如果已有（loaded == true），调用 call.wg.Wait() 等待结果返回。
//     - 如果没有，创建新的 *call 并存入 flights 映射。
//   Step 2: 作为"负责人"的 goroutine 执行 wg.Add(1)，调用 loader 回源。
//   Step 3: loader 完成后：
//     - 成功：回填 L2（Redis）和 L1（freecache），将 val 写入 call.val。
//     - 失败：将 err 写入 call.err。
//   Step 4: 从 flights 映射中删除该 key，调用 wg.Done() 唤醒等待者。
//
// 参数：
//   - ctx:    上下文
//   - key:    缓存键
//   - loader: 回源加载函数
//
// 返回值：
//   - string: loader 返回的值（或等待到的已有结果）
//   - error:  loader 执行失败的错误
//
// 函数调用说明：
//   - c.flights.LoadOrStore(key, &call{}):
//     sync.Map 的原子操作。如果 key 已存在，返回 (existing, true)；
//     否则存储新的 call 并返回 (new, false)。
//   - call.wg.Wait() / call.wg.Add(1) / call.wg.Done():
//     sync.WaitGroup 的经典用法。负责人 wg.Add(1)，等待者 wg.Wait()，
//     完成后 wg.Done() 释放所有等待者。
//
// 边界情况：
//   - loader 返回 error：call.err 会保存并传播给所有等待者。
//   - 多个并发请求同一 key：只有第一个进入的 goroutine 会执行 loader，
//     其余在 wg.Wait() 处等待，完成后全部获得相同结果。
//   - loader 执行期间又有新的同 key 请求到达：检查 flights 时发现已存在，
//     直接等待结果，不会触发第二次 loader 调用。
func (c *MultiLevelCache) singleflightLoad(ctx context.Context, key string, loader func() (string, error)) (string, error) {
	// 检查该键是否已经有加载中的请求
	callIface, loaded := c.flights.LoadOrStore(key, &call{})
	call := callIface.(*call)

	if loaded {
		// 已有其他 goroutine 正在加载，直接等待其结果
		call.wg.Wait()
		return call.val, call.err
	}

	// 当前 goroutine 负责实际加载
	call.wg.Add(1)
	defer func() {
		c.flights.Delete(key)
		call.wg.Done()
	}()

	// 执行回源加载
	val, err := loader()
	if err != nil {
		call.err = err
		return "", err
	}

	// 回填缓存
	call.val = val
	c.l2.Set(ctx, key, val, c.ttl)
	c.l1.Set([]byte(key), []byte(val), int(c.ttl.Seconds()))

	return val, nil
}
