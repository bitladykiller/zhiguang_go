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

// NewMultiLevelCache 创建一个新的两级缓存实例。
// size 表示 freecache 的容量，单位为字节，例如 10MB = 10 * 1024 * 1024。
func NewMultiLevelCache(size int, ttlSeconds int, redisClient *redis.Client) *MultiLevelCache {
	return &MultiLevelCache{
		l1:  freecache.NewCache(size),
		l2:  redisClient,
		ttl: time.Duration(ttlSeconds) * time.Second,
	}
}

// Get 根据键读取缓存值；未命中时调用 loader 回源并回填缓存。
// loader 会在 singleflight 锁内执行，同一键同一时刻只会有一次真实加载。
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

// Set 同时写入 L1 和 L2。
func (c *MultiLevelCache) Set(ctx context.Context, key, value string, ttl time.Duration) error {
	// 写 L1
	c.l1.Set([]byte(key), []byte(value), int(ttl.Seconds()))
	// 写 L2
	return c.l2.Set(ctx, key, value, ttl).Err()
}

// Del 同时删除 L1 和 L2 中的同名缓存键。
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

// singleflightLoad 保证同一个键只有一个 goroutine 会真正执行 loader。
// 其他等待同一个键的 goroutine 会复用它的结果。
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
