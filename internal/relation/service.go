package relation

import (
	"github.com/coocood/freecache"
	"github.com/jmoiron/sqlx"
	"github.com/redis/go-redis/v9"
)

// TOKEN_BUCKET_LUA 实现用户级关注频率控制。
//
// KEYS[1] 是限流键；ARGV[1] 是容量；ARGV[2] 是每秒补充的令牌数。
// 返回值：1 表示允许，0 表示拒绝。
const TOKEN_BUCKET_LUA = `
local key = KEYS[1]
local capacity = tonumber(ARGV[1])
local rate = tonumber(ARGV[2])
local now = redis.call('TIME')[1]
local last = redis.call('HGET', key, 'last')
local tokens = redis.call('HGET', key, 'tokens')
if not last then last = now; tokens = capacity end
local elapsed = tonumber(now) - tonumber(last)
local add = elapsed * rate
tokens = math.min(capacity, tonumber(tokens) + add)
if tokens < 1 then
  redis.call('HSET', key, 'last', now)
  redis.call('HSET', key, 'tokens', tokens)
  return 0
end
tokens = tokens - 1
redis.call('HSET', key, 'last', now)
redis.call('HSET', key, 'tokens', tokens)
redis.call('PEXPIRE', key, 60000)
return 1
`

// BigV 阈值：粉丝数 >= 500 的用户会进入 L1 缓存。
const bigVThreshold = 500

// RelationService 实现关注关系的写路径、列表缓存和状态查询。
//
// 为避免单文件持续膨胀，当前按职责拆分为：
//   - service_write.go: 关注/取关/关系状态写路径
//   - service_read.go: offset/cursor 列表读取
//   - service_cache.go: 多级缓存回填与辅助函数
//
// 这样做的目的是把“事务写入、缓存读路径、缓存回填策略”拆开维护，
// 否则关系域会很快回到一个几百行 service 大文件的状态。
type RelationService struct {
	db    *sqlx.DB
	redis *redis.Client
	repo  *RelationRepository
	l1    *freecache.Cache
	idGen IDGenerator
}

// IDGenerator 定义关系域依赖的分布式唯一 ID 生成接口。
type IDGenerator interface {
	NextID() uint64
}

// NewRelationService 创建一个带多级缓存的关系服务实例。
func NewRelationService(db *sqlx.DB, rdb *redis.Client, cacheSize int, idGen IDGenerator) *RelationService {
	return &RelationService{
		db:    db,
		redis: rdb,
		repo:  NewRelationRepository(db),
		l1:    freecache.NewCache(cacheSize),
		idGen: idGen,
	}
}
