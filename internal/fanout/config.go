package fanout

import "time"

// Config 控制混合扩散的行为参数。
type Config struct {
	// CelebrityThreshold 是「大 V」的粉丝数阈值。
	//
	// 粉丝数达到该值的作者不再做写扩散，其帖子改由读者在读取时拉取。
	// 取值权衡：
	//   - 调大 → 更多作者走推，写放大上升，但读路径更省（要拉的作者更少）。
	//   - 调小 → 更多作者走拉，写入更轻，但读路径要合并的发件箱变多。
	// 经验做法是让「绝大多数作者走推、极少数头部作者走拉」，
	// 因为推的成本随该作者粉丝数线性增长，而拉的成本随**读者**关注的大 V 数增长。
	CelebrityThreshold int

	// FanoutBatchSize 是每批推送的粉丝数（一次 pipeline 覆盖多少个收件箱）。
	FanoutBatchSize int

	// FanoutMaxFans 是单次写扩散允许触达的粉丝数上限（兜底保护）。
	//
	// 正常情况下 CelebrityThreshold 会先生效；该上限用于防御
	// 「粉丝计数失真且真实粉丝数远超阈值」这类异常，避免单条消息拖垮消费者。
	// 触顶时会把作者补记为大 V，后续帖子自动走拉路。
	FanoutMaxFans int

	// TimelineMaxItems 是收件箱保留的最大条数，同时也是首页信息流的**深度上限**。
	//
	// 信息流不做无限深翻页是行业惯例：越往后翻价值越低，而代价（内存、归并开销）线性上升。
	TimelineMaxItems int

	// TimelineTTL 是收件箱的过期时间。长期不活跃用户的收件箱会自然回收，
	// 其再次访问时由拉路 + 关注回填重建。
	TimelineTTL time.Duration

	// AuthorBoxMaxItems 是发件箱保留的最大条数。
	//
	// 它决定了「拉」能覆盖的历史深度：读者最多能从一个大 V 处拉到这么多条。
	AuthorBoxMaxItems int

	// AuthorBoxTTL 是发件箱的过期时间。
	AuthorBoxTTL time.Duration

	// FollowBackfillLimit 是关注某人时，从其发件箱回填到自己收件箱的条数。
	//
	// 0 表示不回填。回填让「关注后立刻能看到对方内容」，
	// 且成本有界（最多一次 ZRANGE + 一次 ZADD）。
	FollowBackfillLimit int

	// MaxPullAuthors 是读路径单次最多拉取的大 V 数量。
	//
	// 关注了极多大 V 的用户会让归并成本上升，这里设一个上限保证读延迟可控。
	// 超出部分按关注时间倒序截断。
	MaxPullAuthors int
}

// DefaultConfig 返回一组适用于中小规模社区的默认参数。
func DefaultConfig() Config {
	return Config{
		CelebrityThreshold:  5000,
		FanoutBatchSize:     500,
		FanoutMaxFans:       100000,
		TimelineMaxItems:    1000,
		TimelineTTL:         7 * 24 * time.Hour,
		AuthorBoxMaxItems:   200,
		AuthorBoxTTL:        30 * 24 * time.Hour,
		FollowBackfillLimit: 20,
		MaxPullAuthors:      200,
	}
}

// withDefaults 用默认值补齐未设置（<=0）的字段，避免零值配置导致除零或空转。
func (c Config) withDefaults() Config {
	d := DefaultConfig()
	if c.CelebrityThreshold <= 0 {
		c.CelebrityThreshold = d.CelebrityThreshold
	}
	if c.FanoutBatchSize <= 0 {
		c.FanoutBatchSize = d.FanoutBatchSize
	}
	if c.FanoutMaxFans <= 0 {
		c.FanoutMaxFans = d.FanoutMaxFans
	}
	if c.TimelineMaxItems <= 0 {
		c.TimelineMaxItems = d.TimelineMaxItems
	}
	if c.TimelineTTL <= 0 {
		c.TimelineTTL = d.TimelineTTL
	}
	if c.AuthorBoxMaxItems <= 0 {
		c.AuthorBoxMaxItems = d.AuthorBoxMaxItems
	}
	if c.AuthorBoxTTL <= 0 {
		c.AuthorBoxTTL = d.AuthorBoxTTL
	}
	if c.FollowBackfillLimit < 0 {
		c.FollowBackfillLimit = 0
	}
	if c.MaxPullAuthors <= 0 {
		c.MaxPullAuthors = d.MaxPullAuthors
	}
	return c
}
