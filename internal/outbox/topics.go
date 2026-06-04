package outbox

const (
	// CanalOutboxTopic 是 Canal 桥接将 outbox 表变更写入的 Kafka 主题。
	//
	// Canal 在 binlog 中捕获到 outbox 表的 INSERT/UPDATE 操作后，
	// 会将变更事件转换为 CanalEnvelope JSON 并发布到此主题。
	// 多个消费者组（关系事件、搜索索引）都会订阅此主题，各自做投影。
	CanalOutboxTopic = "canal-outbox"

	// RelationOutboxConsumerGroup 是关系事件消费者组 ID。
	//
	// 该消费者负责处理 FollowCreated/FollowCanceled 等关系事件，
	// 包括维护 Redis 中的关注/粉丝 ZSet 缓存和用户维度的计数。
	RelationOutboxConsumerGroup = "relation-outbox-consumer"

	// SearchOutboxConsumerGroup 是搜索索引消费者组 ID。
	//
	// 该消费者负责将知文的变更（发布、更新、删除）同步到 Elasticsearch 索引中。
	// 它是 outbox 模式在搜索场景的具体实现：写操作在启库事务内写入 outbox 表，
	// 由 Canal 捕获后异步投递到搜索索引。
	SearchOutboxConsumerGroup = "search-index-consumer"
)
