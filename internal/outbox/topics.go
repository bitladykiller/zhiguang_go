package outbox

const (
	// CanalOutboxTopic 是 Canal 桥接将 outbox 表变更写入的 Kafka 主题。
	//
	// Canal 在 binlog 中捕获到 outbox 表的 INSERT/UPDATE 操作后，
	// 会将变更事件转换为 CanalEnvelope JSON 并发布到此主题。
	// 多个消费者组（关系事件、搜索索引）都会订阅此主题，各自做投影。
	CanalOutboxTopic = "canal-outbox"

	// FanoutTopic 是写扩散事件的主题。
	// 知文发布后，fanout 生产者将 FanoutEvent 写入此 topic，
	// FanoutConsumer 消费后写入粉丝的 timeline ZSet。
	FanoutTopic = "fanout"

	// FanoutConsumerGroup 是写扩散消费者组 ID。
	FanoutConsumerGroup = "fanout-group"

	// RelationOutboxConsumerGroup 是关系事件消费者组 ID。
	RelationOutboxConsumerGroup = "relation-outbox-consumer"

	// SearchOutboxConsumerGroup 是搜索索引消费者组 ID。
	SearchOutboxConsumerGroup = "search-index-consumer"
)
