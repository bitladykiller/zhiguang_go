package outbox

const (
	// CanalOutboxTopic 是 Canal 桥接写入的 Kafka 主题。
	CanalOutboxTopic = "canal-outbox"

	// RelationOutboxConsumerGroup 是关系事件消费者组。
	RelationOutboxConsumerGroup = "relation-outbox-consumer"

	// SearchOutboxConsumerGroup 是搜索索引消费者组。
	SearchOutboxConsumerGroup = "search-index-consumer"
)
