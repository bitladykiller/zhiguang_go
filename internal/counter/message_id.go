package counter

// MessageIDGenerator 抽象本地雪花 ID 生成能力，便于复用应用现有生成器。
//
// 当前 message_id 仍然会写入 CounterEvent，便于消息追踪和故障排查；
// 但消费者的正确性已经不再依赖本地短期去重，而是依赖 partition + offset 的共享水位线。
type MessageIDGenerator interface {
	NextID() uint64
}
