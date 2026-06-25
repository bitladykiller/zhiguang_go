package counter

import (
	"encoding/json"
	"fmt"
	"sort"
	"time"

	"github.com/segmentio/kafka-go"
)

// counterBatch 表示单个分区上的待 flush 事件批次。
//
// partition: Kafka 分区号，同一 batch 内所有消息必须在同一分区且 offset 连续。
// startOffset/endOffset: 批内首条/末条消息的 offset（inclusive；endOffset 随 addEvent 递增）。
// entities: dirty member set，flush 失败时用于标记脏数据。
type counterBatch struct {
	partition   int
	openedAt    time.Time
	startOffset int64
	endOffset   int64
	messages    []kafka.Message
	events      []counterBatchEvent
	entities    map[string]struct{}
}

// counterBatchEvent 表示批次中的单条计数事件。
type counterBatchEvent struct {
	offset     int64
	entityType string
	entityID   string
	index      int
	delta      int
}

func newCounterBatch(capacity int) *counterBatch {
	if capacity <= 0 {
		capacity = 1
	}
	return &counterBatch{
		partition: -1,
		messages:  make([]kafka.Message, 0, capacity),
		events:    make([]counterBatchEvent, 0, capacity),
		entities:  make(map[string]struct{}, capacity),
	}
}

func (b *counterBatch) add(msg kafka.Message) error {
	// add 是 addEvent 的便捷封装，供测试使用，生产路径不会调用。
	evt, err := parseCounterEvent(msg.Value)
	if err != nil {
		return fmt.Errorf("counter batch: parse event: %w", err)
	}
	return b.addEvent(msg, evt)
}

func (b *counterBatch) addEvent(msg kafka.Message, evt CounterEvent) error {
	if evt.EntityType == "" || evt.EntityID == "" {
		return fmt.Errorf("counter event missing entity: %+v", evt)
	}
	if evt.Index < 0 || evt.Index >= SchemaLen {
		return fmt.Errorf("counter event index out of range: %d", evt.Index)
	}
	if evt.Delta == 0 {
		return fmt.Errorf("counter event delta is zero")
	}

	if b.size() == 0 {
		b.partition = msg.Partition
		b.openedAt = time.Now()
		b.startOffset = msg.Offset
		b.endOffset = msg.Offset
	} else {
		if msg.Partition != b.partition {
			return fmt.Errorf("counter batch partition mismatch: got=%d want=%d", msg.Partition, b.partition)
		}
		if msg.Offset != b.endOffset+1 {
			return fmt.Errorf("counter batch offset gap: partition=%d got=%d want=%d", msg.Partition, msg.Offset, b.endOffset+1)
		}
		b.endOffset = msg.Offset
	}

	b.messages = append(b.messages, msg)
	b.events = append(b.events, counterBatchEvent{
		offset:     msg.Offset,
		entityType: evt.EntityType,
		entityID:   evt.EntityID,
		index:      evt.Index,
		delta:      evt.Delta,
	})
	b.entities[DirtyMember(evt.EntityType, evt.EntityID)] = struct{}{}
	return nil
}

func (b *counterBatch) size() int {
	if b == nil {
		return 0
	}
	return len(b.messages)
}

func (b *counterBatch) collectDirtyMembers() []string {
	members := make([]string, 0, len(b.entities))
	for member := range b.entities {
		members = append(members, member)
	}
	return members
}

func (b *counterBatch) cntKeys() ([]string, map[string]int) {
	members := b.collectDirtyMembers()
	sort.Strings(members)

	keys := make([]string, 0, len(members))
	indexes := make(map[string]int, len(members))
	for i, member := range members {
		entityType, entityID, err := ParseDirtyMember(member)
		if err != nil {
			continue
		}
		keys = append(keys, SdsKey(entityType, entityID))
		indexes[member] = i
	}
	return keys, indexes
}

func (b *counterBatch) reset() {
	if b == nil {
		return
	}
	b.partition = -1
	b.openedAt = time.Time{}
	b.startOffset = 0
	b.endOffset = 0
	b.messages = b.messages[:0]
	b.events = b.events[:0]
	clear(b.entities)
}

// parseCounterEvent 将 Kafka 消息的 Value (JSON bytes) 反序列化为 CounterEvent。
//
// 参数:
//   - value: []byte，Kafka 消息 Body
//
// 返回值:
//   - CounterEvent: 解析后的计数变更事件
//   - error: JSON 解析失败时返回错误
func parseCounterEvent(value []byte) (CounterEvent, error) {
	var evt CounterEvent
	if err := json.Unmarshal(value, &evt); err != nil {
		return CounterEvent{}, err
	}
	return evt, nil
}
