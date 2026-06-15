package counter

import (
	"encoding/json"
	"fmt"
	"sort"
	"time"

	"github.com/segmentio/kafka-go"
)

type counterBatch struct {
	partition   int
	openedAt    time.Time
	startOffset int64
	endOffset   int64
	messages    []kafka.Message
	events      []counterBatchEvent
	entities    map[string]struct{}
}

type counterBatchEvent struct {
	offset     int64
	entityType string
	entityID   string
	index      int
	delta      int
	epoch      uint64
	usesEpoch  bool
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
		epoch:      evt.Epoch,
		usesEpoch:  metricUsesEpochFence(evt.Metric),
	})
	b.entities[CounterEntityMember(evt.EntityType, evt.EntityID)] = struct{}{}
	return nil
}

func (b *counterBatch) size() int {
	if b == nil {
		return 0
	}
	return len(b.messages)
}

func (b *counterBatch) collectEntityMembers() []string {
	members := make([]string, 0, len(b.entities))
	for member := range b.entities {
		members = append(members, member)
	}
	return members
}

func (b *counterBatch) entityKeys() ([]string, []string, map[string]int) {
	members := b.collectEntityMembers()
	sort.Strings(members)

	keys := make([]string, 0, len(members))
	epochKeys := make([]string, 0, len(members))
	indexes := make(map[string]int, len(members))
	for i, member := range members {
		entityType, entityID, err := ParseCounterEntityMember(member)
		if err != nil {
			continue
		}
		keys = append(keys, SdsKey(entityType, entityID))
		epochKeys = append(epochKeys, ActiveEpochKey(entityType, entityID))
		indexes[member] = i
	}
	return keys, epochKeys, indexes
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

func parseCounterEvent(value []byte) (CounterEvent, error) {
	var evt CounterEvent
	if err := json.Unmarshal(value, &evt); err != nil {
		return CounterEvent{}, err
	}
	return evt, nil
}
