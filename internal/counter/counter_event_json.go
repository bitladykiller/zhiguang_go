package counter

import (
	"strconv"
	"strings"
)

// MarshalCounterEventJSON 将 CounterEvent 序列化为 JSON，字段与 json tag 一致，避免反射开销。
func MarshalCounterEventJSON(e *CounterEvent) []byte {
	if e == nil {
		return []byte("null")
	}
	var b strings.Builder
	b.Grow(160)
	b.WriteString(`{"message_id":`)
	b.WriteString(strconv.FormatUint(e.MessageID, 10))
	b.WriteString(`,"entity_type":`)
	writeJSONString(&b, e.EntityType)
	b.WriteString(`,"entity_id":`)
	writeJSONString(&b, e.EntityID)
	b.WriteString(`,"metric":`)
	writeJSONString(&b, e.Metric)
	b.WriteString(`,"index":`)
	b.WriteString(strconv.Itoa(e.Index))
	b.WriteString(`,"user_id":`)
	b.WriteString(strconv.FormatUint(e.UserID, 10))
	b.WriteString(`,"delta":`)
	b.WriteString(strconv.Itoa(e.Delta))
	b.WriteByte('}')
	return []byte(b.String())
}

func writeJSONString(b *strings.Builder, s string) {
	b.WriteByte('"')
	for i := 0; i < len(s); i++ {
		c := s[i]
		switch c {
		case '\\':
			b.WriteString(`\\`)
		case '"':
			b.WriteString(`\"`)
		case '\n':
			b.WriteString(`\n`)
		case '\r':
			b.WriteString(`\r`)
		case '\t':
			b.WriteString(`\t`)
		default:
			if c < 0x20 {
				b.WriteString(`\u00`)
				b.WriteByte("0123456789abcdef"[c>>4])
				b.WriteByte("0123456789abcdef"[c&0xf])
			} else {
				b.WriteByte(c)
			}
		}
	}
	b.WriteByte('"')
}