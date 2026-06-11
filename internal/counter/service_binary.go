package counter

import "encoding/binary"

func readInt32BE(b []byte, offset int) int32 {
	return int32(binary.BigEndian.Uint32(b[offset:]))
}

func writeInt32BE(b []byte, offset int, val int32) {
	binary.BigEndian.PutUint32(b[offset:], uint32(val))
}
