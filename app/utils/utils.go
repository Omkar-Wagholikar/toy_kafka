package utils

import (
	"encoding/binary"
	"fmt"
	"net"
)

// parseVarint parses a signed varint from the byte slice and returns the value and bytes consumed
func ParseVarint(bytes []byte) (int64, int, error) {
	var result int64
	var shift uint
	var bytesRead int

	for i, b := range bytes {
		if i >= 10 { // varint can be at most 10 bytes for int64
			return 0, 0, fmt.Errorf("varint too long")
		}

		result |= int64(b&0x7F) << shift
		bytesRead++

		if b&0x80 == 0 {
			// Apply zigzag decoding for signed varints
			return int64(uint64(result)>>1) ^ -(result & 1), bytesRead, nil
		}

		shift += 7
	}

	return 0, 0, fmt.Errorf("incomplete varint")
}

func BytesToInt(bs []byte, start int, end int) int {
	valLen := end - start

	switch valLen {
	case 1:
		return int(bs[start])
	case 2:
		return int(binary.BigEndian.Uint16(bs[start:end]))
	case 4:
		return int(binary.BigEndian.Uint32(bs[start:end]))
	case 8:
		return int(binary.BigEndian.Uint64(bs[start:end]))
	default:
		fmt.Println("CUSTOM LENGTH PASSED FOR BYTE 2 INT CONVERSION")
		val := 0
		for i := 0; i < valLen && i < 8; i++ {
			val |= int(bs[start+i]) << ((valLen - 1 - i) * 8)
		}
		return val
	}
}

func intToBytes(val int, val_byte_len int) []byte {
	bs := make([]byte, val_byte_len)

	switch val_byte_len {
	case 1:
		bs[0] = byte(val)
	case 2:
		binary.BigEndian.PutUint16(bs, uint16(val))
	case 4:
		binary.BigEndian.PutUint32(bs, uint32(val))
	case 8:
		binary.BigEndian.PutUint64(bs, uint64(val))
	default:
		fmt.Println("CUSTOM LENGTH PASSED FOR INT 2 BYTE CONVERSION")
		for i := 0; i < val_byte_len && i < 8; i++ {
			bs[i] = byte(val >> ((val_byte_len - 1 - i) * 8))
		}
	}

	return bs
}

func writeAll(conn net.Conn, data []byte) error {
	total := 0
	for total < len(data) {
		n, err := conn.Write(data[total:])
		if err != nil {
			return err
		}
		total += n
	}
	return nil
}

func encodeLength(l uint16) []byte {
	out := make([]byte, 0)
	if l == 0 {
		out = append(out, 0)
	} else {
		for {
			next := byte(l & 0x3f)
			l = l >> 7
			if l == 0 {
				out = append(out, next)
				break
			} else {
				next = next | 0x80
				out = append(out, next)
			}
		}
	}

	return out
}
