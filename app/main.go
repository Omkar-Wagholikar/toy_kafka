package main

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"os"
)

// 00000023001200046f7fc66100096b61666b612d636c69000a6b61666b612d636c6904302e3100
// 00 00 00 23  // message_size:        35
// 00 12        // request_api_key:     18
// 00 04        // request_api_version: 4
// 6f 7f c6 61  // correlation_id:      1870644833

func main() {
	fmt.Println("Logs from your program will appear here!")

	l, err := net.Listen("tcp", "0.0.0.0:9092")
	if err != nil {
		fmt.Println("Failed to bind to port 9092")
		os.Exit(1)
	}

	fmt.Println("Start listening on 9092")

	for {
		// Keep looping and waiting/ accepting connections
		conn, err := l.Accept()
		fmt.Println("Accepted connection")
		if err != nil {
			fmt.Println("Error accepting connection: ", err.Error())
			os.Exit(1)
		}
		// Spawn a go routine/ thread for each connection that is accepted
		go handleConnection(conn)
	}
}

func handleConnection(conn net.Conn) {
	fmt.Println("Handling connection")

	defer conn.Close()

	for {
		received := bytes.Buffer{}
		buff := make([]byte, 1024)

		n, err := conn.Read(buff)
		if err != nil {
			if err == io.EOF {
				break // client closed connection
			}
			fmt.Println("Read error:", err)
			return
		}

		received.Write(buff[:n])

		fmt.Println()
		mapping := deSerializeMessage(buff)

		for key, value := range mapping {
			fmt.Println(key, value)
		}
		message_length := 0

		response := serializeMessage(
			message_length,
			mapping["correlation_id"],
			4,
			4,
		)

		conn.Write(response)
	}
}

func serializeMessage(message_length int, correlation_id int, message_byte_len int, correlation_byte_len int) []byte {
	return append(
		intToBytes(message_length, message_byte_len),
		intToBytes(correlation_id, correlation_byte_len)...,
	)
}

func deSerializeMessage(buff []byte) map[string]int {
	mapping := make(map[string]int)

	mapping["message_size"] = bytesToInt(buff, 0, 4)
	mapping["request_api_key"] = bytesToInt(buff, 4, 6)
	mapping["request_api_version"] = bytesToInt(buff, 6, 8)
	mapping["correlation_id"] = bytesToInt(buff, 8, 12)

	return mapping
}

func bytesToInt(bs []byte, start int, end int) int {
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
			val |= int(bs[i]) << (i * 8)
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
			bs[i] = byte(val >> (i * 8))
		}
	}

	return bs
}
