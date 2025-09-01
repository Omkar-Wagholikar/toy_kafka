package main

import (
	"bytes"
	"encoding/binary"

	// "encoding/hex"
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
type request struct {
	message_size        int
	request_api_key     int
	request_api_version int
	correlation_id      int
}

type response struct {
	message_size   int
	correlation_id int
	error_code     int
}

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

		req := deSerializeMessage(buff)

		fmt.Println("message_size: ", req.message_size)
		fmt.Println("request_api_key: ", req.request_api_key)
		fmt.Println("request_api_version: ", req.request_api_version)
		fmt.Println("correlation_id: ", req.correlation_id)

		message_length := 0

		res := response{
			message_length,
			req.correlation_id,
			0,
		}

		if !(req.request_api_version >= 0 && req.request_api_version <= 4) {
			res.error_code = 35
		}

		res_bytes := serializeResponse(res)
		fmt.Println(len(res_bytes))
		conn.Write(res_bytes)
	}
}

func serializeResponse(response response) []byte {
	output := bytes.Buffer{}
	binary.Write(&output, binary.LittleEndian, int32(response.message_size))
	binary.Write(&output, binary.BigEndian, int32(response.correlation_id))
	binary.Write(&output, binary.BigEndian, int16(response.error_code))
	return output.Bytes()
}

func deSerializeMessage(buff []byte) request {
	req := request{
		bytesToInt(buff, 0, 4),
		bytesToInt(buff, 4, 6),
		bytesToInt(buff, 6, 8),
		bytesToInt(buff, 8, 12),
	}
	return req
}

func bytesToInt(bs []byte, start int, end int) int {
	valLen := end - start

	// hexString := hex.EncodeToString(bs[start:end])
	// fmt.Printf("> %s\n", hexString)

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
