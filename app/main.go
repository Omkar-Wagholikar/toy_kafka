package main

import (
	"bytes"
	"encoding/hex"
	"fmt"
	"io"
	"net"
	"os"
)

// echo -n "00000031004b00000bcefe56000c6b61666b612d746573746572000212756e6b6e6f776e2d746f7069632d71757a0000000001ff00"  | xxd -r -p | nc localhost 9092 | hexdump -C

//  My response: 000000380bcefe56000000000002000312756e6b6e6f776e2d746f7069632d71757a0000000000000000000000000000000000000001000000000000

// (Ideally) correct response: 0000003d0bcefe56000000000002000312756e6b6e6f776e2d746f7069632d71757a00000000000000000000000000000000000001000000000001000000000000

// echo -n "00000031004b00002a5d9747000c6b61666b612d746573746572000212756e6b6e6f776e2d746f7069632d70617a0000000001ff00" | xxd -r -p | nc localhost 9092 | hexdump -C
// echo -n "00000020004b00000000000700096b61666b612d636c69000204666f6f0000000064ff00" | xxd -r -p | nc localhost 9092 | hexdump -C

func main() {
	fmt.Println("Logs from your program will appear here!")

	l, err := net.Listen("tcp", "0.0.0.0:9092")
	if err != nil {
		fmt.Println("Failed to bind to port 9092")
		os.Exit(1)
	}

	fmt.Println("Start listening on 9092")

	for {
		conn, err := l.Accept()
		if err != nil {
			fmt.Println("Error accepting connection: ", err.Error())
			os.Exit(1)
		}
		go handleConnection(conn)
	}
}

func handleConnection(conn net.Conn) {
	defer conn.Close()

	for {
		buff := make([]byte, 1024)

		n, err := conn.Read(buff)
		if err != nil {
			if err == io.EOF {
				break
			}
			fmt.Println("Read error:", err)
			return
		}

		minimalReq, err := deserializeMinimalRequest(buff[:n])
		if err != nil {
			fmt.Println("Error deserializing minimal request:", err)
			continue
		}

		fmt.Printf("Parsed request: APIKey=%d, Version=%d, CorrelationID=%d\n",
			minimalReq.RequestAPIKey, minimalReq.RequestAPIVersion, minimalReq.CorrelationID)

		switch minimalReq.RequestAPIKey {
		case ApiVersionAPIKEY:
			fmt.Println("ApiVersionAPIKEY")
			response := handleAPIRequest(minimalReq)
			responseBytes := serializeResponse(response)
			conn.Write(responseBytes)

		case DescribeTopicPartitionsAPIKEY:
			fmt.Println("DescribeTopicPartitionsAPIKEY")
			response := handleDescribeRequest(buff, n)
			printDescribeTopicResponse(response)
			responseBytes := serializeDescribeTopicPartitionsResponse(response)
			data := hex.EncodeToString(responseBytes)
			fmt.Println(">", data)
			n, err = conn.Write(responseBytes)
			if err != nil {
				fmt.Println("Error in writing: ", err.Error())
				os.Exit(1)
			}
			if n != len(responseBytes) {
				fmt.Println("Improper write")
			}
			fmt.Println("PRINT COMPLETE")

		default:
			fmt.Println("UNHANDLED CASE")
			os.Exit(1)
		}
	}
}

func handleDescribeRequest(buff []byte, n int) *DescribeTopicPartitionsResponse {
	req, err := deserializeDescribeTopicPartitionsRequest(buff[:n])
	if err != nil {
		fmt.Println("Error deserializing request:", err)
		os.Exit(1)
	}

	fmt.Printf("Parsed DescribeTopicPartitions request: CorrelationID=%d, Topics=%d\n",
		req.CorrelationID, len(req.Topics))

	// Create response with unknown topic error
	response := createUnknownTopicResponse(req)

	return response
}

func handleAPIRequest(minimalReq *MinimalRequest) Response {

	response := Response{
		MessageSize:   33, // Will be calculated properly in serialization
		CorrelationID: minimalReq.CorrelationID,
		ErrorCode:     0,
		ArrayLength:   4, // 3 API versions + 1
		APIVersions: []APIVersion{
			{APIKey: 1, MinVersion: 0, MaxVersion: 17, TagBuffer: 0},
			{APIKey: 18, MinVersion: 0, MaxVersion: 4, TagBuffer: 0},
			{APIKey: 75, MinVersion: 0, MaxVersion: 0, TagBuffer: 0},
		},
		ThrottleTime: 0,
		TagBuffer:    0,
	}

	// Create API func bytesToIntVersions response
	if !(minimalReq.RequestAPIVersion >= 0 && minimalReq.RequestAPIVersion <= 4) {
		response.ErrorCode = 35
	}

	return response
}

// Deserialize minimal request (always works for basic Kafka requests)
func deserializeMinimalRequest(buff []byte) (*MinimalRequest, error) {
	if len(buff) < 12 {
		return nil, fmt.Errorf("buffer too short for minimal request")
	}

	req := &MinimalRequest{
		MessageSize:       bytesToInt(buff, 0, 4),
		RequestAPIKey:     bytesToInt(buff, 4, 6),
		RequestAPIVersion: bytesToInt(buff, 6, 8),
		CorrelationID:     bytesToInt(buff, 8, 12),
	}

	return req, nil
}

// Deserialize full request (handles variable length fields)
func deserializeFullRequest(buff []byte) (*FullRequest, error) {
	if len(buff) < 12 {
		return nil, fmt.Errorf("buffer too short")
	}

	req := &FullRequest{}
	req.MessageSize = bytesToInt(buff, 0, 4)
	req.RequestAPIKey = bytesToInt(buff, 4, 6)
	req.RequestAPIVersion = bytesToInt(buff, 6, 8)
	req.CorrelationID = bytesToInt(buff, 8, 12)

	offset := 12

	// Check if we have more data for extended fields
	if len(buff) <= offset {
		return req, nil
	}

	// Parse API Client ID Length
	if len(buff) >= offset+2 {
		req.APIClientIDLength = bytesToInt(buff, offset, offset+2)
		offset += 2

		// Parse API Client ID Content
		if len(buff) >= offset+req.APIClientIDLength {
			req.APIClientIDContent = buff[offset : offset+req.APIClientIDLength]
			offset += req.APIClientIDLength
		}
	}

	// Parse remaining fields similarly...
	// (Implementation depends on exact protocol requirements)

	return req, nil
}

// Serialize response
func serializeResponse(resp Response) []byte {
	var buf bytes.Buffer

	// Calculate message size (excluding the message size field itself)
	messageContent := bytes.Buffer{}

	// Correlation ID (4 bytes)
	messageContent.Write(intToBytes(resp.CorrelationID, 4))

	// Error code (2 bytes)
	messageContent.Write(intToBytes(resp.ErrorCode, 2))

	// Array length (1 byte for compact array)
	messageContent.WriteByte(byte(resp.ArrayLength))

	// API Versions
	for _, apiVer := range resp.APIVersions {
		messageContent.Write(intToBytes(apiVer.APIKey, 2))
		messageContent.Write(intToBytes(apiVer.MinVersion, 2))
		messageContent.Write(intToBytes(apiVer.MaxVersion, 2))
		messageContent.WriteByte(byte(apiVer.TagBuffer))
	}

	// Throttle time (4 bytes)
	messageContent.Write(intToBytes(resp.ThrottleTime, 4))

	// Tag buffer (1 byte)
	messageContent.WriteByte(byte(resp.TagBuffer))

	// Write actual message size
	actualMessageSize := messageContent.Len()
	buf.Write(intToBytes(actualMessageSize, 4))

	// Write message content
	buf.Write(messageContent.Bytes())

	return buf.Bytes()
}
