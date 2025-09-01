package main

import (
	"bytes"
	"fmt"
	"io"
	"net"
	"os"
)

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
		fmt.Println("Accepted connection")
		if err != nil {
			fmt.Println("Error accepting connection: ", err.Error())
			os.Exit(1)
		}
		go handleConnection(conn)
	}
}

func handleConnection(conn net.Conn) {
	fmt.Println("Handling connection")
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

		fmt.Println("Received bytes:", n)

		minimalReq, err := deserializeMinimalRequest(buff[:n])
		if err != nil {
			fmt.Println("Error deserializing minimal request:", err)
			continue
		}

		fmt.Printf("Parsed request: APIKey=%d, Version=%d, CorrelationID=%d\n",
			minimalReq.RequestAPIKey, minimalReq.RequestAPIVersion, minimalReq.CorrelationID)

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

		switch minimalReq.RequestAPIKey {
		case ApiVersionAPIKEY:
			handleAPIRequest(minimalReq, &response)
		case DescribeTopicPartitionsAPIKEY:
			handleDescribeRequest(minimalReq, &response)
		default:
			// response = handleDefaultRequest(minimalReq);
		}

		responseBytes := serializeResponse(response)
		conn.Write(responseBytes)
	}
}

func handleDescribeRequest(minimalReq *MinimalRequest, response *Response) {

}

func handleAPIRequest(minimalReq *MinimalRequest, response *Response) {
	// Create API func bytesToIntVersions response
	if !(minimalReq.RequestAPIVersion >= 0 && minimalReq.RequestAPIVersion <= 4) {
		response.ErrorCode = 35
	}
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
