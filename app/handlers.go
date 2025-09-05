package main

import (
	"encoding/hex"
	"fmt"
	"io"
	"net"
	"os"
)

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
			// printDescribeTopicResponse(response)
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
