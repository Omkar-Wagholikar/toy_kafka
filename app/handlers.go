package main

import (
	"fmt"
	"io"
	"net"
	"os"
	"toy_kafka/app/file_metadata"
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

		switch minimalReq.RequestAPIKey {
		case ApiVersionAPIKEY:
			fmt.Println("ApiVersionAPIKEY")
			response := handleAPIRequest(minimalReq)
			responseBytes := serializeResponse(response)
			conn.Write(responseBytes)

		case DescribeTopicPartitionsAPIKEY:
			response := handleDescribeRequest(buff, n)
			responseBytes := serializeDescribeTopicPartitionsResponse(response)

			// data := hex.EncodeToString(responseBytes)
			// fmt.Println(">", data)
			fmt.Printf("\n\nRESPONSE\n\n")
			printDescribeTopicResponse(response)

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
	fmt.Printf("\nREQUEST\n\n")
	printDescribeTopicRequest(req)

	if err != nil {
		fmt.Println("Error deserializing request:", err)
		os.Exit(1)
	}

	var found *file_metadata.TopicValue
	for _, topic := range req.Topics {
		found = FindTopicInGlobalMetadata(*global_metadata, string(topic.TopicName))
		if found != nil {
			break
		}
	}
	// Create response with unknown topic error
	response := createUnknownTopicResponse(req)
	if found != nil {
		response.Topics[0].ErrorCode = 0
		response.Topics[0].TopicID = found.TopicId
	}

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
