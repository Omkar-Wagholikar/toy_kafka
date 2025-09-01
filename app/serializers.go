package main

import (
	"bytes"
	"fmt"
)

func deserializeDescribeTopicPartitionsRequest(buff []byte) (*DescribeTopicPartitionsRequest, error) {
	if len(buff) < 12 {
		return nil, fmt.Errorf("buffer too short for minimal request")
	}

	req := &DescribeTopicPartitionsRequest{}
	offset := 0

	// Parse minimal request
	req.MessageSize = bytesToInt(buff, offset, offset+4)
	offset += 4
	req.RequestAPIKey = bytesToInt(buff, offset, offset+2)
	offset += 2
	req.RequestAPIVersion = bytesToInt(buff, offset, offset+2)
	offset += 2
	req.CorrelationID = bytesToInt(buff, offset, offset+4)
	offset += 4

	if len(buff) <= offset {
		return req, nil // Only minimal request available
	}

	// Parse topic client ID length
	if len(buff) >= offset+2 {
		req.TopicClientIDLength = bytesToInt(buff, offset, offset+2)
		offset += 2

		// Parse topic client ID content
		if len(buff) >= offset+req.TopicClientIDLength {
			req.TopicClientIDContent = make([]byte, req.TopicClientIDLength)
			copy(req.TopicClientIDContent, buff[offset:offset+req.TopicClientIDLength])
			offset += req.TopicClientIDLength
		}
	}

	// Parse tag buffer
	if len(buff) > offset {
		req.TagBuffer = int(buff[offset])
		offset++
	}

	// Parse topics array length
	if len(buff) > offset {
		req.TopicsArrayLength = int(buff[offset])
		offset++

		// Parse topics
		topicsCount := req.TopicsArrayLength - 1 // Compact array encoding
		for i := 0; i < topicsCount && len(buff) > offset; i++ {
			topic := TopicRequest{}

			// Topic name length
			if len(buff) > offset {
				topic.TopicNameLength = int(buff[offset])
				offset++

				// Topic name
				nameLen := topic.TopicNameLength - 1 // Compact string encoding
				if len(buff) >= offset+nameLen {
					topic.TopicName = make([]byte, nameLen)
					copy(topic.TopicName, buff[offset:offset+nameLen])
					offset += nameLen
				}
			}

			// Topic tag buffer
			if len(buff) > offset {
				topic.TopicTagBuffer = int(buff[offset])
				offset++
			}

			req.Topics = append(req.Topics, topic)
		}
	}

	// Parse response partition limit
	if len(buff) >= offset+4 {
		req.ResponsePartitionLimit = bytesToInt(buff, offset, offset+4)
		offset += 4
	}

	// Parse cursor
	if len(buff) > offset {
		req.Cursor = int(buff[offset])
		offset++
	}

	// Parse request tag buffer
	if len(buff) > offset {
		req.RequestTagBuffer = int(buff[offset])
		offset++
	}

	return req, nil
}

func serializeDescribeTopicPartitionsResponse(resp *DescribeTopicPartitionsResponse) []byte {
	var buf bytes.Buffer

	// Build message content first (excluding message size)
	var messageContent bytes.Buffer

	// Correlation ID (4 bytes)
	messageContent.Write(intToBytes(resp.CorrelationID, 4))

	// Tag buffer (1 byte)
	messageContent.WriteByte(byte(resp.TagBuffer))

	// Throttle time (4 bytes)
	messageContent.Write(intToBytes(resp.ThrottleTime, 4))

	// Topics array length (1 byte - compact array)
	messageContent.WriteByte(byte(resp.TopicsArrayLength))

	// Topics
	for _, topic := range resp.Topics {
		// Error code (2 bytes)
		messageContent.Write(intToBytes(topic.ErrorCode, 2))

		// Topic name length (1 byte - compact string)
		messageContent.WriteByte(byte(topic.TopicNameLength))

		// Topic name
		messageContent.Write(topic.TopicName)

		// Topic tag buffer (1 byte)
		messageContent.WriteByte(byte(topic.TopicTagBuffer))

		// Topic ID (16 bytes)
		messageContent.Write(topic.TopicID[:])

		// Is internal (1 byte)
		if topic.IsInternal {
			messageContent.WriteByte(1)
		} else {
			messageContent.WriteByte(0)
		}

		// Partitions array (1 byte - compact array)
		messageContent.WriteByte(byte(topic.PartitionsArray))

		// Topic auth ops (4 bytes)
		messageContent.Write(intToBytes(topic.TopicAuthOps, 4))

		// Response tag buffer (1 byte)
		messageContent.WriteByte(byte(topic.ResponseTagBuffer))
	}

	// Next cursor (1 byte)
	messageContent.WriteByte(byte(resp.NextCursor))

	// Response tag buffer (1 byte)
	messageContent.WriteByte(byte(resp.ResponseTagBuffer))

	// Calculate and write message size
	actualMessageSize := messageContent.Len()
	buf.Write(intToBytes(actualMessageSize, 4))

	// Write message content
	buf.Write(messageContent.Bytes())

	return buf.Bytes()
}

func createUnknownTopicResponse(req *DescribeTopicPartitionsRequest) *DescribeTopicPartitionsResponse {
	resp := &DescribeTopicPartitionsResponse{
		CorrelationID:     req.CorrelationID,
		TagBuffer:         0,
		ThrottleTime:      0,
		TopicsArrayLength: len(req.Topics) + 1, // Compact array encoding
		NextCursor:        0,
		ResponseTagBuffer: 0,
	}

	// Create topic responses with unknown topic error
	for _, reqTopic := range req.Topics {
		topicResp := TopicResponse{
			ErrorCode:         ErrorCodeUnknownTopic,
			TopicNameLength:   reqTopic.TopicNameLength,
			TopicName:         reqTopic.TopicName,
			TopicTagBuffer:    0,
			TopicID:           [16]byte{}, // All zeros
			IsInternal:        false,
			PartitionsArray:   1, // Compact array with 0 partitions
			TopicAuthOps:      0, // No authorized operations
			ResponseTagBuffer: 0,
		}
		resp.Topics = append(resp.Topics, topicResp)
	}

	return resp
}
