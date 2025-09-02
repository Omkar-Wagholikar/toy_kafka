package main

import (
	// "bytes"
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
	var buf []byte

	// Reserve 4 bytes for message size (we’ll patch later)
	buf = append(buf, 0, 0, 0, 0)

	// Correlation ID (int32)
	buf = append(buf, intToBytes(resp.CorrelationID, 4)...)

	// Top-level tag buffer (varint / 1 byte here since empty)
	buf = append(buf, byte(0))

	// Throttle time (int32)
	buf = append(buf, intToBytes(resp.ThrottleTime, 4)...)

	// Topics array length (compact array = len + 1)
	buf = append(buf, byte(len(resp.Topics)+1))

	// Topics
	for _, topic := range resp.Topics {
		// Error code (int16)
		buf = append(buf, intToBytes(topic.ErrorCode, 2)...)

		// Topic name (compact string = length+1 + bytes)
		buf = append(buf, byte(len(topic.TopicName)+1)) // topic name length + 1
		buf = append(buf, topic.TopicName...)           // the actual topic name

		// Topic tag buffer
		buf = append(buf, byte(0))

		// Topic ID (16 bytes, all zeros here)
		buf = append(buf, topic.TopicID[:]...)

		// IsInternal (boolean -> byte)
		if topic.IsInternal {
			buf = append(buf, byte(1))
		} else {
			buf = append(buf, byte(0))
		}

		// Partitions array (compact array -> only header since empty)
		buf = append(buf, byte(0))

		// Topic authorized ops (int32)
		// NOTE: spec default is 16777216 (0x01000000), not zero
		buf = append(buf, intToBytes(16777216, 4)...)

		// Response tag buffer
		buf = append(buf, byte(0))
	}

	// Next cursor (nullable compact struct -> 0x00 when null)
	buf = append(buf, byte(0))

	// Response tag buffer
	buf = append(buf, byte(0))

	// Patch message size at start (total length - 4 bytes)
	msgSize := len(buf) - 4
	sizeBytes := intToBytes(msgSize, 4)
	copy(buf[0:4], sizeBytes)

	return buf
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
		var topicID [16]byte
		for i := range topicID {
			topicID[i] = 0x00
		}

		topicResp := TopicResponse{
			ErrorCode:         ErrorCodeUnknownTopic,
			TopicNameLength:   reqTopic.TopicNameLength,
			TopicName:         reqTopic.TopicName,
			TopicTagBuffer:    0,
			TopicID:           topicID, // All zeros
			IsInternal:        false,
			PartitionsArray:   1, // Compact array with 0 partitions
			TopicAuthOps:      0, // No authorized operations
			ResponseTagBuffer: 0,
		}
		resp.Topics = append(resp.Topics, topicResp)
	}

	return resp
}

func deserializeDescribeTopicPartitionsResponse(buff []byte) (*DescribeTopicPartitionsResponse, error) {
	if len(buff) < 4 {
		return nil, fmt.Errorf("buffer too short for message size")
	}

	resp := &DescribeTopicPartitionsResponse{}
	offset := 0

	// Parse message size
	resp.MessageSize = bytesToInt(buff, offset, offset+4)
	offset += 4

	if len(buff) < offset+4 {
		return nil, fmt.Errorf("buffer too short for correlation ID")
	}

	// Parse correlation ID
	resp.CorrelationID = bytesToInt(buff, offset, offset+4)
	offset += 4

	if len(buff) <= offset {
		return nil, fmt.Errorf("buffer too short for tag buffer")
	}

	// Parse tag buffer
	resp.TagBuffer = int(buff[offset])
	offset++

	if len(buff) < offset+4 {
		return nil, fmt.Errorf("buffer too short for throttle time")
	}

	// Parse throttle time
	resp.ThrottleTime = bytesToInt(buff, offset, offset+4)
	offset += 4

	if len(buff) <= offset {
		return nil, fmt.Errorf("buffer too short for topics array length")
	}

	// Parse topics array length (compact array)
	resp.TopicsArrayLength = int(buff[offset])
	offset++

	// Parse topics
	topicsCount := resp.TopicsArrayLength - 1 // Compact array encoding
	for i := 0; i < topicsCount; i++ {
		topic := TopicResponse{}

		// Error code (2 bytes)
		if len(buff) < offset+2 {
			return nil, fmt.Errorf("buffer too short for topic error code")
		}
		topic.ErrorCode = bytesToInt(buff, offset, offset+2)
		offset += 2

		// Topic name length (1 byte - compact string)
		if len(buff) <= offset {
			return nil, fmt.Errorf("buffer too short for topic name length")
		}
		topic.TopicNameLength = int(buff[offset])
		offset++

		// Topic name
		nameLen := topic.TopicNameLength - 1 // Compact string encoding
		if nameLen > 0 {
			if len(buff) < offset+nameLen {
				return nil, fmt.Errorf("buffer too short for topic name")
			}
			topic.TopicName = make([]byte, nameLen)
			copy(topic.TopicName, buff[offset:offset+nameLen])
			offset += nameLen
		}

		// Topic tag buffer (1 byte)
		if len(buff) <= offset {
			return nil, fmt.Errorf("buffer too short for topic tag buffer")
		}
		topic.TopicTagBuffer = int(buff[offset])
		offset++

		// Topic ID (16 bytes)
		if len(buff) < offset+16 {
			return nil, fmt.Errorf("buffer too short for topic ID")
		}
		copy(topic.TopicID[:], buff[offset:offset+16])
		offset += 16

		// Is internal (1 byte)
		if len(buff) <= offset {
			return nil, fmt.Errorf("buffer too short for is_internal")
		}
		topic.IsInternal = buff[offset] != 0
		offset++

		// Partitions array (1 byte - compact array)
		if len(buff) <= offset {
			return nil, fmt.Errorf("buffer too short for partitions array")
		}
		topic.PartitionsArray = int(buff[offset])
		offset++

		// Topic auth ops (4 bytes)
		if len(buff) < offset+4 {
			return nil, fmt.Errorf("buffer too short for topic auth ops")
		}
		topic.TopicAuthOps = bytesToInt(buff, offset, offset+4)
		offset += 4

		// Response tag buffer (1 byte)
		if len(buff) <= offset {
			return nil, fmt.Errorf("buffer too short for response tag buffer")
		}
		topic.ResponseTagBuffer = int(buff[offset])
		offset++

		resp.Topics = append(resp.Topics, topic)
	}

	// Parse next cursor (1 byte)
	if len(buff) > offset {
		resp.NextCursor = int(buff[offset])
		offset++
	}

	// Parse response tag buffer (1 byte)
	if len(buff) > offset {
		resp.ResponseTagBuffer = int(buff[offset])
		offset++
	}

	return resp, nil
}
