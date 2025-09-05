package main

import (
	// "bytes"
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
	req.CorrelationID = bytesToInt32(buff, offset, offset+4)
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
				topic.TopicNameLength = int8(buff[offset])
				offset++

				// Topic name
				nameLen := int(topic.TopicNameLength - 1) // Compact string encoding
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
	var buffer []byte

	// Placeholder for message size (4 bytes)
	messageSizePos := len(buffer)
	buffer = append(buffer, 0, 0, 0, 0)

	// Correlation ID
	buffer = append(buffer, int32ToBytes(resp.CorrelationID)...)

	// Tag buffer (1 byte)
	buffer = append(buffer, byte(resp.TagBuffer))

	// Throttle time
	buffer = append(buffer, int32ToBytes(resp.ThrottleTime)...)

	// Topics array length (compact = len + 1)
	buffer = append(buffer, byte(len(resp.Topics)+1))

	for _, topic := range resp.Topics {
		// Error code
		buffer = append(buffer, int16ToBytes(topic.ErrorCode)...)

		// Topic name length + name
		buffer = append(buffer, byte(len(topic.TopicName)+1)) // Compact string length
		buffer = append(buffer, topic.TopicName...)

		// Topic tag buffer
		buffer = append(buffer, byte(topic.TopicTagBuffer))

		// IsInternal
		if topic.IsInternal {
			buffer = append(buffer, byte(1))
		} else {
			buffer = append(buffer, byte(0))
		}

		// Topic ID (16 bytes)
		buffer = append(buffer, topic.TopicID[:]...)

		// Partitions array length (compact = len + 1)
		buffer = append(buffer, byte(len(topic.PartitionsArray)+1))

		for _, partition := range topic.PartitionsArray {
			buffer = append(buffer, int16ToBytes(partition.ErrorCode)...)
			buffer = append(buffer, int32ToBytes(partition.PartitionIndex)...)
			buffer = append(buffer, int32ToBytes(partition.LeaderID)...)
			buffer = append(buffer, int32ToBytes(partition.LeaderEpoch)...)

			// Replica nodes (compact)
			buffer = append(buffer, byte(len(partition.ReplicaNodes)+1))
			for _, id := range partition.ReplicaNodes {
				buffer = append(buffer, int32ToBytes(id)...)
			}

			// ISR nodes (compact)
			buffer = append(buffer, byte(len(partition.ISRNodes)+1))
			for _, id := range partition.ISRNodes {
				buffer = append(buffer, int32ToBytes(id)...)
			}

			// Eligible leader replicas (compact)
			buffer = append(buffer, byte(len(partition.EligibleLeaderReplicas)+1))
			for _, id := range partition.EligibleLeaderReplicas {
				buffer = append(buffer, int32ToBytes(id)...)
			}

			// Last known ELR (compact)
			buffer = append(buffer, byte(len(partition.LastKnownELR)+1))
			for _, id := range partition.LastKnownELR {
				buffer = append(buffer, int32ToBytes(id)...)
			}

			// Offline replicas (compact)
			buffer = append(buffer, byte(len(partition.OfflineReplicas)+1))
			for _, id := range partition.OfflineReplicas {
				buffer = append(buffer, int32ToBytes(id)...)
			}

			// Partition tag buffer (1 byte)
			buffer = append(buffer, byte(partition.TaggedFields))
		}

		// Topic auth ops
		buffer = append(buffer, int32ToBytes(topic.TopicAuthOps)...)

		// Response tag buffer
		buffer = append(buffer, byte(topic.ResponseTagBuffer))
	}

	// Next cursor
	buffer = append(buffer, byte(resp.NextCursor))

	// Response tag buffer
	buffer = append(buffer, byte(resp.ResponseTagBuffer))

	// Set message size at the beginning (excluding the 4 bytes of length itself)
	messageSize := len(buffer) - 4
	copy(buffer[messageSizePos:messageSizePos+4], int32ToBytes(int32(messageSize)))

	return buffer
}

func createUnknownTopicResponse(req *DescribeTopicPartitionsRequest) *DescribeTopicPartitionsResponse {
	resp := &DescribeTopicPartitionsResponse{
		CorrelationID:     req.CorrelationID,
		TagBuffer:         0,
		ThrottleTime:      0,
		TopicsArrayLength: int8(len(req.Topics) + 1), // Compact array encoding
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
			PartitionsArray:   make([]Partition, 0), // Compact array with 0 partitions
			TopicAuthOps:      0,                    // No authorized operations
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
	resp.MessageSize = bytesToInt32(buff, offset, offset+4)
	offset += 4

	if len(buff) < offset+4 {
		return nil, fmt.Errorf("buffer too short for correlation ID")
	}

	// Parse correlation ID
	resp.CorrelationID = bytesToInt32(buff, offset, offset+4)
	offset += 4

	if len(buff) <= offset {
		return nil, fmt.Errorf("buffer too short for tag buffer")
	}

	// Parse tag buffer
	resp.TagBuffer = int8(buff[offset])
	offset++

	if len(buff) < offset+4 {
		return nil, fmt.Errorf("buffer too short for throttle time")
	}

	// Parse throttle time
	resp.ThrottleTime = bytesToInt32(buff, offset, offset+4)
	offset += 4

	if len(buff) <= offset {
		return nil, fmt.Errorf("buffer too short for topics array length")
	}

	// Parse topics array length (compact array)
	resp.TopicsArrayLength = int8(buff[offset])
	offset++

	// Parse topics
	topicsCount := int(resp.TopicsArrayLength) - 1 // Compact array encoding
	for i := 0; i < topicsCount; i++ {
		topic := TopicResponse{}

		// Error code (2 bytes)
		if len(buff) < offset+2 {
			return nil, fmt.Errorf("buffer too short for topic error code")
		}
		topic.ErrorCode = bytesToInt16(buff, offset, offset+2)
		offset += 2

		// Topic name length (1 byte - compact string)
		if len(buff) <= offset {
			return nil, fmt.Errorf("buffer too short for topic name length")
		}
		topic.TopicNameLength = int8(buff[offset])
		offset++

		// Topic name
		nameLen := int(topic.TopicNameLength) - 1 // Compact string encoding
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
		topic.TopicTagBuffer = int8(buff[offset])
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

		// Partitions array length (1 byte - compact array)
		if len(buff) <= offset {
			return nil, fmt.Errorf("buffer too short for partitions array length")
		}
		partitionsArrayLength := int8(buff[offset])
		offset++

		// Parse partitions
		partitionsCount := int(partitionsArrayLength) - 1 // Compact array encoding
		for j := 0; j < partitionsCount; j++ {
			partition := Partition{}

			// Error code (2 bytes)
			if len(buff) < offset+2 {
				return nil, fmt.Errorf("buffer too short for partition error code")
			}
			partition.ErrorCode = bytesToInt16(buff, offset, offset+2)
			offset += 2

			// Partition index (4 bytes)
			if len(buff) < offset+4 {
				return nil, fmt.Errorf("buffer too short for partition index")
			}
			partition.PartitionIndex = bytesToInt32(buff, offset, offset+4)
			offset += 4

			// Leader ID (4 bytes)
			if len(buff) < offset+4 {
				return nil, fmt.Errorf("buffer too short for leader ID")
			}
			partition.LeaderID = bytesToInt32(buff, offset, offset+4)
			offset += 4

			// Leader epoch (4 bytes)
			if len(buff) < offset+4 {
				return nil, fmt.Errorf("buffer too short for leader epoch")
			}
			partition.LeaderEpoch = bytesToInt32(buff, offset, offset+4)
			offset += 4

			// Replica nodes array (compact array)
			if len(buff) <= offset {
				return nil, fmt.Errorf("buffer too short for replica nodes length")
			}
			replicaNodesLength := int8(buff[offset])
			offset++

			replicaNodesCount := int(replicaNodesLength) - 1
			for k := 0; k < replicaNodesCount; k++ {
				if len(buff) < offset+4 {
					return nil, fmt.Errorf("buffer too short for replica node")
				}
				replicaNode := bytesToInt32(buff, offset, offset+4)
				partition.ReplicaNodes = append(partition.ReplicaNodes, replicaNode)
				offset += 4
			}

			// ISR nodes array (compact array)
			if len(buff) <= offset {
				return nil, fmt.Errorf("buffer too short for ISR nodes length")
			}
			isrNodesLength := int8(buff[offset])
			offset++

			isrNodesCount := int(isrNodesLength) - 1
			for k := 0; k < isrNodesCount; k++ {
				if len(buff) < offset+4 {
					return nil, fmt.Errorf("buffer too short for ISR node")
				}
				isrNode := bytesToInt32(buff, offset, offset+4)
				partition.ISRNodes = append(partition.ISRNodes, isrNode)
				offset += 4
			}

			// Eligible leader replicas array (compact array)
			if len(buff) <= offset {
				return nil, fmt.Errorf("buffer too short for eligible leader replicas length")
			}
			eligibleLeaderReplicasLength := int8(buff[offset])
			offset++

			eligibleLeaderReplicasCount := int(eligibleLeaderReplicasLength) - 1
			for k := 0; k < eligibleLeaderReplicasCount; k++ {
				if len(buff) < offset+4 {
					return nil, fmt.Errorf("buffer too short for eligible leader replica")
				}
				eligibleLeaderReplica := bytesToInt32(buff, offset, offset+4)
				partition.EligibleLeaderReplicas = append(partition.EligibleLeaderReplicas, eligibleLeaderReplica)
				offset += 4
			}

			// Last known ELR array (compact array)
			if len(buff) <= offset {
				return nil, fmt.Errorf("buffer too short for last known ELR length")
			}
			lastKnownELRLength := int8(buff[offset])
			offset++

			lastKnownELRCount := int(lastKnownELRLength) - 1
			for k := 0; k < lastKnownELRCount; k++ {
				if len(buff) < offset+4 {
					return nil, fmt.Errorf("buffer too short for last known ELR")
				}
				lastKnownELR := bytesToInt32(buff, offset, offset+4)
				partition.LastKnownELR = append(partition.LastKnownELR, lastKnownELR)
				offset += 4
			}

			// Offline replicas array (compact array)
			if len(buff) <= offset {
				return nil, fmt.Errorf("buffer too short for offline replicas length")
			}
			offlineReplicasLength := int8(buff[offset])
			offset++

			offlineReplicasCount := int(offlineReplicasLength) - 1
			for k := 0; k < offlineReplicasCount; k++ {
				if len(buff) < offset+4 {
					return nil, fmt.Errorf("buffer too short for offline replica")
				}
				offlineReplica := bytesToInt32(buff, offset, offset+4)
				partition.OfflineReplicas = append(partition.OfflineReplicas, offlineReplica)
				offset += 4
			}

			// Partition tag buffer (1 byte)
			if len(buff) <= offset {
				return nil, fmt.Errorf("buffer too short for partition tag buffer")
			}
			partition.TaggedFields = int8(buff[offset])
			offset++

			topic.PartitionsArray = append(topic.PartitionsArray, partition)
		}

		// Topic auth ops (4 bytes)
		if len(buff) < offset+4 {
			return nil, fmt.Errorf("buffer too short for topic auth ops")
		}
		topic.TopicAuthOps = bytesToInt32(buff, offset, offset+4)
		offset += 4

		// Response tag buffer (1 byte)
		if len(buff) <= offset {
			return nil, fmt.Errorf("buffer too short for response tag buffer")
		}
		topic.ResponseTagBuffer = int8(buff[offset])
		offset++

		resp.Topics = append(resp.Topics, topic)
	}

	// Parse next cursor (1 byte)
	if len(buff) > offset {
		resp.NextCursor = int8(buff[offset])
		offset++
	}

	// Parse response tag buffer (1 byte)
	if len(buff) > offset {
		resp.ResponseTagBuffer = int8(buff[offset])
		offset++
	}

	return resp, nil
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

// Deserialize minimal request (always works for basic Kafka requests)
func deserializeMinimalRequest(buff []byte) (*MinimalRequest, error) {
	if len(buff) < 12 {
		return nil, fmt.Errorf("buffer too short for minimal request")
	}

	req := &MinimalRequest{
		MessageSize:       bytesToInt(buff, 0, 4),
		RequestAPIKey:     bytesToInt(buff, 4, 6),
		RequestAPIVersion: bytesToInt(buff, 6, 8),
		CorrelationID:     bytesToInt32(buff, 8, 12),
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
	req.CorrelationID = bytesToInt32(buff, 8, 12)

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
