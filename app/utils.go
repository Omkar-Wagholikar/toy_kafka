package main

import (
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"net"
	"toy_kafka/app/file_metadata"
)

func printDescribeTopicResponse(response *DescribeTopicPartitionsResponse) {
	fmt.Println("MessageSize: ", response.MessageSize)
	fmt.Println("CorrelationID: ", response.CorrelationID)
	fmt.Println("TagBuffer: ", response.TagBuffer)
	fmt.Println("ThrottleTime: ", response.ThrottleTime)
	fmt.Println("TopicsArrayLength: ", response.TopicsArrayLength)
	for i, topic := range response.Topics {
		fmt.Println("TOPIC: ", (i + 1))
		fmt.Println("\tErrorCode: ", topic.ErrorCode)
		fmt.Println("\tTopicNameLength: ", topic.TopicNameLength)
		fmt.Println("\tTopicName: ", hex.EncodeToString(topic.TopicName))
		fmt.Println("\tTopicTagBuffer: ", topic.TopicTagBuffer)
		fmt.Println("\tTopicID: ", hex.EncodeToString(topic.TopicID[:]))
		fmt.Println("\tIsInternal: ", topic.IsInternal)
		fmt.Println("\tPartitionsArray: ", topic.PartitionsArray)
		fmt.Println("\tTopicAuthOps: ", topic.TopicAuthOps)
		fmt.Println("\tResponseTagBuffer: ", topic.ResponseTagBuffer)
	}
	fmt.Println("NextCursor: ", response.NextCursor)
	fmt.Println("ResponseTagBuffer: ", response.ResponseTagBuffer)
}

func printDescribeTopicRequest(request *DescribeTopicPartitionsRequest) {

	fmt.Println("MessageSize: ", request.MessageSize)
	fmt.Println("RequestAPIKey: ", request.RequestAPIKey)
	fmt.Println("RequestAPIVersion: ", request.RequestAPIVersion)
	fmt.Println("CorrelationID: ", request.CorrelationID)
	fmt.Println("TopicClientIDLength: ", request.TopicClientIDLength)
	fmt.Println("TopicClientIDContent: ", hex.EncodeToString(request.TopicClientIDContent))
	fmt.Println("TagBuffer: ", request.TagBuffer)
	fmt.Println("TopicsArrayLength: ", request.TopicsArrayLength)

	for i, topic := range request.Topics {
		fmt.Println("\tTopic: ", (i + 1))
		fmt.Println("\tTopicNameLength: ", topic.TopicNameLength)
		fmt.Println("\tTopicName: ", hex.EncodeToString(topic.TopicName))
		fmt.Println("\tTopicTagBuffer: ", topic.TopicTagBuffer)
	}

	fmt.Println("ResponsePartitionLimit: ", request.ResponsePartitionLimit)
	fmt.Println("Cursor: ", request.Cursor)
	fmt.Println("RequestTagBuffer: ", request.RequestTagBuffer)
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

func FindTopicInGlobalMetadata(global_metadata file_metadata.ClusterMetaData, topic_name string) {
	for _, batch := range global_metadata.Batches {
		for _, record := range batch.Records {
			if record.ValueType == 2 {
				// Assert interface{} to TopicRecord
				if topic, ok := record.Value.(file_metadata.TopicValue); ok {
					fmt.Println(topic.TopicName)
				}
			}
		}
	}
}
