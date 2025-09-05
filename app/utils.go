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
		fmt.Println("\tTopicName: ", string(topic.TopicName))
		fmt.Println("\tTopicTagBuffer: ", topic.TopicTagBuffer)
		fmt.Println("\tTopicID: ", string(topic.TopicID[:]))
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
	fmt.Println("TopicClientIDContent: ", string(request.TopicClientIDContent))
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

func int16ToBytes(val int16) []byte {
	return []byte{byte(val >> 8), byte(val)}
}

func int32ToBytes(val int32) []byte {
	return []byte{byte(val >> 24), byte(val >> 16), byte(val >> 8), byte(val)}
}

// intToBytes converts various integer types to a byte slice of specified length
func intToBytes(val interface{}, valByteLen int) []byte {
	bs := make([]byte, valByteLen)
	var v uint64

	// Convert val to uint64 for unified processing
	switch t := val.(type) {
	case int:
		v = uint64(t)
	case int8:
		v = uint64(t)
	case int16:
		v = uint64(t)
	case int32:
		v = uint64(t)
	case int64:
		v = uint64(t)
	case uint:
		v = uint64(t)
	case uint8:
		v = uint64(t)
	case uint16:
		v = uint64(t)
	case uint32:
		v = uint64(t)
	case uint64:
		v = t
	default:
		panic(fmt.Sprintf("Unsupported type: %T", val))
	}

	switch valByteLen {
	case 1:
		bs[0] = byte(v)
	case 2:
		binary.BigEndian.PutUint16(bs, uint16(v))
	case 4:
		binary.BigEndian.PutUint32(bs, uint32(v))
	case 8:
		binary.BigEndian.PutUint64(bs, v)
	default:
		// Custom length: manually encode from most significant byte down
		fmt.Println("CUSTOM LENGTH PASSED FOR INT TO BYTE CONVERSION")
		for i := 0; i < valByteLen && i < 8; i++ {
			bs[i] = byte(v >> ((valByteLen - 1 - i) * 8))
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

func bytesToInt16(buff []byte, start, end int) int16 {
	if end-start == 2 {
		return int16(buff[start])<<8 | int16(buff[start+1])
	}
	return 0
}

func bytesToInt32(buff []byte, start, end int) int32 {
	if end-start == 4 {
		return int32(buff[start])<<24 | int32(buff[start+1])<<16 | int32(buff[start+2])<<8 | int32(buff[start+3])
	}
	return 0
}

func FindTopicInGlobalMetadata(global_metadata file_metadata.ClusterMetaData, topic_name string) *file_metadata.TopicValue {
	for _, batch := range global_metadata.Batches {
		for _, record := range batch.Records {
			if record.ValueType == 2 {
				if topic, ok := record.Value.(file_metadata.TopicValue); ok {
					if topic.TopicName == topic_name {
						return &topic
					}
				}
			}
		}
	}
	return nil
}
