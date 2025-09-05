package file_metadata

import (
	"fmt"
	"toy_kafka/app/utils"

	"github.com/google/uuid"
)

func CreateAndPopulateLog(path string) {

	hexString := "00000000000000010000004f0000000102b069457c00000000000000000191e05af81800000191e05af818ffffffffffffffffffffffffffff000000013a000000012e010c00116d657461646174612e76657273696f6e0014000000000000000000020000009a00000001029b7c443100000000000100000191e05b2d1500000191e05b2d15ffffffffffffffffffffffffffff000000023c00000001300102000462617a000000000000400080000000000000640000900100000201820101030100000000000000000000400080000000000000640200000001020000000101010000000100000000000000000210000000000040008000000000000001000000000000000000040000009a00000001021b29b4bf00000000000100000191e05b2d1500000191e05b2d15ffffffffffffffffffffffffffff000000023c000000013001020004666f6f00000000000040008000000000000070000090010000020182010103010000000000000000000040008000000000000070020000000102000000010101000000010000000000000000021000000000004000800000000000000100000000000000000006000000e40000000102f7c074ff00000000000200000191e05b2d1500000191e05b2d15ffffffffffffffffffffffffffff000000033c00000001300102000470617800000000000040008000000000000034000090010000020182010103010000000000000000000040008000000000000034020000000102000000010101000000010000000000000000021000000000004000800000000000000100009001000004018201010301000000010000000000004000800000000000003402000000010200000001010100000001000000000000000002100000000000400080000000000000010000"

	write_bin(path, hexString)
}

func createRecordHeader(stream []byte, offset int) (ValueTypeHeader, int) {

	frameVersion := int8(utils.BytesToInt(stream, offset, offset+1))
	offset += 1
	valueType := int8(utils.BytesToInt(stream, offset, offset+1))
	offset += 1

	version := int8(utils.BytesToInt(stream, offset, offset+1))
	offset += 1

	return ValueTypeHeader{
		frameVersion: frameVersion,
		valueType:    valueType,
		version:      version,
	}, offset
}

func CreateClusterMetaData(stream []byte) (*ClusterMetaData, error) {
	// fmt.Printf("[DEBUG] Parsing ClusterMetaData\n")
	// fmt.Printf("[DEBUG] Parsing - %x\n", stream)
	if len(stream) < 8 {
		return &ClusterMetaData{}, fmt.Errorf("insufficient data to parse the file")
	}
	streamSize := len(stream)
	offset := 0

	var batches []RecordBatch
	recordCount := 0
	for offset < streamSize {
		// baseOffset, _ := utils.BytesToInt(stream, offset, offset+8)
		// fmt.Printf("[DEBUG] baseOffet: %x - offset: %d", stream[offset:offset+8], offset)
		// if recordCount != int(baseOffset) {
		// 	// fmt.Printf("(panic) recordCount(%d) != baseOffset(%d)\n", recordCount, baseOffset)
		// 	panic("record count does not match baseOffset")
		// }
		recordCount++
		recordBatch, batch_offset := CreateRecordBatch(stream, offset)
		batches = append(batches, recordBatch)
		offset += batch_offset
	}
	metaData := &ClusterMetaData{Batches: batches}
	// fmt.Printf("[DEBUG] Parsed ClusterMetaData: %+v\n", metaData)
	return metaData, nil
}

func CreateRecordBatch(stream []byte, offset int) (RecordBatch, int) {
	// Parse out a particular record. (how to know if we're at the end of a file?)
	offset += 8 // initial offset (BaseOffset)

	batchLength := utils.BytesToInt(stream, offset, offset+4)
	finalOffset := 12 + int(batchLength)
	// // fmt.Printf("[DEUBG] batchLength  - %x; value: %d\n", stream[offset:offset+4], batchLength)
	offset += 4

	paritionLeaderEpochId := int32(utils.BytesToInt(stream, offset, offset+4))
	offset += 4

	// magicByte, _ := utils.BytesToInt8(stream[offset : offset+1])
	// // fmt.Printf("[DEUBG] magicByte: %x - %d\n", stream[offset:offset+1], magicByte)
	offset += 1

	CRC := int32(utils.BytesToInt(stream, offset, offset+4))
	// // fmt.Printf("[DEUBG] CRC: %x - %d\n", stream[offset:offset+4], CRC)
	offset += 4

	attributes := int16(utils.BytesToInt(stream, offset, offset+2))
	// // fmt.Printf("[DEUBG] attributes: %x - %d\n", stream[offset:offset+2], attributes)
	offset += 2

	// lastOffsetDelta, _ := utils.BytesToInt32(stream[offset : offset+4])
	offset += 4

	baseTimestamp := int64(utils.BytesToInt(stream, offset, offset+8))
	// // fmt.Printf("[DEUBG] baseTimestamp: %x - %d\n", stream[offset:offset+8], baseTimestamp)
	offset += 8

	maxTimestamp := int64(utils.BytesToInt(stream, offset, offset+8))
	// // fmt.Printf("[DEUBG] maxTimestamp: %x - %d\n", stream[offset:offset+8], maxTimestamp)
	offset += 8

	producerId := int64(utils.BytesToInt(stream, offset, offset+8))
	// // fmt.Printf("[DEUBG] producerId: %x\n", stream[offset:offset+8])
	offset += 8

	producerEpoch := int16(utils.BytesToInt(stream, offset, offset+2))
	offset += 2

	baseSequenceId := int32(utils.BytesToInt(stream, offset, offset+4))
	offset += 4

	recordsLength := int32(utils.BytesToInt(stream, offset, offset+4))
	offset += 4

	var records []Record
	for i := 0; i < int(recordsLength); i++ {
		// parsing the records
		recordStartOffset := offset
		recordLength, bytesConsumed, _ := utils.ParseVarint(stream[offset:])
		offset += bytesConsumed

		// fmt.Printf("Record %d: recordLength=%d, startOffset=%d\n", i, recordLength, recordStartOffset)

		attributes := int8(utils.BytesToInt(stream, offset, offset+1))
		offset += 1

		timestampDelta, bytesConsumed, _ := utils.ParseVarint(stream[offset:])
		offset += bytesConsumed

		_, bytesConsumed, _ = utils.ParseVarint(stream[offset:])
		offset += bytesConsumed

		keyLength, bytesConsumed, _ := utils.ParseVarint(stream[offset:])
		offset += bytesConsumed
		// fmt.Printf("offset %d - size %d, keyLength: %d\n", offset, len(stream), keyLength)

		if keyLength != -1 {
			// parse the key value..
			// key := any
			offset += int(keyLength)
		}

		valueLength, bytesConsumed, _ := utils.ParseVarint(stream[offset:])
		offset += bytesConsumed
		// fmt.Printf("valueLength: %d, recordLength: %d\n", valueLength, recordLength)

		var value interface{}
		valueType := int8(0)
		if valueLength > 0 {
			// Parsing the ValueHeader
			header, newOffset := createRecordHeader(stream, offset)
			valueType = int8(header.valueType)
			offset = newOffset
			// fmt.Printf("header %v+ - offset %d\n", header, offset)

			// Parse the value
			value, newOffset = parseValue(stream, header, offset)
			offset = newOffset
			// fmt.Printf("value object: %v+\n", value)
		} else {
			// No value to parse
			value = nil
		}

		headersArrayCount := int(stream[offset])
		if headersArrayCount == 0x00 {
			offset++
		}

		// Ensure we've consumed exactly recordLength bytes from the record start
		expectedOffset := recordStartOffset + int(recordLength) + bytesConsumed
		if offset != expectedOffset {
			// fmt.Printf("WARNING: offset mismatch. Expected: %d, Actual: %d\n", expectedOffset, offset)
			// Adjust offset to expected position
			offset = expectedOffset
		}

		record := Record{
			attributes:     attributes,
			timestampDelta: timestampDelta,
			keySize:        keyLength,
			ValueType:      valueType,
			key:            nil,
			Value:          value,
		}
		records = append(records, record)
		// fmt.Printf("Final offset: %d - calc offset: %d\n", finalOffset, offset)
	}

	// fmt.Printf("Final offset: %d - calc offset: %d\n", finalOffset, offset)
	// // fmt.Printf("Final byte: %x\n", stream[offset])

	return RecordBatch{
		partitionLeaderEpoch: paritionLeaderEpochId,
		CRC:                  CRC,
		attributes:           attributes,
		baseTimestamp:        baseTimestamp,
		maxTimestamp:         maxTimestamp,
		producerId:           producerId,
		producerEpoch:        producerEpoch,
		baseSequence:         baseSequenceId,
		Records:              records,
	}, finalOffset
}

func parseValue(stream []byte, header ValueTypeHeader, offset int) (interface{}, int) {
	switch header.valueType {
	case 2: // Topic
		return parseTopicValue(stream, header, offset)
	case 3: // Partition Record
		return parsePartitionValue(stream, header, offset)
	case 12: // Feature Level Record
		return parseFeatureLevelValue(stream, header, offset)
	default:
		// fmt.Printf("Unknown valueType: %d, skipping...\n", header.valueType)
		// Return a placeholder value and don't advance offset much
		return fmt.Sprintf("UnknownValueType_%d", header.valueType), offset
	}
}

func parseTopicValue(
	stream []byte,
	header ValueTypeHeader,
	offset int,
) (TopicValue, int) {
	nameLength := int8(utils.BytesToInt(stream, offset, offset+1))
	offset++

	name := string(stream[offset : offset+int(nameLength)-1])
	offset += int(nameLength) - 1

	topicIdBytes := stream[offset : offset+16]
	// // fmt.Printf("DEBUG: Topic UUID bytes: %x\n", topicIdBytes)
	topicId, err := uuid.FromBytes(topicIdBytes)
	if err != nil {
		// fmt.Printf("DEBUG: UUID parsing error: %v\n", err)
	}
	// fmt.Printf("DEBUG: Parsed Topic UUID: %s\n", topicId)
	offset += 16

	// Tagged Fields Count
	offset += 1

	return TopicValue{
		header:    header,
		TopicName: name,
		topicId:   topicId,
	}, offset
}

func parseFeatureLevelValue(
	stream []byte,
	header ValueTypeHeader,
	offset int,
) (FeatureLevelValue, int) {
	nameLength := int8(utils.BytesToInt(stream, offset, offset+1))
	offset++

	name := string(stream[offset : offset+int(nameLength)-1])
	offset += int(nameLength) - 1

	featureLevel := int16(utils.BytesToInt(stream, offset, offset+2))
	offset += 2

	// Tagged Fields Count
	offset += 1

	return FeatureLevelValue{
		header:       header,
		Name:         name,
		featureLevel: featureLevel,
	}, offset
}

func parsePartitionValue(
	stream []byte,
	header ValueTypeHeader,
	offset int,
) (PartitionValue, int) {
	partitionId := int32(utils.BytesToInt(stream, offset, offset+4))
	offset += 4

	topicIdBytes := stream[offset : offset+16]
	// fmt.Printf("DEBUG: Partition Topic UUID bytes: %x\n", topicIdBytes)
	topicId, err := uuid.FromBytes(topicIdBytes)
	if err != nil {
		// fmt.Printf("DEBUG: Partition UUID parsing error: %v\n", err)
	}
	// fmt.Printf("DEBUG: Parsed Partition Topic UUID: %s\n", topicId)
	offset += 16

	replicaArrayLength := int(stream[offset]) - 1
	offset += 1

	replicaArray := make([]int32, replicaArrayLength)
	for i := 0; i < replicaArrayLength; i++ {
		replicaArrayId := int32(utils.BytesToInt(stream, offset, offset+4))
		offset += 4
		replicaArray[i] = replicaArrayId
	}

	syncReplicaArrayLength := int(stream[offset]) - 1
	offset += 1
	syncReplicaArray := make([]int32, syncReplicaArrayLength)
	for i := 0; i < syncReplicaArrayLength; i++ {
		syncReplicaArrayId := int32(utils.BytesToInt(stream, offset, offset+4))
		offset += 4
		syncReplicaArray[i] = syncReplicaArrayId
	}

	removingReplicaArrayLength := int(stream[offset]) - 1
	offset += 1
	removingReplicaArray := make([]int32, removingReplicaArrayLength)
	for i := 0; i < removingReplicaArrayLength; i++ {
		removingReplicaArrayId := int32(utils.BytesToInt(stream, offset, offset+4))
		offset += 4
		removingReplicaArray[i] = removingReplicaArrayId
	}

	addingReplicaArrayLength := int(stream[offset]) - 1
	offset += 1
	addingReplicaArray := make([]int32, addingReplicaArrayLength)
	for i := 0; i < addingReplicaArrayLength; i++ {
		addingReplicaArrayId := int32(utils.BytesToInt(stream, offset, offset+4))
		offset += 4
		addingReplicaArray[i] = addingReplicaArrayId
	}

	replicaLeaderId := int32(utils.BytesToInt(stream, offset, offset+4))
	offset += 4

	replicaLeaderEpoch := int32(utils.BytesToInt(stream, offset, offset+4))
	offset += 4

	partitionEpoch := int32(utils.BytesToInt(stream, offset, offset+4))
	offset += 4

	directoryArrayLength := int(stream[offset]) - 1
	offset += 1
	directoryArray := make([]uuid.UUID, directoryArrayLength)
	for i := 0; i < directoryArrayLength; i++ {
		directoryIdBytes := stream[offset : offset+16]
		// fmt.Printf("DEBUG: Directory UUID bytes: %x\n", directoryIdBytes)
		directoryId, err := uuid.FromBytes(directoryIdBytes)
		if err != nil {
			// fmt.Printf("DEBUG: Directory UUID parsing error: %v\n", err)
		}
		// fmt.Printf("DEBUG: Parsed Directory UUID: %s\n", directoryId)
		offset += 16
		directoryArray[i] = directoryId
	}

	// TaggedFieldsCount
	offset++

	return PartitionValue{
		header:               header,
		partitionId:          partitionId,
		topicId:              topicId,
		replicaIdArray:       replicaArray,
		inSyncReplicaArray:   syncReplicaArray,
		removingReplicaArray: removingReplicaArray,
		addingReplicaArray:   addingReplicaArray,
		leaderId:             replicaLeaderId,
		leaderEpoch:          replicaLeaderEpoch,
		partitionEpoch:       partitionEpoch,
		directoriesArray:     directoryArray,
	}, offset
}
