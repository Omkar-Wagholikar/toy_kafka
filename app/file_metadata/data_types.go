package file_metadata

import "github.com/google/uuid"

type ClusterMetaData struct {
	Batches []RecordBatch
}

type RecordBatch struct {
	baseOffset           int64
	partitionLeaderEpoch int32
	magicByte            int8
	CRC                  int32
	attributes           int16
	lastOffsetDelta      int32
	baseTimestamp        int64
	maxTimestamp         int64
	producerId           int64
	producerEpoch        int16
	baseSequence         int32 // do we need this?
	Records              []Record
}

type Record struct {
	attributes     int8
	timestampDelta int64
	keySize        int64
	ValueType      int8
	key            interface{} // variable sized integer?
	Value          interface{} // should be one of
}

type ValueTypeHeader struct {
	frameVersion int8
	valueType    int8
	version      int8
}

type FeatureLevelValue struct {
	header       ValueTypeHeader
	Name         string
	featureLevel int16
}

type TopicValue struct {
	header    ValueTypeHeader
	TopicName string
	topicId   uuid.UUID
}

type PartitionValue struct {
	header               ValueTypeHeader
	partitionId          int32
	topicId              uuid.UUID
	replicaIdArray       []int32
	inSyncReplicaArray   []int32
	removingReplicaArray []int32
	addingReplicaArray   []int32
	leaderId             int32
	leaderEpoch          int32
	partitionEpoch       int32
	directoriesArray     []uuid.UUID
}
