package main

// ===================================================================================

// Kafka API Versions Request (v4)

// Part 1
// 00 00 00 23  // message_size

// Part 2
// 00 12		// request_api_key
// 00 04 		// request_api_version
// 00 00 00 07  // correlation_id
// 00 09 		// api_client_id_length
// 7d 00 09 	// api_client_id_content (variable length)
// 6b 61 66
// 6b 61 2d
// 00			// api_tag_buffer

// Part 3: Client ID
// 0a 			// client_id_length
// 00 00 00 	// cliend_id_content (variable length)
// 00 00 00
// 00 00 00

// Client Software Version
// 04 			// client_software_version_length
// 00 00 00 	// client_software_version_content <- length is 1 less (variable length)
// 00 			// client_software_version_tag_buffer

// Minimal request structure (always present)
type MinimalRequest struct {
	MessageSize       int
	RequestAPIKey     int
	RequestAPIVersion int
	CorrelationID     int32
}

// Full request structure with optional fields
type FullRequest struct {
	MinimalRequest
	APIClientIDLength              int
	APIClientIDContent             []byte
	APITagBuffer                   int
	ClientIDLength                 int
	ClientIDContent                []byte
	ClientSoftwareVersionLength    int
	ClientSoftwareVersionContent   []byte
	ClientSoftwareVersionTagBuffer int
}

// ===================================================================================

// Kafka API Versions Response (v4)

// 00 00 00 21  // message_size

// Response Header V0
// 00 00 00 07  // correlation_id

// API Versions Response Body (v4)
// 00 00		// Error code
// 04			// Array Length: The length of the API Versions array + 1

// API Version #1
// 00 01		// API Key
// 00 00		// Min supported API Version
// 00 11		// Max supported API Version
// 00			// Tag Buffer

// API Version #2
// 00 12		// API Key
// 00 00		// Min supported API Version
// 00 04		// Max supported API Version
// 00 			// Tag Buffer

// API Version #3
// 00 4b		// API Key
// 00 00		// Min supported API Version
// 00 00		// Max supported API Version
// 00 			// Tag Buffer

// 00 00 00 00 	// Throttle time
// 00  			// Tag buffer

// Response structure
type Response struct {
	MessageSize   int
	CorrelationID int32
	ErrorCode     int
	ArrayLength   int
	APIVersions   []APIVersion
	ThrottleTime  int
	TagBuffer     int
}

// API Version entry for response
type APIVersion struct {
	APIKey     int
	MinVersion int
	MaxVersion int
	TagBuffer  int
}

// ===================================================================================

// Kafka DescribeTopicPartitions Request (v0)
// MINIMAL REQUEST +
// 00 09 		// topic_client_id_length
// 00 00 00		// topic_client_id_content
// 00 00 00
// 00 00 00
// 00			// tag buffer

// Describe topics paritions

// 02			// topics_array_length (The length of the topics array + 1)
// 04			// topic_name_length (The length of the topic name + 1)
// 00 00 00 	// topic_name (actual topic name)
// 00 			// topic tag buffer

// Response Patition Limit
// 00 00 00 64	// response_partition_limit (imits the number of partitions to be returned in the response)
// 00 			// cursor (A nullable field that can be used for pagination.)
// 00 			// Tag Buffer

// Topic in the DescribeTopicPartitions request
type TopicRequest struct {
	TopicNameLength int8
	TopicName       []byte
	TopicTagBuffer  int
}

// DescribeTopicPartitions Request (v0)
type DescribeTopicPartitionsRequest struct {
	MinimalRequest
	TopicClientIDLength    int
	TopicClientIDContent   []byte
	TagBuffer              int
	TopicsArrayLength      int
	Topics                 []TopicRequest
	ResponsePartitionLimit int
	Cursor                 int
	RequestTagBuffer       int
}

// ===================================================================================

// Kafka DescribeTopicPartitions Response (v0) - Unknown Topic

// 00 00 00 29  // message_size
// 00 00 00 07	// correlation_id
// 00 			// tag buffer

// Describe topic partitions

// 00 00 00 00	// throttle_time
// 02			// topics_array_length (The length of the topics array + 1)

// topic 1

// 00 03		// Error code (03 means unknown_topic)
// 04			// topic_name_length (The length of the topic name + 1)
// 00 00 00 	// topic_name (actual topic name)
// 00 			// topic tag buffer
// 00 00 00 00 	// topic_id (16 bytes)
// 00 00 00 00
// 00 00 00 00
// 00 00 00 00
// 00 			// is_internal (A boolean indicating whether the topic is internal)
// 03			// partitions_array_length (A COMPACT_ARRAY of partitions for this topic, which contains the length + 1 encoded as a varint, followed by the contents.)
//	00 00	 	// Error code 00 means no error
//	00 00 00 00	// Partition Index
//	00 00 00 01 // leader id
//  00 00 00 00 // leader epoch
//  02 			// replica_node_length
// 00 00 00 01  // replica_node A 4-byte integer representing a replica node ID
// 02			// isr nodes length
// 00 00 00 01  // A 4-byte integer representing an in-sync replica node ID.
// 01			// eligible leader replica number
// eligible leader replicas
// LastKnownELR
// offline replicas
// 00 			// tag buffer
// 00 00		// error code
// 00 00 00 01	// Partition Index
// ...
// 00 00 00 00 	// topic_auth_ops 4 byte bit field to show authorized operations for this topic.
// 00 			// tag buffer

// 00 			// next_Cursor
// 00 			// tag buffer

type Partition struct {
	ErrorCode              int16
	PartitionIndex         int32
	LeaderID               int32
	LeaderEpoch            int32
	ReplicaNodes           []int32
	ISRNodes               []int32
	EligibleLeaderReplicas []int32
	LastKnownELR           []int32
	OfflineReplicas        []int32
	TaggedFields           int8
}

type TopicResponse struct {
	ErrorCode         int16
	TopicNameLength   int8
	TopicName         []byte
	TopicTagBuffer    int8
	TopicID           [16]byte // 16 bytes UUID
	IsInternal        bool
	PartitionsArray   []Partition
	TopicAuthOps      int32 // 4 byte bit field for authorized operations
	ResponseTagBuffer int8
}

// DescribeTopicPartitions Response (v0)
type DescribeTopicPartitionsResponse struct {
	MessageSize       int32
	CorrelationID     int32
	TagBuffer         int8
	ThrottleTime      int32
	TopicsArrayLength int8
	Topics            []TopicResponse
	NextCursor        int8
	ResponseTagBuffer int8
}

// Meaning of different codes in the topic_auth_ops
// READ (bit index 3 from the right)
// WRITE (bit index 4 from the right)
// CREATE (bit index 5 from the right)
// DELETE (bit index 6 from the right)
// ALTER (bit index 7 from the right)
// DESCRIBE (bit index 8 from the right)
// DESCRIBE_CONFIGS (bit index 10 from the right)
// ALTER_CONFIGS (bit index 11 from the right)

const (
	TopicAuthOpRead            = 1 << 3  // bit index 3 from the right
	TopicAuthOpWrite           = 1 << 4  // bit index 4 from the right
	TopicAuthOpCreate          = 1 << 5  // bit index 5 from the right
	TopicAuthOpDelete          = 1 << 6  // bit index 6 from the right
	TopicAuthOpAlter           = 1 << 7  // bit index 7 from the right
	TopicAuthOpDescribe        = 1 << 8  // bit index 8 from the right
	TopicAuthOpDescribeConfigs = 1 << 10 // bit index 10 from the right
	TopicAuthOpAlterConfigs    = 1 << 11 // bit index 11 from the right
)

// Error codes
const (
	ErrorCodeNone         = 0
	ErrorCodeUnknownTopic = 3
)

// ===================================================================================
