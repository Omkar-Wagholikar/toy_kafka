package file_metadata

import (
	"fmt"
)

// PrettyPrintClusterMetaData prints a ClusterMetaData structure in a readable format.
func PrettyPrintClusterMetaData(cm ClusterMetaData) {
	fmt.Println("=== ClusterMetaData ===")
	for i, batch := range cm.Batches {
		fmt.Printf("Batch %d:\n", i)
		fmt.Printf("  BaseOffset: %d\n", batch.baseOffset)
		fmt.Printf("  PartitionLeaderEpoch: %d\n", batch.partitionLeaderEpoch)
		fmt.Printf("  MagicByte: %d\n", batch.magicByte)
		fmt.Printf("  CRC: %d\n", batch.CRC)
		fmt.Printf("  Attributes: %d\n", batch.attributes)
		fmt.Printf("  LastOffsetDelta: %d\n", batch.lastOffsetDelta)
		fmt.Printf("  BaseTimestamp: %d\n", batch.baseTimestamp)
		fmt.Printf("  MaxTimestamp: %d\n", batch.maxTimestamp)
		fmt.Printf("  ProducerId: %d\n", batch.producerId)
		fmt.Printf("  ProducerEpoch: %d\n", batch.producerEpoch)
		fmt.Printf("  BaseSequence: %d\n", batch.baseSequence)

		for j, record := range batch.Records {
			fmt.Printf("  Record %d:\n", j)
			fmt.Printf("    Attributes: %d\n", record.attributes)
			fmt.Printf("    TimestampDelta: %d\n", record.timestampDelta)
			fmt.Printf("    KeySize: %d\n", record.keySize)
			fmt.Printf("    ValueType: %d\n", record.ValueType)
			fmt.Printf("    Key: %v\n", record.key)
			fmt.Printf("    Value: %v\n", record.Value)
		}
	}
}
