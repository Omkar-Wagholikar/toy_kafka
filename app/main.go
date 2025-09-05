package main

import (
	"fmt"
	"net"
	"os"
	"toy_kafka/app/file_metadata"
)

var global_metadata file_metadata.ClusterMetaData

func main() {
	path := "/tmp/kraft-combined-logs/__cluster_metadata-0/00000000000000000000.log"
	// file_metadata.CreateAndPopulateLog(path)
	stream := file_metadata.ReadBin(path)
	global_metadata, _ := file_metadata.CreateClusterMetaData(stream)
	file_metadata.PrettyPrintClusterMetaData(*global_metadata)

	fmt.Println("Logs from your program will appear here!")

	l, err := net.Listen("tcp", "0.0.0.0:9092")
	if err != nil {
		fmt.Println("Failed to bind to port 9092")
		os.Exit(1)
	}

	fmt.Println("Start listening on 9092")

	for {
		conn, err := l.Accept()
		if err != nil {
			fmt.Println("Error accepting connection: ", err.Error())
			os.Exit(1)
		}
		go handleConnection(conn)
	}
}

// echo -n "00000031004b00000bcefe56000c6b61666b612d746573746572000212756e6b6e6f776e2d746f7069632d71757a0000000001ff00"  | xxd -r -p | nc localhost 9092 | hexdump -C

//  My response: 000000380bcefe56000000000002000312756e6b6e6f776e2d746f7069632d71757a0000000000000000000000000000000000000001000000000000

// (Ideally) correct response: 0000003d0bcefe56000000000002000312756e6b6e6f776e2d746f7069632d71757a00000000000000000000000000000000000001000000000001000000000000

// echo -n "00000031004b00002a5d9747000c6b61666b612d746573746572000212756e6b6e6f776e2d746f7069632d70617a0000000001ff00" | xxd -r -p | nc localhost 9092 | hexdump -C
// echo -n "00000020004b00000000000700096b61666b612d636c69000204666f6f0000000064ff00" | xxd -r -p | nc localhost 9092 | hexdump -C
