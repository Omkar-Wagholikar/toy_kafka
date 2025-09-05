package main

import (
	"encoding/hex"
	"fmt"
	"io"
	"net"
	"os"
	"testing"
	"time"
)

func TestServerHandlesHardcodedRequest(t *testing.T) {
	// Start the server in a goroutine
	go func() {
		main()
	}()

	// Give the server time to start
	time.Sleep(200 * time.Millisecond)

	// Connect to the server like a client
	conn, err := net.Dial("tcp", "127.0.0.1:9092")
	if err != nil {
		t.Fatalf("Failed to connect to server: %v", err)
	}
	defer conn.Close()

	// Hardcoded request in hex
	hexInput := "00000031004b00002a5d9747000c6b61666b612d746573746572000212756e6b6e6f776e2d746f7069632d70617a0000000001ff00"

	expectedHexOutput := "0000003d2a5d9747000000000002000312756e6b6e6f776e2d746f7069632d70617a00000000000000000000000000000000000000000000000001000000000000"
	// hexInput := "00000031004b000070d12963000c6b61666b612d746573746572000212756e6b6e6f776e2d746f7069632d73617a0000000001ff00"

	data, err := hex.DecodeString(hexInput)
	if err != nil {
		t.Fatalf("Failed to decode hex input: %v", err)
	}

	// _, err = deserializeDescribeTopicPartitionsRequest(data)
	// if err != nil {
	// 	t.Fatalf("Failed to serialize request: %s", err.Error())
	// }

	// fmt.Println()
	// fmt.Println("REQUEST >")
	// printDescribeTopicRequest(request)
	// fmt.Println()

	// Send request to server
	_, err = conn.Write(data)
	if err != nil {
		t.Fatalf("Failed to write to server: %v", err)
	}

	// Read response
	buf := make([]byte, 1024)
	n, err := conn.Read(buf)
	if err != nil && err != io.EOF {
		t.Fatalf("Failed to read from server: %v", err)
	}

	actualHexOutput := hex.EncodeToString(buf[:n])
	// t.Logf("\n\nResponse (hex): %s\n\n", actualHexOutput)
	_, err = deserializeDescribeTopicPartitionsResponse(buf[:n])
	// val, err := deserializeDescribeTopicPartitionsResponse(buf[:n])

	if err != nil {
		fmt.Println("THERE WAS AN ERROR! ", err.Error())
	} else {
		// fmt.Println()
		// fmt.Println("RESPONSE >")
		// printDescribeTopicResponse(val)
		// fmt.Println()
	}

	if expectedHexOutput != actualHexOutput {
		fmt.Println("FAILED TEST")
		os.Exit(1)
	}
}

func TestServerHandlesPartitionRequest(t *testing.T) {
	// Start the server in a goroutine
	go func() {
		main()
	}()

	// Give the server time to start
	time.Sleep(200 * time.Millisecond)

	// Connect to the server like a client
	conn, err := net.Dial("tcp", "127.0.0.1:9092")
	if err != nil {
		t.Fatalf("Failed to connect to server: %v", err)
	}
	defer conn.Close()

	// Hardcoded request in hex
	hexInput := "00000023004b00007ff21070000c6b61666b612d74657374657200020462617a0000000001ff00"

	expectedHexOutput := "0000003d2a5d9747000000000002000312756e6b6e6f776e2d746f7069632d70617a00000000000000000000000000000000000000000000000001000000000000"
	// hexInput := "00000031004b000070d12963000c6b61666b612d746573746572000212756e6b6e6f776e2d746f7069632d73617a0000000001ff00"

	data, err := hex.DecodeString(hexInput)
	if err != nil {
		t.Fatalf("Failed to decode hex input: %v", err)
	}

	request, err := deserializeDescribeTopicPartitionsRequest(data)
	if err != nil {
		t.Fatalf("Failed to serialize request: %s", err.Error())
	}

	fmt.Println()
	fmt.Println("REQUEST >")
	printDescribeTopicRequest(request)
	fmt.Println()

	// Send request to server
	_, err = conn.Write(data)
	if err != nil {
		t.Fatalf("Failed to write to server: %v", err)
	}

	// Read response
	buf := make([]byte, 1024)
	n, err := conn.Read(buf)
	if err != nil && err != io.EOF {
		t.Fatalf("Failed to read from server: %v", err)
	}

	actualHexOutput := hex.EncodeToString(buf[:n])
	// t.Logf("\n\nResponse (hex): %s\n\n", actualHexOutput)
	val, err := deserializeDescribeTopicPartitionsResponse(buf[:n])

	if err != nil {
		fmt.Println("THERE WAS AN ERROR! ", err.Error())
	} else {
		fmt.Println()
		fmt.Println("RESPONSE >")
		printDescribeTopicResponse(val)
		fmt.Println()
	}

	if expectedHexOutput != actualHexOutput {
		fmt.Println("FAILED TEST")
		os.Exit(1)
	}
}
