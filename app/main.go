package main

import (
	"bytes"
	"fmt"
	"io"
	"net"
	"os"
)

func main() {
	fmt.Println("Logs from your program will appear here!")

	l, err := net.Listen("tcp", "0.0.0.0:9092")
	if err != nil {
		fmt.Println("Failed to bind to port 9092")
		os.Exit(1)
	}

	fmt.Println("Start listening on 9092")

	for {
		// Keep looping and waiting/ accepting connections
		conn, err := l.Accept()
		fmt.Println("Accepted connection")
		if err != nil {
			fmt.Println("Error accepting connection: ", err.Error())
			os.Exit(1)
		}
		// Spawn a go routine/ thread for each connection that is accepted
		go handleConnection(conn)
	}
}

func handleConnection(conn net.Conn) {
	fmt.Println("Handling connection")

	defer conn.Close()

	for {
		received := bytes.Buffer{}
		buff := make([]byte, 1024)

		n, err := conn.Read(buff)
		if err != nil {
			if err == io.EOF {
				break // client closed connection
			}
			fmt.Println("Read error:", err)
			return
		}

		received.Write(buff[:n])
		// request := request_decoder.DecodeRequest(received.Bytes())
		// response := handler.HandlerRequest(request)

		fmt.Println("Writing response")
		conn.Write([]byte{0, 0, 0, 0, 0, 0, 0, 7})
	}
}
