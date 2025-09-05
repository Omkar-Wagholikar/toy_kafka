package file_metadata

import (
	"fmt"
	"os"
)

func ReadBin(path string) []byte {
	file, err := os.Open(path)
	if err != nil {
		fmt.Println("File ops error: ", err.Error())
		os.Exit(1)
	} else {
		defer file.Close()
		buffer := make([]byte, 4096) // 4KB buffer
		n, err := file.Read(buffer)
		if err != nil {
			fmt.Println("File read error")
			os.Exit(1)
		} else {
			buffer = buffer[:n]
			return buffer
			// file_str := hex.EncodeToString(buffer)
			// return file_str
		}
	}
	os.Exit(1)
	return nil
}
