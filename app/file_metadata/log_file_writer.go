package file_metadata

import (
	"encoding/hex"
	"fmt"
	"os"
	"path/filepath"
)

func write_bin(path string, hexData string) {
	// Decode hex string into bytes
	data, err := hex.DecodeString(hexData)
	if err != nil {
		fmt.Println("Error in bin decode to hex")
	}

	// Ensure parent directories exist
	if err := os.MkdirAll(filepath.Dir(path), 0755); err != nil {
		panic(err)
	}

	// Write file (creates if not exists, overwrites if it does)
	if err := os.WriteFile(path, data, 0644); err != nil {
		panic(err)
	}
	fmt.Println("Wrote", len(data), "bytes to .log file")
}
