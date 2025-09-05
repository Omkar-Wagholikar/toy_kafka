package file_metadata

import (
	"encoding/hex"
)

func DeSerializeFile(path string) string {
	val := hex.EncodeToString(ReadBin(path))
	return val
}
