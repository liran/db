package db

import (
	"encoding/json"
)

func ToBytes(data any) []byte {
	var value []byte
	switch v := data.(type) {
	case []byte:
		value = v
	case string: // Prevent repeated double quotes in the string
		value = []byte(v)
	default:
		value, _ = json.Marshal(data)
	}
	return value
}
