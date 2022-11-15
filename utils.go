package db

import (
	"bytes"
	"compress/gzip"
	"encoding/json"
	"io"
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

func GzipCompress(src []byte) ([]byte, error) {
	var b bytes.Buffer
	w := gzip.NewWriter(&b)
	_, err := w.Write(src)
	if err != nil {
		return nil, err
	}
	err = w.Close()
	if err != nil {
		return nil, err
	}
	return b.Bytes(), nil
}

func GzipUncompress(src []byte) ([]byte, error) {
	zr, err := gzip.NewReader(bytes.NewReader(src))
	if err != nil {
		return nil, err
	}
	return io.ReadAll(zr)
}
