package db

import (
	"bytes"
	"compress/gzip"
	"encoding/json"
	"fmt"
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
		// no encode html tag
		buffer := &bytes.Buffer{}
		encoder := json.NewEncoder(buffer)
		encoder.SetEscapeHTML(false)
		encoder.Encode(data)
		value = buffer.Bytes()
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

func PaddingZero(val any, length int) string {
	text := fmt.Sprintf("%v", val)
	diff := length - len(text)
	if diff <= 0 {
		return text
	}

	var b bytes.Buffer
	for i := 0; i < diff; i++ {
		b.WriteString("0")
	}
	b.WriteString(text)
	return b.String()
}
