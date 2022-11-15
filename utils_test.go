package db

import (
	"bytes"
	"log"
	"testing"
)

func TestCompress(t *testing.T) {
	src := []byte("hello world!")
	dst, err := GzipCompress(src)
	if err != nil {
		t.Fatal(err)
	}
	raw, err := GzipUncompress(dst)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(src, raw) {
		t.Fatal("GzipUncompress !Equal")
	}
	log.Printf("[%d] [%d] %s", len(src), len(dst), raw)
}
