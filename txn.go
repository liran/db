package db

import (
	"bytes"
	"encoding/json"

	"github.com/pkg/errors"
	bolt "go.etcd.io/bbolt"
)

type Txn struct {
	b *bolt.Bucket
}

func (t *Txn) Set(key string, value any) error {
	raw := ToBytes(value)
	compressed, err := GzipCompress(raw)
	if err != nil || len(compressed) > len(raw) {
		compressed = raw
	}
	return t.b.Put([]byte(key), compressed)
}

func (t *Txn) Get(key string) ([]byte, error) {
	val := t.b.Get([]byte(key))
	if val == nil {
		return nil, ErrKeyNotFound
	}

	decode, err := GzipUncompress(val)
	if err != nil {
		decode = val
	}
	return decode, nil
}

func (t *Txn) Has(key string) bool {
	item := t.b.Get([]byte(key))
	return item != nil
}

func (t *Txn) Del(key string) error {
	return t.b.Delete([]byte(key))
}

func (t *Txn) Unmarshal(key string, value any) error {
	raw, err := t.Get(key)
	if err != nil {
		return errors.Wrapf(err, "read item, key: %s", key)
	}
	err = json.Unmarshal(raw, value)
	if err != nil {
		return errors.Wrapf(err, "unmarshal, key: %s, raw: %s", key, raw)
	}
	return nil
}

// return new value
func (t *Txn) Inc(key string, step int64) (int64, error) {
	var val int64
	err := t.Unmarshal(key, &val)
	if err != nil && !errors.Is(err, ErrKeyNotFound) {
		return 0, err
	}
	val += step
	return val, t.Set(key, val)
}

// return new value
func (t *Txn) Dec(key string, step int64) (int64, error) {
	var val int64
	err := t.Unmarshal(key, &val)
	if err != nil && !errors.Is(err, ErrKeyNotFound) {
		return 0, err
	}
	val -= step
	return val, t.Set(key, val)
}

func (t *Txn) List(prefix string, beginKey string, keyOnly bool, fn func(key string, value []byte) error) error {
	c := t.b.Cursor()

	bytePrefix := []byte(prefix)

	var k []byte
	var v []byte
	if beginKey != "" {
		k, v = c.Seek([]byte(beginKey))
		// skip to next
		if k != nil {
			k, v = c.Next()
		}
	} else {
		k, v = c.Seek(bytePrefix)
	}

	for ; bytes.HasPrefix(k, bytePrefix); k, v = c.Next() {
		var val []byte
		if !keyOnly {
			decode, err := GzipUncompress(v)
			if err != nil {
				val = v
			} else {
				val = decode
			}
		}
		if err := fn(string(k), val); err != nil {
			if errors.Is(err, ErrStopIterate) {
				err = nil
			}
			return err
		}
	}
	return nil
}
