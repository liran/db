package db

import (
	"bytes"
	"encoding/json"

	"github.com/pkg/errors"
	bolt "go.etcd.io/bbolt"
)

type Txn struct {
	t *bolt.Tx
}

func (txn *Txn) Set(key string, value any) error {
	bucket := GetBucket(key)
	b, err := txn.t.CreateBucketIfNotExists([]byte(bucket))
	if err != nil {
		return err
	}
	b.FillPercent = 1.0

	raw := ToBytes(value)
	if len(raw) > 0 {
		compressed, err := GzipCompress(raw)
		if err != nil || len(compressed) > len(raw) {
			compressed = raw
		}
		return b.Put([]byte(key), compressed)
	}
	return b.Put([]byte(key), nil)
}

func (txn *Txn) Get(key string) ([]byte, error) {
	bucket := GetBucket(key)
	b := txn.t.Bucket([]byte(bucket))
	if b == nil {
		return nil, ErrKeyNotFound
	}

	val := b.Get([]byte(key))
	if val == nil {
		return nil, ErrKeyNotFound
	}

	decode, err := GzipUncompress(val)
	if err != nil {
		decode = nil
		decode = append(decode, val...)
	}
	return decode, nil
}

func (txn *Txn) Has(key string) bool {
	bucket := GetBucket(key)
	b := txn.t.Bucket([]byte(bucket))
	if b == nil {
		return false
	}

	item := b.Get([]byte(key))
	return item != nil
}

func (txn *Txn) Del(key string) error {
	bucket := GetBucket(key)
	b, err := txn.t.CreateBucketIfNotExists([]byte(bucket))
	if err != nil {
		return err
	}

	return b.Delete([]byte(key))
}

func (txn *Txn) Unmarshal(key string, value any) error {
	raw, err := txn.Get(key)
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
func (txn *Txn) Inc(key string, step int64) (int64, error) {
	var val int64
	err := txn.Unmarshal(key, &val)
	if err != nil && !errors.Is(err, ErrKeyNotFound) {
		return 0, err
	}
	val += step
	return val, txn.Set(key, val)
}

// return new value
func (txn *Txn) Dec(key string, step int64) (int64, error) {
	var val int64
	err := txn.Unmarshal(key, &val)
	if err != nil && !errors.Is(err, ErrKeyNotFound) {
		return 0, err
	}
	val -= step
	return val, txn.Set(key, val)
}

func (txn *Txn) List(prefix string, fn func(key string, value []byte) (stop bool, err error), options ...*ListOption) error {
	beginKey := ""
	containBegin := false
	reverse := false
	limit := 0
	keyOnly := false
	if len(options) > 0 {
		opt := options[0]
		beginKey = opt.Begin
		containBegin = opt.ContainBegin
		reverse = opt.Reverse
		limit = opt.Limit
		keyOnly = opt.KeyOnly
	}

	bucket := GetBucket(prefix)
	b := txn.t.Bucket([]byte(bucket))
	if b == nil {
		return nil
	}

	c := b.Cursor()
	it := func() (key []byte, value []byte) {
		if reverse {
			return c.Prev()
		}
		return c.Next()
	}

	bytePrefix := []byte(prefix)

	var k []byte
	var v []byte
	if beginKey != "" {
		k, v = c.Seek([]byte(beginKey))
		// skip to next
		if k != nil && !containBegin {
			k, v = it()
		}
	} else {
		k, v = c.Seek(bytePrefix)
	}

	for i := 0; bytes.HasPrefix(k, bytePrefix); k, v = it() {
		var val []byte
		if !keyOnly {
			decode, err := GzipUncompress(v)
			if err != nil {
				decode = nil
				val = append(val, v...)
			} else {
				val = decode
			}
		}
		if b, err := fn(string(k), val); err != nil || b {
			return err
		}

		i++
		if limit > 0 && i >= limit {
			break
		}
	}
	return nil
}
