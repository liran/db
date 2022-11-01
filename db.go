package db

import (
	"encoding/json"
	"os"
	"sync"
	"time"

	badger "github.com/dgraph-io/badger/v3"
	"github.com/pkg/errors"
)

var ErrKeyNotFound = badger.ErrKeyNotFound

type Txn struct {
	*badger.Txn
}

type DB struct {
	db *badger.DB
	m  sync.Mutex
}

// or set env: DATABASE_DIR
func New(dirs ...string) (*DB, error) {
	var dir string
	if len(dirs) > 0 {
		dir = dirs[0]
	}
	if dir == "" {
		dir = os.Getenv("DATABASE_DIR")
	}
	if dir == "" {
		dir = "/tmp/db"
	}
	db := &DB{}
	return db, db.open(dir)
}

func (t *DB) open(dir string) error {
	opts := badger.DefaultOptions(dir)
	opts = opts.WithLoggingLevel(badger.WARNING)
	db, err := badger.Open(opts)
	if err != nil {
		return err
	}
	t.db = db

	// garbage recycling
	go func() {
		ticker := time.NewTicker(5 * time.Minute)
		defer ticker.Stop()

		clear := func() (end, terminate bool) {
			t.m.Lock()
			defer t.m.Unlock()

			if t.db == nil || t.db.IsClosed() {
				return true, true
			}

			return t.db.RunValueLogGC(0.7) != nil, false
		}

		for range ticker.C {
			for {
				end, terminate := clear()
				if terminate {
					return
				}
				if end {
					break
				}
			}
		}
	}()

	return nil
}

func (t *DB) Close() {
	t.m.Lock()
	defer t.m.Unlock()

	if t.db != nil {
		t.db.Close()
		t.db = nil
	}
}

func (t *DB) Set(txn *Txn, key string, value any) error {
	return txn.Set([]byte(key), ToBytes(value))
}

func (t *DB) Get(txn *Txn, key string) ([]byte, error) {
	var result []byte
	item, err := txn.Get([]byte(key))
	if err != nil {
		return nil, err
	}
	err = item.Value(func(val []byte) error {
		result = append(result, val...)
		return nil
	})
	if err != nil {
		return nil, err
	}
	return result, nil
}

func (t *DB) Has(txn *Txn, key string) (bool, error) {
	_, err := txn.Get([]byte(key))
	if err != nil {
		if errors.Is(err, ErrKeyNotFound) {
			return false, nil
		}
		return false, err
	}
	return true, nil
}

func (t *DB) Del(txn *Txn, key string) error {
	err := txn.Delete([]byte(key))
	if err != nil && !errors.Is(err, ErrKeyNotFound) {
		return err
	}
	return nil
}

func (t *DB) Unmarshal(txn *Txn, key string, value any) error {
	raw, err := t.Get(txn, key)
	if err != nil {
		return errors.Wrapf(err, "read item, key: %s", key)
	}
	err = json.Unmarshal(raw, value)
	if err != nil {
		return errors.Wrapf(err, "unmarshal, key: %s, raw: %s", key, raw)
	}
	return nil
}

func (t *DB) List(txn *Txn, prefix string, beginKey string, keyOnly bool, fn func(key string, value []byte) (continue_ bool)) error {
	opts := badger.DefaultIteratorOptions
	opts.PrefetchValues = !keyOnly
	it := txn.NewIterator(opts)
	defer it.Close()

	dataValid := beginKey == ""

	prefixBytes := []byte(prefix)
	for it.Seek(prefixBytes); it.ValidForPrefix(prefixBytes); it.Next() {
		item := it.Item()

		k := string(item.Key())

		if !dataValid {
			dataValid = beginKey == k
			continue
		}

		if keyOnly {
			if !fn(k, nil) {
				return nil
			}
			continue
		}

		raw, err := item.ValueCopy(nil)
		if err != nil {
			return err
		}
		if !fn(k, raw) {
			return nil
		}
	}
	return nil
}

// return new value
func (t *DB) Inc(txn *Txn, key string, step int64) (int64, error) {
	var val int64
	err := t.Unmarshal(txn, key, &val)
	if err != nil && !errors.Is(err, ErrKeyNotFound) {
		return 0, err
	}
	val += step
	return val, t.Set(txn, key, val)
}

func (t *DB) Dec(txn *Txn, key string, step int64) (int64, error) {
	var val int64
	err := t.Unmarshal(txn, key, &val)
	if err != nil && !errors.Is(err, ErrKeyNotFound) {
		return 0, err
	}
	val -= step
	return val, t.Set(txn, key, val)
}

func (t *DB) Txn(fn func(txn *Txn) error, readOnly ...bool) error {
	cb := func(t *badger.Txn) error {
		return fn(&Txn{Txn: t})
	}
	if len(readOnly) > 0 && readOnly[0] {
		return t.db.View(cb)
	}
	return t.db.Update(cb)
}

func (t *DB) ConflictRetryTxn(fn func(txn *Txn) error) error {
	for {
		err := t.Txn(fn)
		if err != nil && errors.Is(err, badger.ErrConflict) {
			continue
		}
		return err
	}
}
