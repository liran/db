package db

import (
	"encoding/json"
	"os"
	"strconv"
	"time"

	badger "github.com/dgraph-io/badger/v3"
	"github.com/pkg/errors"
)

type DB struct {
	db  *badger.DB
	dir string
}

func NewDB(dirs ...string) *DB {
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
	return &DB{dir: dir}
}

func (t *DB) Open() error {
	if t.db != nil {
		return errors.New("duplicate database instance")
	}

	db, err := badger.Open(badger.DefaultOptions(t.dir))
	if err != nil {
		return err
	}
	t.db = db

	// garbage recycling
	go func() {
		ticker := time.NewTicker(5 * time.Minute)
		defer ticker.Stop()
		for range ticker.C {
		again:
			if t.db == nil || db.IsClosed() {
				return
			}
			err := db.RunValueLogGC(0.7)
			if err == nil {
				goto again
			}
		}
	}()

	return nil
}

func (t *DB) Close() {
	if t.db != nil {
		t.db.Close()
		t.db = nil
	}
}

func (t *DB) DB() *badger.DB {
	return t.db
}

func (t *DB) BytesToInt64(b []byte) int64 {
	n, err := strconv.ParseInt(string(b), 10, 64)
	if err != nil {
		return 0
	}
	return n
}

func (t *DB) ToBytes(data any) []byte {
	var value []byte
	switch v := data.(type) {
	case string: // Prevent repeated double quotes in the string
		value = []byte(v)
	default:
		value, _ = json.Marshal(data)
	}
	return value
}

func (t *DB) ReadItem(txn *badger.Txn, key string) ([]byte, error) {
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

func (t *DB) LoadValue(txn *badger.Txn, key string, value any) error {
	raw, err := t.ReadItem(txn, key)
	if err != nil {
		return errors.Wrapf(err, "read item, key: %s", key)
	}
	err = json.Unmarshal(raw, value)
	if err != nil {
		return errors.Wrapf(err, "unmarshal, key: %s, raw: %s", key, raw)
	}
	return nil
}

func (t *DB) IncreaseValue(txn *badger.Txn, key string) (newVal int64, err error) {
	item, err := t.ReadItem(txn, key)
	if err != nil {
		if err == badger.ErrKeyNotFound {
			newVal = 1
		} else {
			return
		}
	} else {
		newVal = t.BytesToInt64(item) + 1
	}
	err = txn.Set([]byte(key), t.ToBytes(newVal))
	return
}

func (t *DB) DecreaseValue(txn *badger.Txn, key string) (newVal int64, err error) {
	item, err := t.ReadItem(txn, key)
	if err != nil {
		if err == badger.ErrKeyNotFound {
			newVal = 0
		} else {
			return
		}
	} else {
		newVal = t.BytesToInt64(item) - 1
	}
	err = txn.Set([]byte(key), t.ToBytes(newVal))
	return
}
