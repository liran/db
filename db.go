package db

import (
	bolt "go.etcd.io/bbolt"
)

type DB struct {
	db *bolt.DB
}

// or set env: DATABASE_DIR
func New(dir string, readOnly bool) (*DB, error) {
	opts := bolt.DefaultOptions
	opts.ReadOnly = readOnly
	db, err := bolt.Open(dir, 0666, opts)
	if err != nil {
		return nil, err
	}

	return &DB{db: db}, nil
}

func (t *DB) Close() {
	if t.db != nil {
		t.db.Close()
		t.db = nil
	}
}

func (t *DB) Txn(fn func(txn *Txn) error, readOnly ...bool) error {
	cb := func(tx *bolt.Tx) error {
		return fn(&Txn{t: tx})
	}
	if len(readOnly) > 0 && readOnly[0] {
		return t.db.View(cb)
	}
	return t.db.Batch(cb)
}

func (t *DB) List(prefix string, fn func(key string, value []byte) (stop bool, err error), options ...*ListOption) error {
	return t.Txn(func(txn *Txn) error {
		return txn.List(prefix, fn, options...)
	}, true)
}
