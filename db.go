package db

import (
	bolt "go.etcd.io/bbolt"
)

type DB struct {
	db     *bolt.DB
	bucket []byte
}

// or set env: DATABASE_DIR
func New(dir string, readOnly bool) (*DB, error) {
	opts := bolt.DefaultOptions
	opts.ReadOnly = readOnly
	db, err := bolt.Open(dir, 0666, opts)
	if err != nil {
		return nil, err
	}

	// create a default bucket
	bucket := []byte("d")

	if !readOnly {
		err = db.Update(func(tx *bolt.Tx) error {
			_, err := tx.CreateBucketIfNotExists(bucket)
			return err
		})
		if err != nil {
			return nil, err
		}
	}

	return &DB{db: db, bucket: bucket}, nil
}

func (t *DB) Close() {
	if t.db != nil {
		t.db.Close()
		t.db = nil
		t.bucket = nil
	}
}

func (t *DB) Txn(fn func(txn *Txn) error, readOnly ...bool) error {
	cb := func(tx *bolt.Tx) error {
		b := tx.Bucket(t.bucket)
		b.FillPercent = 1.0
		return fn(&Txn{b: b})
	}
	if len(readOnly) > 0 && readOnly[0] {
		return t.db.View(cb)
	}
	return t.db.Update(cb)
}
