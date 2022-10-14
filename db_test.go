package db

import (
	"errors"
	"testing"
)

func TestAll(t *testing.T) {
	db, err := New("/tmp/db")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	db.Txn(func(txn *Txn) error {
		n, _ := db.Inc(txn, "a", 1)
		if n != 1 {
			t.Fatal("not expected")
		}
		return nil
	})

	err = db.Txn(func(txn *Txn) error {
		_, err := db.Get(txn, "b")
		return err
	})
	if !errors.Is(err, ErrKeyNotFound) {
		t.Fatal("not expected")
	}
}
