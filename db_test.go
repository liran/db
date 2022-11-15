package db

import (
	"errors"
	"fmt"
	"log"
	"os"
	"testing"
	"time"
)

func TestAll(t *testing.T) {
	db, err := New("/tmp/db")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	db.Txn(func(txn *Txn) error {
		n, _ := txn.Inc("a", 1)
		if n != 1 {
			t.Fatal("not expected")
		}
		return nil
	})

	err = db.Txn(func(txn *Txn) error {
		_, err := txn.Get("b")
		return err
	})
	if !errors.Is(err, ErrKeyNotFound) {
		t.Fatal("not expected")
	}
}

func TestList(t *testing.T) {
	db, err := New("/tmp/db")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	db.Txn(func(txn *Txn) error {
		for i := 0; i < 1000; i++ {
			txn.Set(fmt.Sprintf("data:%d", i), i)
		}
		return nil
	})

	n := 0
	err = db.Txn(func(txn *Txn) error {
		return txn.List("data:", "data:0", func(key string, value []byte) error {
			n++
			log.Printf("[%s] %s", key, value)
			return nil
		})
	}, true)
	if err != nil {
		t.Fatal(err)
	}
	log.Println(n)
}

func TestStream(t *testing.T) {
	db, err := New("/tmp/db")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	db.Txn(func(txn *Txn) error {
		for i := 0; i < 1000; i++ {
			txn.Set(fmt.Sprintf("data:%d", i), i)
		}
		return nil
	})
}

func TestListPerformance(t *testing.T) {
	db, err := New("/tmp/db")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	raw, err := os.ReadFile("go.mod")
	if err != nil {
		t.Fatal(err)
	}

	for i := 0; i < 100*10000; i++ {
		err = db.Txn(func(txn *Txn) error {
			err = txn.Set(fmt.Sprintf("data:%d", i), raw)
			if err != nil {
				return err
			}
			log.Printf("writen: %d", i)
			return nil
		})
		if err != nil {
			t.Fatal(err)
		}
	}

	err = db.Txn(func(txn *Txn) error {
		return txn.List("data:", "", func(key string, value []byte) error {
			log.Println(key)
			return nil
		})
	}, true)
	if err != nil {
		t.Fatal(err)
	}

	log.Println("sleep minute")
	time.Sleep(time.Minute)
}
