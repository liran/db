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

func TestList(t *testing.T) {
	db, err := New("/tmp/db")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	db.Txn(func(txn *Txn) error {
		for i := 0; i < 1000; i++ {
			db.Set(txn, fmt.Sprintf("data:%d", i), i)
		}
		return nil
	})

	n := 0
	err = db.List("data:", "data:0", true, func(key string, value []byte) error {
		n++
		log.Printf("[%s] %s", key, value)
		return nil
	})
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
			db.Set(txn, fmt.Sprintf("data:%d", i), i)
		}
		return nil
	})

	err = db.ConcurrencyList("data:", 2, func(key string, value []byte) error {
		log.Printf("[%s] %s", key, value)
		return errors.New("stop")
	})
	if err != nil {
		log.Println(err)
	}
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
			err = db.Set(txn, fmt.Sprintf("data:%d", i), raw)
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

	err = db.List("data:", "", false, func(key string, value []byte) error {
		log.Println(key)
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}

	log.Println("sleep minute")
	time.Sleep(time.Minute)
}
