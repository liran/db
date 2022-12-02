package db

import (
	"context"
	"errors"
	"fmt"
	"log"
	"testing"
	"time"
)

func TestAll(t *testing.T) {
	db, err := New("/tmp/db", false)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	db.Txn(func(txn *Txn) error {
		err = txn.Del("aaa")
		if err != nil {
			return err
		}
		if txn.Has("a") {
			txn.Del("a")
		}
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
	db, err := New("/tmp/db", false)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	db.Txn(func(txn *Txn) error {
		for i := 0; i < 1000; i++ {
			txn.Set(fmt.Sprintf("data:%d", i), i)
		}
		txn.Set("test:1", 1)
		return nil
	})

	n := 0
	err = db.Txn(func(txn *Txn) error {
		return txn.List("test:", "", false, func(key string, value []byte) error {
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

func TestRW(t *testing.T) {
	db, err := New("/tmp/db", false)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	err = db.Txn(func(txn *Txn) error {
		return txn.Set("bool", true)
	})
	if err != nil {
		t.Fatal(err)
	}

	err = db.Txn(func(txn *Txn) error {
		raw, err := txn.Get("bool")
		if err != nil {
			return err
		}
		log.Printf("%s", raw)

		sss := false
		err = txn.Unmarshal("bool", &sss)
		if err != nil {
			return err
		}
		log.Printf("%v", sss)

		return nil
	}, true)
	if err != nil {
		t.Fatal(err)
	}
}

func TestNotFound(t *testing.T) {
	db, err := New("/tmp/db", false)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	err = db.Txn(func(txn *Txn) error {
		_, err := txn.Get("not_found")
		return err
	}, true)
	if err != nil {
		t.Fatal(err)
	}
}

func TestConflict(t *testing.T) {
	db, err := New("/tmp/db", false)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	for i := 0; i < 5; i++ {
		go func(index int) {
			for {
				select {
				case <-ctx.Done():
					return
				default:
				}

				db.Txn(func(txn *Txn) error {
					val, _ := txn.Get("kk")
					log.Printf("[%d] %s", index, val)
					return txn.Set("kk", index)
				})
			}
		}(i)
	}
	<-ctx.Done()
}
