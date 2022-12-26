package db

import (
	"context"
	"errors"
	"fmt"
	"log"
	"os"
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
		return txn.List("test:", func(key string, value []byte) (bool, error) {
			n++
			log.Printf("[%s] %s", key, value)
			return false, nil
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

func TestReadOnly(t *testing.T) {
	log.SetFlags(log.Ldate | log.Ltime | log.Lshortfile)

	go func() {
		db, err := New("/tmp/db", false)
		if err != nil {
			log.Fatal(err)
		}
		defer db.Close()

		err = db.Txn(func(txn *Txn) error {
			return txn.Set("a", "1")
		})
		if err != nil {
			log.Fatal(err)
		}

		log.Println("sleep 10s")
		time.Sleep(10 * time.Second)
	}()

	log.Println("sleep 1s")
	time.Sleep(time.Second)

	db, err := New("/tmp/db", true)
	log.Println("open db read only")
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	err = db.Txn(func(txn *Txn) error {
		a, err := txn.Get("a")
		if err != nil {
			return err
		}

		log.Printf("a: %s", a)
		return nil
	}, true)
	if err != nil {
		log.Fatal(err)
	}
}

func BenchmarkUpdate(b *testing.B) {
	db, err := New("/tmp/db", false)
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	raw, _ := os.ReadFile("test.json")

	for i := 0; i < b.N; i++ {
		db.Txn(func(txn *Txn) error {
			return txn.Set("CA500000211380900", raw)
		})
	}
}

// func BenchmarkBatch(b *testing.B) {
// 	db, err := New("/tmp/db", false)
// 	if err != nil {
// 		log.Fatal(err)
// 	}
// 	defer db.Close()

// 	raw, _ := os.ReadFile("test.json")

// 	for i := 0; i < b.N; i++ {
// 		db.Batch(func(txn *Txn) error {
// 			return txn.Set("CA500000211380900", raw)
// 		})
// 	}
// }

func UpdateWrite100Times(db *DB, data []byte) {
	for i := 0; i < 100; i++ {
		db.Txn(func(txn *Txn) error {
			return txn.Set("CA500000211380900", data)
		})
	}
}

// func BatchWrite100Times(db *DB, data []byte) {
// 	var wg sync.WaitGroup
// 	wg.Add(100)
// 	for i := 0; i < 100; i++ {
// 		go func() {
// 			defer wg.Done()
// 			db.Batch(func(txn *Txn) error {
// 				return txn.Set("CA500000211380900", data)
// 			})
// 		}()
// 	}
// 	wg.Wait()
// }

func BenchmarkUpdateWrite1000Times(b *testing.B) {
	db, err := New("/tmp/db", false)
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	raw, _ := os.ReadFile("test.json")

	for i := 0; i < b.N; i++ {
		UpdateWrite100Times(db, raw)
	}
}

// func BenchmarkBatchWrite1000Times(b *testing.B) {
// 	db, err := New("/tmp/db", false)
// 	if err != nil {
// 		log.Fatal(err)
// 	}
// 	defer db.Close()

// 	raw, _ := os.ReadFile("test.json")

// 	for i := 0; i < b.N; i++ {
// 		BatchWrite100Times(db, raw)
// 	}
// }

func TestSort(t *testing.T) {
	db, err := New("/tmp/db", false)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	err = db.Txn(func(txn *Txn) error {
		txn.Set("job:b", "b")
		txn.Set("job:c", "c")
		txn.Set("job:a", "a")
		txn.Set("job:a2", "a")
		txn.Set("job:4", "b")
		txn.Set("job:3", "c")
		txn.Set("job:2", "a")
		txn.Set("job:1", "a")
		txn.Set("job:0", "a")
		txn.Set("job:34", "a")
		txn.Set("job:34a", "a")
		txn.Set("job:34b", "a")
		txn.Set("job:35", "a")
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}

	db.Txn(func(txn *Txn) error {
		return txn.List("job:", func(key string, value []byte) (bool, error) {
			log.Println(key)
			return false, nil
		})
	}, true)
}

func TestReserveList(t *testing.T) {
	db, err := New("/tmp/db", false)
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	db.Txn(func(txn *Txn) error {
		for i := 0; i < 10; i++ {
			txn.Set(fmt.Sprintf("task:%d", i), i)
		}
		return nil
	})

	db.Txn(func(txn *Txn) error {
		return txn.List("task:", func(key string, value []byte) (bool, error) {
			log.Printf("%s = %s", key, value)
			return false, nil
		}, &ListOption{
			Begin:        "task:8",
			ContainBegin: true,
			Reverse:      true,
			Limit:        3,
			KeyOnly:      true,
		})
	}, true)
}
