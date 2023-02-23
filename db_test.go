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
	db, err := New("/tmp/db1", false)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	err = db.Txn(func(txn *Txn) error {
		txn.Set("job:0001", "")
		txn.Set("job:0011", "")
		txn.Set("job:1101", "")
		txn.Set("job:0100", "")
		txn.Set("job:0099", "")
		txn.Set("job:0302", "")
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

func TestSet(t *testing.T) {
	db, err := New("/tmp/db", false)
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	db.Txn(func(txn *Txn) error {
		return txn.Set("abcd", "")
	})

	db.Txn(func(txn *Txn) error {
		raw, err := txn.Get("abcd")
		if err != nil {
			return err
		}

		log.Printf("%s,has: %v", raw, txn.Has("abcd"))
		return nil
	}, true)

}

func TestIndexModel(t *testing.T) {
	db, err := New("/tmp/db1", false)
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	type UserUsUUS struct {
		ID      int
		Name    string         `db:"index=name,"`
		Email   string         `db:"index,tohen=1"`
		Tail    []string       `db:"index= tail"`
		Float   []float64      `db:"index"`
		Map     map[string]any `db:"index,"`
		Null    *time.Time     `db:"index =null;"`
		Now     *time.Time     `db:"index;"`
		Empty   string         `db:"index"`
		NoIndex string
	}

	now := time.Now()
	db.Txn(func(txn *Txn) error {
		user := &UserUsUUS{
			ID:      1,
			Name:    "John Doe",
			Email:   "john@example",
			Tail:    []string{"One", "tWe"},
			Float:   []float64{1.2, 3.4},
			Map:     map[string]any{"int": 234, "float": 22.3, "string": "Ac", "time": &now, "nil": nil},
			Now:     &now,
			NoIndex: "node",
		}
		return txn.IndexModel(1, user)
	})

	db.Txn(func(txn *Txn) error {
		list, _ := txn.IndexList(&UserUsUUS{}, "email", "joHn@example")
		count := txn.IndexCount(&UserUsUUS{}, "email", "joHn@example")
		log.Printf("%+v, count: %d", list, count)

		list, _ = txn.IndexList(&UserUsUUS{}, "tail", "twe")
		count = txn.IndexCount(&UserUsUUS{}, "tail", "twe")
		log.Printf("%+v, count: %d", list, count)

		id, _ := txn.IndexFirst(&UserUsUUS{}, "map", "22.3")
		log.Printf("id: %s", id)

		return nil
	}, true)
}
