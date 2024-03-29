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

	type StatusType string

	type UserUsUUS struct {
		ID        int
		Name      string         `db:"index=name,"`
		Email     string         `db:"index,tohen=1"`
		Tail      []string       `db:"index= tail"`
		Float     []float64      `db:"index"`
		Map       map[string]any `db:"index,"`
		Null      *time.Time     `db:"index =null;"`
		NowIdsafL *time.Time     `db:"index;"`
		EmptyIo   string         `db:"index"`
		NoIndex   string
		Status    StatusType `db:"index"`
	}

	type User interface {
	}

	var user User

	now := time.Now()
	user = &UserUsUUS{
		ID:        1,
		Name:      "John Doe",
		Email:     "john@example",
		Tail:      []string{"One", "tWe"},
		Float:     []float64{1.2, 3.4},
		Map:       map[string]any{"int": 234, "float": 22.3, "string": "Ac", "time": &now, "nil": nil},
		NowIdsafL: &now,
		NoIndex:   "node",
		Status:    "2023",
	}

	db.Txn(func(txn *Txn) error {
		return txn.IndexModel(1, user, true)
	})

	printUser := func() {
		db.Txn(func(txn *Txn) error {
			list, _ := txn.IndexList(user, "email", "joHn@example")
			count := txn.IndexCount(user, "email", "joHn@example")
			log.Printf("email: %+v, count: %d", list, count)

			list, _ = txn.IndexList(user, "tail", "twe")
			count = txn.IndexCount(user, "tail", "twe")
			log.Printf("tail: %+v, count: %d", list, count)

			id, _ := txn.IndexFirst(user, "map", "22.3")
			log.Printf("map->id: %s", id)

			list, _ = txn.IndexList(user, "status", "2023")
			count = txn.IndexCount(user, "status", "2023")
			log.Printf("status: %+v, count: %d", list, count)

			return nil
		}, true)
	}

	printUser()

	log.Println("delelte index --------------------------")
	db.Txn(func(txn *Txn) error {
		return txn.IndexModel(1, user, false)
	})

	printUser()
}

func TestMultipleBuckectInOneTxn(t *testing.T) {
	db, err := New("/tmp/db1", false)
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	err = db.Txn(func(txn *Txn) error {
		if err = txn.Set("a:1", "1"); err != nil {
			return err
		}

		return txn.Set("b:2", "2")
	})
	if err != nil {
		t.Fatal(err)
	}
}

func TestIndexCount(t *testing.T) {
	db, err := New("/tmp/db1", false)
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	type UserUsUUS struct {
		ID        int
		Name      string         `db:"index=name,"`
		Email     string         `db:"index,tohen=1"`
		Tail      []string       `db:"index= tail"`
		Float     []float64      `db:"index"`
		Map       map[string]any `db:"index,"`
		Null      *time.Time     `db:"index =null;"`
		NowIdsafL *time.Time     `db:"index;"`
		EmptyIo   string         `db:"index"`
		NoIndex   string
	}

	now := time.Now()
	user := &UserUsUUS{
		ID:        1,
		Name:      "John Doe",
		Email:     "john@example",
		Tail:      []string{"One", "tWe"},
		Float:     []float64{1.2, 3.4},
		Map:       map[string]any{"int": 234, "float": 22.3, "string": "Ac", "time": &now, "nil": nil},
		NowIdsafL: &now,
		NoIndex:   "node",
	}

	for i := 0; i < 5; i++ {
		err = db.Txn(func(txn *Txn) error {
			// if err := txn.IndexModel(user.ID, user, false); err != nil {
			// 	return err
			// }
			return txn.IndexModel(user.ID, user, true)
		})
		if err != nil {
			t.Fatal(err)
		}
	}

	var total int64
	db.Txn(func(txn *Txn) error {
		total = txn.IndexCount(user, "Email", "john@example")
		return nil
	}, true)
	log.Println("total:", total)
}

func TestHas(t *testing.T) {
	db, err := New("/tmp/db1", false)
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	key := "12900-3"
	db.Txn(func(txn *Txn) error {
		txn.Set(key, "")
		val, _ := txn.Get(key)
		log.Printf("val:%v, has: %v", val, txn.Has(key))
		return nil
	})

	db.Txn(func(txn *Txn) error {
		val, _ := txn.Get(key)
		log.Printf("val:%v, has: %v", val, txn.Has(key))
		return nil
	}, true)
}

func TestModel(t *testing.T) {
	db, err := New("/tmp/db1", false)
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	type UserUsUUS struct {
		ID        string
		Name      string         `db:"index=name,"`
		Email     string         `db:"index,tohen=1"`
		Tail      []string       `db:"index= tail"`
		Float     []float64      `db:"index"`
		Map       map[string]any `db:"index,"`
		Null      *time.Time     `db:"index =null;"`
		NowIdsafL *time.Time     `db:"index;"`
		EmptyIo   string         `db:"index"`
		NoIndex   string
	}

	now := time.Now()
	user := &UserUsUUS{
		ID:        "0",
		Name:      "John Doe",
		Email:     "john@example",
		Tail:      []string{"One", "tWe"},
		Float:     []float64{1.2, 3.4},
		Map:       map[string]any{"int": 234, "float": 22.3, "string": "Ac", "time": &now, "nil": nil},
		NowIdsafL: &now,
		NoIndex:   "node",
	}

	err = db.Txn(func(txn *Txn) error {
		total := txn.ModelTotal(user)
		log.Println("total:", total)

		counter := txn.ModelCounter(user)
		log.Println("counter:", counter)

		id := txn.ModelNextID(user, 11)
		log.Println("id:", id)

		if err := txn.ModelSet(user, id); err != nil {
			return err
		}

		if err := txn.ModelSet(nil, id); err != nil {
			return err
		}

		if err := txn.ModelSet(user.Map, id); err != nil {
			return err
		}

		length := txn.ModelIdLength(user)
		log.Println("id length:", length)

		counter = txn.ModelCounter(user)
		log.Println("counter:", counter)

		total = txn.ModelTotal(user)
		log.Println("total:", total)

		err := txn.ModelUpdate(user, id, func(m any) error {
			u := m.(*UserUsUUS)
			u.ID = id
			return nil
		})
		if err != nil {
			return err
		}

		m, err := txn.ModelGet(user, id)
		if err != nil {
			return err
		}
		newUser := ToEntity[*UserUsUUS](m)
		log.Println(newUser.Email)

		list, err := txn.ModelList(user, 10, "", true)
		if err != nil {
			return err
		}
		log.Println("list:", len(list))
		for _, v := range list {
			log.Printf("%+v", v)
		}

		list, err = txn.ModelIndexList(user, "email", "john@example")
		if err != nil {
			return err
		}
		log.Println("list:", len(list))
		for _, v := range list {
			log.Printf("%+v", v)
		}

		if err := txn.ModelDel(user, id); err != nil {
			return err
		}

		list, err = txn.ModelList(user, 10, "", false)
		if err != nil {
			return err
		}
		log.Println("list:", len(list))

		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}
