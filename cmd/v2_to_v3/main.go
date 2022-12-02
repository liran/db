package main

import (
	"context"
	"flag"
	"log"
	"runtime"
	"time"

	"github.com/dgraph-io/badger/v3"
	"github.com/dgraph-io/ristretto/z"
	"github.com/liran/concurrency/v2"
	"github.com/liran/db/v3"
)

func init() {
	log.SetFlags(log.Ldate | log.Ltime | log.Lshortfile)
}

func main() {
	var src string
	var dst string
	var useStream bool
	var garbageCollection bool

	flag.StringVar(&src, "s", "database", "v2 database dir")
	flag.StringVar(&dst, "d", "database.db", "v3 database file")
	flag.BoolVar(&useStream, "stream", false, "stream list")
	flag.BoolVar(&garbageCollection, "g", false, "garbage collection")
	flag.Parse()

	uptime := time.Now()

	opts := badger.DefaultOptions(src)
	opts = opts.WithLoggingLevel(badger.DEBUG)
	srcDB, err := badger.Open(opts)
	if err != nil {
		log.Fatalln(err)
	}
	defer srcDB.Close()

	if garbageCollection {
		for {
			if err := srcDB.RunValueLogGC(0.7); err != nil {
				return
			}
			time.Sleep(time.Second)
		}
	}

	dstDB, err := db.New(dst, false)
	if err != nil {
		log.Fatalln(err)
	}
	defer dstDB.Close()

	index := 0
	pool := concurrency.New(runtime.NumCPU(), func(params ...any) {
		k := params[0].([]byte)
		v := params[1].([]byte)
		err = dstDB.Txn(func(tx *db.Txn) error {
			return tx.Set(string(k), v)
		})
		if err != nil {
			log.Fatalln(err)
		}
		index++
		log.Printf("[%d] copied %s", index, k)
	})
	defer pool.Close()

	if useStream {
		stream := srcDB.NewStream()
		stream.NumGo = runtime.NumCPU()
		stream.Send = func(buf *z.Buffer) error {
			list, err := badger.BufferToKVList(buf)
			if err != nil {
				return err
			}
			kvs := list.GetKv()
			for _, v := range kvs {
				pool.Process(v.Key, v.Value)
			}
			return nil
		}
		err = stream.Orchestrate(context.Background())
		if err != nil {
			log.Fatalln(err)
		}
	} else {
		err = srcDB.View(func(txn *badger.Txn) error {
			opts := badger.DefaultIteratorOptions
			opts.PrefetchSize = 20
			it := txn.NewIterator(opts)
			defer it.Close()
			for it.Rewind(); it.Valid(); it.Next() {
				item := it.Item()
				k := item.Key()
				v, err := item.ValueCopy(nil)
				if err != nil {
					return err
				}
				pool.Process(k, v)
			}
			return nil
		})
		if err != nil {
			log.Fatalln(err)
		}
	}

	pool.Wait()

	log.Printf("%d keys complete. uptime: %v", index, time.Since(uptime))
}
