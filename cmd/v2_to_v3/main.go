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
	flag.StringVar(&src, "src", "database", "v2 database dir")
	flag.StringVar(&dst, "dst", "database.db", "v3 database file")
	flag.Parse()

	uptime := time.Now()

	opts := badger.DefaultOptions(src)
	opts = opts.WithLoggingLevel(badger.INFO)
	srcDB, err := badger.Open(opts)
	if err != nil {
		log.Fatalln(err)
	}
	defer srcDB.Close()

	dstDB, err := db.New(dst)
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

	pool.Wait()

	log.Printf("%d keys complete. uptime: %v", index, time.Since(uptime))
}
