package main

import (
	"flag"
	"log"
	"time"

	"github.com/dgraph-io/badger/v3"
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
	err = srcDB.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		it := txn.NewIterator(opts)
		defer it.Close()
		for it.Rewind(); it.Valid(); it.Next() {
			item := it.Item()
			k := item.Key()
			err := item.Value(func(v []byte) error {
				return dstDB.Txn(func(tx *db.Txn) error {
					return tx.Set(string(k), v)
				})
			})
			if err != nil {
				return err
			}
			log.Printf("copied %s", k)
			index++
		}
		return nil
	})
	if err != nil {
		log.Fatalln(err)
	}

	log.Printf("%d keys complete. uptime: %v", index, time.Since(uptime))
}
