package main

import (
	"fmt"
	"log"
	"os"
	"time"

	"github.com/liran/db/v2"
	bolt "go.etcd.io/bbolt"
)

func main() {
	// Open the database.
	client, err := bolt.Open("/tmp/bolt", 0666, nil)
	if err != nil {
		log.Fatal(err)
	}
	// defer os.Remove(db.Path())
	defer client.Close()

	uptime := time.Now()

	// Insert data into a bucket.
	raw, err := os.ReadFile("../db.go")
	if err != nil {
		log.Fatal(err)
	}

	index := 0
	for i := 0; i < 10000; i++ {
		if err := client.Update(func(tx *bolt.Tx) error {
			b, err := tx.CreateBucketIfNotExists([]byte("a"))
			if err != nil {
				return err
			}

			b.FillPercent = 1.0

			for j := 0; j < 100; j++ {
				compressed, _ := db.GzipCompress(raw)
				err = b.Put([]byte(fmt.Sprintf("data:%d", index)), compressed)
				if err != nil {
					return err
				}
				log.Printf("writen: %d", index)
				index++
			}

			return nil
		}); err != nil {
			log.Fatal(err)
		}
	}

	// err = db.View(func(tx *bolt.Tx) error {
	// 	b := tx.Bucket([]byte("a"))
	// 	c := b.Cursor()
	// 	for k, v := c.First(); k != nil; k, v = c.Next() {
	// 		log.Println(string(k), string(v[:50]))
	// 	}
	// 	return nil
	// })
	// if err != nil {
	// 	log.Fatal(err)
	// }

	log.Printf("uptime: %v", time.Since(uptime))
	log.Println("sleep 2 minute")
	time.Sleep(2 * time.Minute)
}
