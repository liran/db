package main

import (
	"fmt"
	"log"
	"os"
	"time"

	"github.com/liran/db/v3"
)

func main() {
	// open the database.
	client, err := db.New("/tmp/bolt")
	if err != nil {
		log.Fatal(err)
	}
	defer client.Close()

	uptime := time.Now()

	index := 0

	// insert data into a bucket
	raw, err := os.ReadFile("../../dist/test.csv")
	if err != nil {
		log.Fatal(err)
	}

	if err := client.Txn(func(txn *db.Txn) error {
		for j := 0; j < 50; j++ {
			err = txn.Set(fmt.Sprintf("data:%d", index), raw)
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

	err = client.Txn(func(txn *db.Txn) error {
		return txn.List("data:", "", func(key string, value []byte) error {
			index++
			log.Println(index, key, string(value[:50]))
			return nil
		})
	}, true)
	if err != nil {
		log.Fatal(err)
	}

	log.Printf("uptime: %v", time.Since(uptime))
	log.Println("sleep 2 minute")
	time.Sleep(2 * time.Minute)
}
