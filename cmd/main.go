package main

import (
	"log"
	"time"

	"github.com/liran/db/v2"
)

func main() {
	client, err := db.New("/tmp/db")
	if err != nil {
		log.Fatal(err)
	}
	defer client.Close()

	// raw, err := os.ReadFile("../go.sum")
	// if err != nil {
	// 	log.Fatal(err)
	// }

	// index := 0
	// for i := 0; i < 10000; i++ {
	// 	err = client.Txn(func(txn *db.Txn) error {
	// 		for j := 0; j < 100; j++ {
	// 			err = client.Set(txn, fmt.Sprintf("data:%d", index), raw)
	// 			if err != nil {
	// 				return err
	// 			}
	// 			log.Printf("writen: %d", index)
	// 			index++
	// 		}
	// 		return nil
	// 	})
	// 	if err != nil {
	// 		log.Fatal(err)
	// 	}
	// }

	err = client.List("data:", "", false, func(key string, value []byte) error {
		log.Println(key, string(value[:50]))
		return nil
	})
	if err != nil {
		log.Fatal(err)
	}

	log.Println("sleep 2 minute")
	time.Sleep(2 * time.Minute)
}
