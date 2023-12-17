package main

import (
	"fmt"
	"github.com/nutsdb/nutsdb"
	"log"
)

func main() {
	db, err := nutsdb.Open(nutsdb.DefaultOptions, nutsdb.WithDir("./tmp/"))
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	// create bucket

	bucket001 := "myZSet1"

	if err := db.Update(func(tx *nutsdb.Tx) error {
		// you should call Bucket with data structure and the name of bucket first
		return tx.NewBucket(nutsdb.DataStructureSortedSet, bucket001)
	}); err != nil {
		log.Fatal(err)
	}

	// zset

	// add

	// ZSet range by rank

	if err := db.Update(
		func(tx *nutsdb.Tx) error {
			bucket := "myZSet1"
			key := []byte("key1")
			return tx.ZAdd(bucket, key, 1, []byte("val1"))
		}); err != nil {
		log.Fatal(err)
	}

	if err := db.Update(
		func(tx *nutsdb.Tx) error {
			bucket := "myZSet1"
			key := []byte("key1")
			return tx.ZAdd(bucket, key, 2, []byte("val2"))
		}); err != nil {
		log.Fatal(err)
	}

	if err := db.Update(
		func(tx *nutsdb.Tx) error {
			bucket := "myZSet1"
			key := []byte("key1")
			return tx.ZAdd(bucket, key, 3, []byte("val3"))
		}); err != nil {
		log.Fatal(err)
	}

	if err := db.View(
		func(tx *nutsdb.Tx) error {
			bucket := "myZSet1"
			key := []byte("key1")
			nodes, err := tx.ZRangeByRank(bucket, key, 1, 3)
			if err != nil {
				return err
			}
			for _, node := range nodes {
				fmt.Println("item:", string(node.Value), node.Score)
			}
			return nil
		}); err != nil {
		log.Fatal(err)
	}
}
