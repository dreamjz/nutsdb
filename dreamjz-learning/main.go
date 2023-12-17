package main

import (
	"fmt"
	"github.com/nutsdb/nutsdb"
	"log"
	"os"
)

const logFmt = "============ %s ============\n"

func main() {
	tmpDir := "./tmp/"
	os.RemoveAll(tmpDir)

	db, err := nutsdb.Open(nutsdb.DefaultOptions, nutsdb.WithDir(tmpDir))
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	// create bucket
	log.Printf(logFmt, "Create Bucket")

	bucket001 := "myZSet1"
	bucket002 := "myZSet2"

	if err := db.Update(func(tx *nutsdb.Tx) error {
		// you should call Bucket with data structure and the name of bucket first
		return tx.NewBucket(nutsdb.DataStructureSortedSet, bucket001)
	}); err != nil {
		log.Fatal(err)
	}

	if err := db.Update(func(tx *nutsdb.Tx) error {
		// you should call Bucket with data structure and the name of bucket first
		return tx.NewBucket(nutsdb.DataStructureSortedSet, bucket002)
	}); err != nil {
		log.Fatal(err)
	}

	// iterate buckets
	log.Printf(logFmt, "iterate buckets")

	if err := db.View(
		func(tx *nutsdb.Tx) error {
			return tx.IterateBuckets(nutsdb.DataStructureSortedSet, "*", func(bucket string) bool {
				fmt.Println("bucket: ", bucket)
				// true: continue, false: break
				return true
			})
		}); err != nil {
		log.Fatal(err)
	}

	// delete bucket
	log.Printf(logFmt, "delete bucket")

	if err := db.Update(
		func(tx *nutsdb.Tx) error {
			return tx.DeleteBucket(nutsdb.DataStructureSortedSet, bucket002)
		}); err != nil {
		log.Fatal(err)
	}

	zsetExample(db)

}

func zsetExample(db *nutsdb.DB) {
	bucket001 := "myZSet1"

	// zset
	log.Printf(logFmt, "zset")

	// ZAdd add new element
	log.Printf(logFmt, "ZAdd add new element")

	if err := db.Update(
		func(tx *nutsdb.Tx) error {
			key := []byte("key1")
			return tx.ZAdd(bucket001, key, 1, []byte("val-new"))
		}); err != nil {
		log.Fatal(err)
	}

	if err := db.Update(
		func(tx *nutsdb.Tx) error {
			key := []byte("key1")
			return tx.ZAdd(bucket001, key, 1, []byte("val-new-01"))
		}); err != nil {
		log.Fatal(err)
	}

	if err := db.Update(
		func(tx *nutsdb.Tx) error {
			key := []byte("key1")
			return tx.ZAdd(bucket001, key, 1, []byte("val-new-02"))
		}); err != nil {
		log.Fatal(err)
	}

	if err := db.Update(
		func(tx *nutsdb.Tx) error {
			key := []byte("key1")
			return tx.ZAdd(bucket001, key, 1, []byte("val1"))
		}); err != nil {
		log.Fatal(err)
	}

	if err := db.Update(
		func(tx *nutsdb.Tx) error {
			key := []byte("key1")
			return tx.ZAdd(bucket001, key, 2, []byte("val2"))
		}); err != nil {
		log.Fatal(err)
	}

	if err := db.Update(
		func(tx *nutsdb.Tx) error {
			key := []byte("key1")
			return tx.ZAdd(bucket001, key, 3, []byte("val3"))
		}); err != nil {
		log.Fatal(err)
	}

	if err := db.Update(
		func(tx *nutsdb.Tx) error {
			key := []byte("key1")
			return tx.ZAdd(bucket001, key, 3, []byte("val3-01"))
		}); err != nil {
		log.Fatal(err)
	}

	// ZCard returns the sorted set cardinality (number of elements) of the sorted set specified by key in a bucket.
	log.Printf(logFmt, "ZCard")

	if err := db.View(
		func(tx *nutsdb.Tx) error {
			key := []byte("key1")
			num, err := tx.ZCard(bucket001, key)
			if err != nil {
				return err
			}
			fmt.Println("ZCard num", num)
			return nil
		}); err != nil {
		log.Fatal(err)
	}

	// ZCount returns the number of elements in the sorted set specified by key in a bucket with a score between min and max and opts.
	// Opts include the following parameters:
	// - Limit int / limit the max nodes to return
	// - ExcludeStart bool / exclude start value, so it search in interval (start, end] or (start, end)
	// - ExcludeEnd bool / exclude end value, so it search in interval [start, end) or (start, end)
	log.Printf(logFmt, "ZCount")

	if err := db.View(
		func(tx *nutsdb.Tx) error {
			key := []byte("key1")
			num, err := tx.ZCount(bucket001, key, 0, 1, nil)
			if err != nil {
				return err
			}
			fmt.Println("ZCount num", num)
			return nil
		}); err != nil {
		log.Fatal(err)
	}

	// ZSore returns the score of members in a sorted set specified by key in a bucket.
	log.Printf(logFmt, "ZSore")

	if err := db.View(
		func(tx *nutsdb.Tx) error {
			bucket := "myZSet1"
			key := []byte("key1")
			score, err := tx.ZScore(bucket, key, []byte("val1"))
			if err != nil {
				return err
			}
			fmt.Println("val1 score: ", score)
			return nil
		}); err != nil {
		log.Fatal(err)
	}

	// ZMembers returns all the members and scores of members of the set specified by key in a bucket.
	log.Printf(logFmt, "ZMembers")

	if err := db.View(
		func(tx *nutsdb.Tx) error {
			bucket := "myZSet1"
			key := []byte("key1")
			nodes, err := tx.ZMembers(bucket, key)
			if err != nil {
				return err
			}
			for node := range nodes {
				fmt.Println("member:", node.Score, string(node.Value))
			}
			return nil
		}); err != nil {
		log.Fatal(err)
	}

	// ZPeekMax Returns the member with the highest score in the sorted set specified by key in a bucket.
	log.Printf(logFmt, "ZPeekMax")

	if err := db.View(
		func(tx *nutsdb.Tx) error {
			bucket := "myZSet1"
			key := []byte("key1")
			node, err := tx.ZPeekMax(bucket, key)
			if err != nil {
				return err
			}
			fmt.Println("ZPeekMax:", node.Score)
			return nil
		}); err != nil {
		log.Fatal(err)
	}

	// ZPeekMin Returns the member with the lowest score in the sorted set specified by key in a bucket.
	log.Printf(logFmt, "ZPeekMin")

	if err := db.View(
		func(tx *nutsdb.Tx) error {
			bucket := "myZSet1"
			key := []byte("key1")
			node, err := tx.ZPeekMin(bucket, key)
			if err != nil {
				return err
			}
			fmt.Println("ZPeekMin:", node.Score)
			return nil
		}); err != nil {
		log.Fatal(err)
	}

	// ZPopMax Removes and returns the member with the highest score in the sorted set specified by key in a bucket.
	// ZPopMin Removes and returns the member with the lowest score in the sorted set specified by key in a bucket.
	log.Printf(logFmt, "ZPopMax ZPopMin")

	if err := db.Update(
		func(tx *nutsdb.Tx) error {
			bucket := "myZSet1"
			key := []byte("key1")
			node, err := tx.ZPopMax(bucket, key)
			if err != nil {
				return err
			}
			fmt.Println("ZPopMax:", node.Score)
			return nil
		}); err != nil {
		log.Fatal(err)
	}
	if err := db.Update(
		func(tx *nutsdb.Tx) error {
			bucket := "myZSet1"
			key := []byte("key1")
			node, err := tx.ZPopMin(bucket, key)
			if err != nil {
				return err
			}
			fmt.Println("ZPopMin:", node.Score)
			return nil
		}); err != nil {
		log.Fatal(err)
	}
	if err := db.View(
		func(tx *nutsdb.Tx) error {
			bucket := "myZSet1"
			key := []byte("key1")
			nodes, err := tx.ZMembers(bucket, key)
			if err != nil {
				return err
			}
			for node := range nodes {
				fmt.Println("member:", node.Score, string(node.Value))
			}
			return nil
		}); err != nil {
		log.Fatal(err)
	}

	// ZRangeByRank Returns all the elements in the sorted set specified by key in a bucket with a rank between start and end (including elements with rank equal to start or end).
	log.Printf(logFmt, "ZRangeByRank")

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

	// ZRangeByScore Returns all the elements in the sorted set specified by key in a bucket with a score between min and max. And the parameter Opts is the same as ZCount's.
	log.Printf(logFmt, "ZRangeByScore")

	if err := db.View(
		func(tx *nutsdb.Tx) error {
			bucket := "myZSet1"
			key := []byte("key1")
			nodes, err := tx.ZRangeByScore(bucket, key, 0, 2, nil)
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

	// ZRank Returns the rank of member in the sorted set specified by key in a bucket, with the scores ordered from low to high.
	log.Printf(logFmt, "ZRank")

	if err := db.View(
		func(tx *nutsdb.Tx) error {
			bucket := "myZSet1"
			key := []byte("key1")
			rank, err := tx.ZRank(bucket, key, []byte("val1"))
			if err != nil {
				return err
			}
			fmt.Println("val1 ZRank :", rank)
			return nil
		}); err != nil {
		log.Fatal(err)
	}

	if err := db.View(
		func(tx *nutsdb.Tx) error {
			bucket := "myZSet1"
			key := []byte("key1")
			rank, err := tx.ZRank(bucket, key, []byte("val2"))
			if err != nil {
				return err
			}
			fmt.Println("val2 ZRank :", rank)
			return nil
		}); err != nil {
		log.Fatal(err)
	}

	if err := db.View(
		func(tx *nutsdb.Tx) error {
			bucket := "myZSet1"
			key := []byte("key1")
			rank, err := tx.ZRank(bucket, key, []byte("val3"))
			if err != nil {
				return err
			}
			fmt.Println("val3 ZRank :", rank)
			return nil
		}); err != nil {
		log.Fatal(err)
	}

	// ZRevRank Returns the rank of member in the sorted set specified by key in a bucket, with the scores ordered from high to low.
	log.Printf(logFmt, "ZRevRank")
	if err := db.View(
		func(tx *nutsdb.Tx) error {
			bucket := "myZSet1"
			key := []byte("key1")
			rank, err := tx.ZRank(bucket, key, []byte("val1"))
			if err != nil {
				return err
			}
			fmt.Println("ZRevRank val1 rank:", rank) // ZRevRank key1 rank: 3
			return nil
		}); err != nil {
		log.Fatal(err)
	}
	if err := db.View(
		func(tx *nutsdb.Tx) error {
			bucket := "myZSet1"
			key := []byte("key1")
			rank, err := tx.ZRank(bucket, key, []byte("val2"))
			if err != nil {
				return err
			}
			fmt.Println("ZRevRank val2 rank:", rank) // ZRevRank key2 rank: 2
			return nil
		}); err != nil {
		log.Fatal(err)
	}
	if err := db.View(
		func(tx *nutsdb.Tx) error {
			bucket := "myZSet1"
			key := []byte("key1")
			rank, err := tx.ZRank(bucket, key, []byte("val3"))
			if err != nil {
				return err
			}
			fmt.Println("ZRevRank val3 rank:", rank) // ZRevRank key3 rank: 1
			return nil
		}); err != nil {
		log.Fatal(err)
	}

	// ZRem Returns the member with the lowest score in the sorted set specified by key in a bucket.
	log.Printf(logFmt, "ZRem")

	if err := db.Update(
		func(tx *nutsdb.Tx) error {
			bucket := "myZSet1"
			key := []byte("key1")
			return tx.ZRem(bucket, key, []byte("val3"))
		}); err != nil {
		log.Fatal(err)
	}

	if err := db.View(
		func(tx *nutsdb.Tx) error {
			bucket := "myZSet1"
			key := []byte("key1")
			nodes, err := tx.ZMembers(bucket, key)
			if err != nil {
				return err
			}
			fmt.Println("after ZRem key1, ZMembers nodes")
			for node := range nodes {
				fmt.Println("item:", node.Score, string(node.Value))
			}
			return nil
		}); err != nil {
		log.Fatal(err)
	}

	// ZRemRangeByRank
	// Removes all elements in the sorted set stored specified by key in a bucket with rank between start and end.
	// The rank is 1-based integer. Rank 1 means the first node; Rank -1 means the last node.
	log.Printf(logFmt, "ZRemRangeByRank")

	if err := db.Update(
		func(tx *nutsdb.Tx) error {
			bucket := "myZSet1"
			key := []byte("key1")
			return tx.ZRemRangeByRank(bucket, key, 1, 2)
		}); err != nil {
		log.Fatal(err)
	}

	if err := db.View(
		func(tx *nutsdb.Tx) error {
			bucket := "myZSet1"
			key := []byte("key1")
			nodes, err := tx.ZMembers(bucket, key)
			if err != nil {
				return err
			}
			fmt.Println("after ZRemRangeByRank, ZMembers nodes is", len(nodes))
			for node := range nodes {
				fmt.Println("item:", node.Score, string(node.Value))
			}
			return nil
		}); err != nil {
		log.Fatal(err)
	}
}
