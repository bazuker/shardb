package examples

import (
	"fmt"
	"log"
	"math/rand"
	"shardb/db"
	"sync"
	"time"
)

var wg = new(sync.WaitGroup)

const letterBytes = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"

func RandStringBytes(n int) string {
	b := make([]byte, n)
	for i := range b {
		b[i] = letterBytes[rand.Intn(len(letterBytes))]
	}
	return string(b)
}

func checkErr(err error) {
	if err != nil {
		panic(err)
	}
}

func fillUp(c *db.Collection) {
	err := c.Write(&Person{RandStringBytes(10), RandStringBytes(10), rand.Intn(100)})
	if err != nil {
		log.Println("error on write", err)
	}
}

func fillUpCollection(c *db.Collection, total, threads int) {
	perThread := total / threads
	wg.Add(threads)
	for i := 0; i < threads; i++ {
		go func() {
			for i := 0; i < perThread; i++ {
				fillUp(c)
			}
			wg.Done()
		}()
	}
}

func RunGeneralExample() {
	database := db.NewDatabase("test")
	InitCustomTypes(database)

	fmt.Println("Scanning and loading the data...")
	start := time.Now()
	err := database.ScanAndLoadData("")
	if err != nil {
		fmt.Println("Failed to scan and load the data", err)
	}

	fmt.Println("After loading attempt database now contains", database.GetCollectionsCount(),
		"collections and", database.GetTotalObjectsCount(), "objects")

	syncRequired := false

	if database.GetCollectionsCount() <= 0 {
		start := time.Now()

		c1, _ := database.AddCollection("c1")
		c2, _ := database.AddCollection("c2")
		c3, _ := database.AddCollection("c3")

		rand.Seed(time.Now().UnixNano())

		total := 10000
		threads := 4

		// filling the collections with random data
		fillUpCollection(c1, total, threads)
		fillUpCollection(c2, total, threads)
		fillUpCollection(c3, total, threads)

		fmt.Println("Filling up the database with", total*3, "objects...")

		wg.Wait()
		syncRequired = true

		fmt.Println("Writing took", time.Now().Sub(start))
	} else {
		fmt.Println("Database loading took", time.Now().Sub(start))
	}

	if syncRequired {
		start = time.Now()
		// save to the disk
		err = database.Sync()
		fmt.Println("Sync took", time.Now().Sub(start))
		if err != nil {
			panic(err)
		}
	}

	// getting a random collection
	randCol, _ := database.GetRandomCollection()
	if randCol == nil {
		panic("database has no collections")
	}
	fmt.Println("Using collection", randCol.Name)
	// getting a random record
	key, element, err := randCol.GetRandomAliveObject()
	if err != nil {
		panic(err)
	}
	p := element.Payload.(*Person)
	fmt.Println("Random object", key, p)
	checkErr(err)

	// scan all
	p2 := Person{Age: p.Age}
	start = time.Now()
	results, err := randCol.Scan(&p2, true)
	fmt.Println("Search (Scan) took", time.Now().Sub(start))
	checkErr(err)

	resultsLen := len(results)
	fmt.Println("Results in set:", resultsLen)
	for i := 0; i < resultsLen; i++ {
		el, err := randCol.DecodeElement(results[i])
		p2 = *el.Payload.(*Person)
		checkErr(err)
		fmt.Println("Result", i, p2.Login)
	}

	// single scan
	fmt.Println()
	start = time.Now()
	data, err := randCol.ScanOne(p, true)
	checkErr(err)

	el, err := randCol.DecodeElement(data)
	p2 = *el.Payload.(*Person)
	checkErr(err)

	fmt.Println("Search (Scan) took", time.Now().Sub(start))
	fmt.Println("Result", p2.Age)
	// perform again (now reads from cache)
	fmt.Println()
	start = time.Now()
	data, err = randCol.ScanOne(p, true)
	checkErr(err)

	el, err = randCol.DecodeElement(data)
	p2 = *el.Payload.(*Person)
	checkErr(err)

	fmt.Println("Search (Scan) took", time.Now().Sub(start))
	fmt.Println("Result", p2.Age)
}
