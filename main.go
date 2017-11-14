package main

import (
	"fmt"
	"math/rand"
	"shardb/db"
	"sync"
	"time"
)

var (
	database *db.Database
	wg       = new(sync.WaitGroup)
)

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
	//c := database.GetCollection("c1")
	c.Write(&Person{RandStringBytes(10), rand.Intn(100)})
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

func main() {
	database = db.NewDatabase("test")
	InitCustomTypes(database)

	start := time.Now()
	fmt.Println("Scanning and loading the data...")
	err := database.ScanAndLoadData("")
	if err != nil {
		fmt.Println("Failed to scan and load the data", err)
	}

	fmt.Println("After loading attempt database now contains", database.GetCollectionsCount(),
		"collections and", database.GetTotalObjectsCount(), "objects")

	syncRequired := false

	if database.GetCollectionsCount() <= 0 {
		start := time.Now()

		database.AddCollection("c1")
		database.AddCollection("c2")
		database.AddCollection("c3")

		rand.Seed(time.Now().UnixNano())

		total := 1000000
		threads := 4

		fillUpCollection(database.GetCollection("c1"), total, threads)
		fillUpCollection(database.GetCollection("c2"), total, threads)
		fillUpCollection(database.GetCollection("c3"), total, threads)

		fmt.Println("Filling up the database with", total*3, "objects...")

		wg.Wait()
		syncRequired = true

		fmt.Println("Writing took", time.Now().Sub(start))
	} else {
		fmt.Println("Database loading took", time.Now().Sub(start))
	}

	if syncRequired {
		start = time.Now()
		err = database.Sync()
		fmt.Println("Sync took", time.Now().Sub(start))
		if err != nil {
			panic(err)
		}
	}

	// getting a random collection
	randCol := database.GetRandomCollection()
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

	// ScanAll
	p2 := Person{Age: p.Age}
	start = time.Now()
	results, err := randCol.ScanAll(&p2)
	fmt.Println("Search (ScanAll) took", time.Now().Sub(start))
	checkErr(err)

	resultsLen := len(results)
	fmt.Println("Results in set:", resultsLen)
	for i := 0; i < resultsLen; i++ {
		el, err := randCol.Map.DecodeElement(results[i])
		p2 = *el.Payload.(*Person)
		checkErr(err)
		fmt.Println("Result", i, p2.FirstName)
	}
	// Single
	fmt.Println()
	start = time.Now()
	data, err := randCol.Scan(p)
	checkErr(err)
	el, err := randCol.Map.DecodeElement(data)
	p2 = *el.Payload.(*Person)
	checkErr(err)
	fmt.Println("Search (Scan) took", time.Now().Sub(start))
	fmt.Println("Result", p2.Age)

	fmt.Print("Press enter to exit...")
	fmt.Scanln()
}
