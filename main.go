package main

import (
	"shardb/db"
	"fmt"
	"math/rand"
	"sync"
	"time"
	"strings"
)

var (
	database *db.Database
	wg = new(sync.WaitGroup)
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
	c.Write(Person{RandStringBytes(10), RandStringBytes(10)})
	wg.Done()
}

func fillUpCollection(c *db.Collection, total int) {
	perThread := total / 4
	wg.Add(4)
	// 1
	go func() {
		for i := 0; i < perThread; i++ {
			wg.Add(1)
			fillUp(c)
		}
		wg.Done()
	}()
	// 2
	go func() {
		for i := 0; i < perThread; i++ {
			wg.Add(1)
			fillUp(c)
		}
		wg.Done()
	}()
	// 3
	go func() {
		for i := 0; i < perThread; i++ {
			wg.Add(1)
			fillUp(c)
		}
		wg.Done()
	}()
	// 4
	go func() {
		for i := 0; i < perThread; i++ {
			wg.Add(1)
			fillUp(c)
		}
		wg.Done()
	}()
}

func main() {
	database = db.NewDatabase("test")
	start := time.Now()
	fmt.Println("Scanning and loading the data...")
	err := database.ScanAndLoadData()
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

		total := 1000
		fillUpCollection(database.GetCollection("c1"), total)
		fillUpCollection(database.GetCollection("c2"), total)
		fillUpCollection(database.GetCollection("c3"), total)

		fmt.Println("Filling up the database with", total * 3, "objects...")

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
	someId, _, err := randCol.GetRandomAliveObject()
	if err != nil {
		panic(err)
	}
	// retrieving the id
	trimmedId := strings.TrimLeft(someId,"id:")
	fmt.Println("Attempt to find", someId, trimmedId)

	// finding a record by id
	start = time.Now()
	data, err := randCol.FindById(trimmedId)
	checkErr(err)

	fmt.Println("Search took", time.Now().Sub(start))
	fmt.Println("RESULT", data.Payload.(map[string]interface{})["FirstName"])

	//fmt.Print("Press enter to exit...")
	//fmt.Scanln()
}
