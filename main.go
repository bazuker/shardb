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

func fillUp() {
	c := database.GetCollection("c1")
	c.Write(Person{RandStringBytes(10), RandStringBytes(10)})
	wg.Done()
}

func main() {
	database = db.NewDatabase("test")
	start := time.Now()
	err := database.ScanAndLoadData()
	if err != nil {
		fmt.Println("Failed to scan and load the data", err)
	}

	fmt.Println("After loading attempt database contains", database.GetCollectionsCount(), "collections and", database.GetTotalObjectsCount(), "objects")

	if database.GetCollectionsCount() <= 0 {
		start := time.Now()
		database.AddCollection("c1")

		rand.Seed(time.Now().UnixNano())

		total := 1000
		perThread := total / 4

		fmt.Println("Filling up the database with", total, "objects...")

		wg.Add(4)
		// 1
		go func() {
			for i := 0; i < perThread; i++ {
				wg.Add(1)
				fillUp()
			}
			wg.Done()
		}()
		// 2
		go func() {
			for i := 0; i < perThread; i++ {
				wg.Add(1)
				fillUp()
			}
			wg.Done()
		}()
		// 3
		go func() {
			for i := 0; i < perThread; i++ {
				wg.Add(1)
				fillUp()
			}
			wg.Done()
		}()
		// 4
		go func() {
			for i := 0; i < perThread; i++ {
				wg.Add(1)
				fillUp()
			}
			wg.Done()
		}()

		wg.Wait()

		fmt.Println("Writing took", time.Now().Sub(start))
	} else {
		fmt.Println("Database loading took", time.Now().Sub(start))
	}

	start = time.Now()
	err = database.Sync()
	fmt.Println("Sync took", time.Now().Sub(start))
	if err != nil {
		panic(err)
	}

	someId, _, err := database.GetRandomAliveObject()
	if err != nil {
		panic(err)
	}
	trimmedId := strings.TrimLeft(someId,"c1:id:")
	fmt.Println("Attempt to find", someId, trimmedId)

	start = time.Now()
	data, err := database.GetCollection("c1").FindById(trimmedId)
	checkErr(err)

	fmt.Println("Search took", time.Now().Sub(start))
	fmt.Println("RESULT", data.Payload.(map[string]interface{})["FirstName"])

	//fmt.Print("Press enter to exit...")
	//fmt.Scanln()
}
