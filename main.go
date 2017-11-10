package main

import (
	"shardb/db"
	"fmt"
	"math/rand"
	"sync"
	"time"
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
	/*err := database.ScanAndLoadData()
	if err != nil {
		fmt.Println("Failed to scan and load the data")
	}

	fmt.Println("After loading attempt database contains", database.GetCollectionsCount(), "collections")*/

	if database.GetCollectionsCount() <= 0 {
		start := time.Now()
		database.AddCollection("c1")

		rand.Seed(time.Now().UnixNano())

		total := 1000
		perThread := total / 4

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

	err := database.Sync()
	if err != nil {
		panic(err)
	}

	/*fmt.Println("Database now contains", database.GetTotalObjectsCount(), "objects")

	someId, _, err := database.GetRandomAliveObject()
	if err != nil {
		panic(err)
	}
	trimmedId := strings.TrimLeft(someId,"c1:id:")

	start = time.Now()
	data, err := database.GetCollection("c1").FindById(trimmedId)
	fmt.Println("Search took", time.Now().Sub(start))

	checkErr(err)
	fmt.Println("RESULT", data.Payload.(map[string]interface{})["FirstName"])

	fmt.Print("Press enter to exit...")
	fmt.Scanln()*/
}
