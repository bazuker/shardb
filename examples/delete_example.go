package examples

import (
	"fmt"
	"shardb/db"
	"time"
)

func RunDeleteExample() {
	database := db.NewDatabase("test")
	InitCustomTypes(database)

	fmt.Println("Scanning and loading the data...")
	start := time.Now()
	err := database.ScanAndLoadData("")
	if err != nil {
		fmt.Println("Failed to scan and load the data", err)
	}

	fmt.Println("After loading attempt database now contains", database.GetCollectionsCount(),
		"collections and", database.GetTotalObjectsCount(), "objects, it took", time.Now().Sub(start))

	if database.GetCollectionsCount() > 0 {
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
		fmt.Println("deleteing", key)
		randCol.DeleteById(element.Id)

		p2 := Person{Age: element.Payload.(*Person).Age}
		deleted, err := randCol.Delete(&p2)
		if err != nil {
			panic(err)
		}
		fmt.Println("deleted", deleted, "records")

		/*n, err := randCol.Restore(&p2)
		if err != nil {
			panic(err)
		}
		fmt.Println("restored", n, "records")*/

		size, err := randCol.Optimize()
		if err != nil {
			panic(err)
		}
		fmt.Println("Optimization removed", size, "redundant bytes of data")

		err = database.Sync()
		if err != nil {
			panic(err)
		}

	}
}
