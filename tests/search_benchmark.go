package tests

import (
	"testing"
	"shardb/db"
	"errors"
	"strings"
	"fmt"
)

func loadDatabase() (*db.Database, error) {
	db := db.NewDatabase("test")
	err := db.ScanAndLoadData()
	return db, err
}

func BenchmarkSearchById(b *testing.B) {
	db, err := loadDatabase()
	if err != nil {
		b.Fatal(err)
	}
	c := db.GetRandomCollection()
	if c == nil {
		b.Fatal(errors.New("database has no collections"))
	}
	someId, _, err := c.GetRandomAliveObject()
	if err != nil {
		panic(err)
	}
	trimmedId := strings.TrimLeft(someId,"id:")

	for n := 0; n < b.N; n++ {
		c.FindById(trimmedId)
	}
}