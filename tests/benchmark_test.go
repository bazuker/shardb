package tests

import (
	"errors"
	"shardb/db"
	"strings"
	"testing"
)

type testStruct struct {
	F1, F2 string
	F3     int
}

func loadDatabase() (*db.Database, error) {
	db := db.NewDatabase("test")
	err := db.ScanAndLoadData("C:\\Users\\furm0008\\GoglandProjects\\src\\shardb")
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
	trimmedId := strings.TrimLeft(someId, "id:")

	for n := 0; n < b.N; n++ {
		c.FindById(trimmedId)
	}
}

func BenchmarkWriteData(b *testing.B) {
	db, err := loadDatabase()
	if err != nil {
		b.Fatal(err)
	}
	c := db.GetRandomCollection()
	if c == nil {
		b.Fatal(errors.New("database has no collections"))
	}
	dat := testStruct{"some", "text", 5120}

	for n := 0; n < b.N; n++ {
		c.Write(dat)
	}
}
