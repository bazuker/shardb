package tests

import (
	"errors"
	"shardb/db"
	"strconv"
	"testing"
)

type person struct {
	FirstName string // primary unique key
	Age       int    // primary key
}

func (c *person) GetDataIndex() []*db.FullDataIndex {
	return []*db.FullDataIndex{
		{"FirstName", c.FirstName, true},
		{"Age", strconv.Itoa(c.Age), false},
	}
}

func loadDatabase() (*db.Database, error) {
	db := db.NewDatabase("test")
	db.RegisterTypeName("testPerson", &person{})

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
	_, obj, err := c.GetRandomAliveObject()
	if err != nil {
		panic(err)
	}
	p := obj.Payload.(*person)

	for n := 0; n < b.N; n++ {
		_, err = c.Scan(p)
		if err != nil {
			b.Fatal(err)
		}
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
	dat := person{"some", 5120}

	for n := 0; n < b.N; n++ {
		c.Write(&dat)
	}
}
