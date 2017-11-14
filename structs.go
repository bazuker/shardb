package main

import (
	"shardb/db"
	"strconv"
)

type Person struct {
	FirstName string
	Age       int // primary key
}

func (c *Person) GetDataIndex() []*db.FullDataIndex {
	return []*db.FullDataIndex{
		{"FirstName", c.FirstName, true},
		{"Age", strconv.Itoa(c.Age), false},
	}
}

func InitCustomTypes(db *db.Database) {
	db.RegisterType(&Person{})
}
