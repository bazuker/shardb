package examples

import (
	"shardb/db"
	"strconv"
)

type Person struct {
	Login string // primary unique key
	Name  string
	Age   int // primary key
}

func (c *Person) GetDataIndex() []*db.FullDataIndex {
	return []*db.FullDataIndex{
		{"Login", c.Login, true},
		{"Age", strconv.Itoa(c.Age), false},
	}
}

func InitCustomTypes(db *db.Database) {
	db.RegisterType(&Person{})
}
