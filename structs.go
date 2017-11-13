package main

import "shardb/db"

type Person struct {
	FirstName string
	LastName string
}

func InitCustomTypes(db *db.Database) {
	db.RegisterType(&Person{})
}
