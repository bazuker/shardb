package tests

import (
	"shardb/db"
	"testing"
)

func TestSuperBuffer(t *testing.T) {
	buffer := db.NewSuperBuffer([]byte("Hello World! This is a test."))
	buffer.Cut(4, 1)
	data := buffer.Bytes()
	result := string(data)
	if result != "Hell World! This is a test." {
		t.Fatal("failed to cut")
	}
	t.Log(result)
}
