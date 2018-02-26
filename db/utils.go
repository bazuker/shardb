package db

import (
	"bytes"
	"encoding/gob"
	"github.com/shirou/gopsutil/mem"
)

var (
	virtual      *mem.VirtualMemoryStat
	mamAvailable uint64
	memCapacity  uint64
	memUsed      uint64
)

func toMegabytes(val uint64) uint64 {
	return (val / 1024) / 1024
}

func GetFreeMemory() uint64 {
	return mamAvailable
}

func GetUsedMemory() uint64 {
	return memUsed
}

func GetMemoryCapacity() uint64 {
	return memCapacity
}

func ProfileSystemMemory() (err error) {
	virtual, err = mem.VirtualMemory()
	if err != nil {
		return err
	}
	mamAvailable = toMegabytes(virtual.Available)
	memCapacity = toMegabytes(virtual.Total)
	memUsed = toMegabytes(virtual.Used)
	return
}

func EncodeGob(i interface{}) ([]byte, error) {
	var data bytes.Buffer
	enc := gob.NewEncoder(&data)
	err := enc.Encode(i)
	if err != nil {
		return nil, err
	}
	return data.Bytes(), nil
}

func GetGobDecoder(data []byte) *gob.Decoder {
	return gob.NewDecoder(bytes.NewReader(data))
}
