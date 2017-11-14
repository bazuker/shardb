package db

import (
	"bytes"
	"encoding/gob"
	"github.com/cloudfoundry/gosigar"
)

var (
	mem  = sigar.Mem{}
	swap = sigar.Swap{}

	freeMem uint64
	memCap  uint64
)

func toMegabytes(val uint64) uint64 {
	return (val / 1024) / 1024
}

func GetFreeMemory() uint64 {
	return freeMem
}

func GerMemoryCapacity() uint64 {
	return memCap
}

func ProfileSystemMemory() {
	mem.Get()
	swap.Get()

	freeMem = toMegabytes(mem.Total - mem.ActualUsed)
	memCap = toMegabytes(mem.Total)
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
