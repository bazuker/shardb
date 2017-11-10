package db

import (
	"encoding/json"
	"sync"
	"math/rand"
	"sync/atomic"
	"os"
)

type Index struct {
	Field string
	Unique bool
}

type IndexData struct {
	Field string
	Data string
}

type Collection struct {
	Name              string
	Map               *ConcurrentMap
	Indexes           []*Index

	ShardDestinations map[string]int
	mx                sync.Mutex

	ObjectsCounter    uint64
}

type Element struct {
	Id string `json:"x"`
	Payload interface{} `json:"p"`
}

func (cm *ConcurrentMap) GetRandomShard() *ConcurrentMapShared {
	return cm.Shared[rand.Intn(len(cm.Shared))]
}

func NewCollection(name string, files []*os.File, indexes []*Index, sd map[string]int) *Collection {
	return &Collection{name, NewConcurrentMap(files), indexes, sd, sync.Mutex{}, 0}
}

func (c *Collection) Size() uint64 {
	return atomic.LoadUint64(&c.ObjectsCounter)
}

func (c *Collection) Write(payload interface{}) error {
	id, objId, err := c.Map.Set(c.Name, nil, payload)
	if err != nil {
		return err
	}

	c.mx.Lock()
	c.ShardDestinations[objId] = id
	c.mx.Unlock()

	atomic.AddUint64(&c.ObjectsCounter, 1)

	return nil
}

func (c *Collection) FindById(id string) (*Element, error) {
	data, err := c.Map.FindById(c.Map.Shared[c.ShardDestinations[id]], c.Name, id)
	if err != nil {
		return nil, err
	}
	e := new(Element)
	return e, json.Unmarshal(data, e)
}

// c1.get.