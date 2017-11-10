package db

import (
	"encoding/json"
	"sync"
	"sync/atomic"
	"os"
	"io/ioutil"
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
	Map               *ConcurrentMap	`json:"-"`
	Indexes           []*Index

	ShardDestinations map[string]int	`json:"dests"`
	mx                sync.Mutex		`json:"-"`

	ObjectsCounter  uint64 `json:"objects"`
	SyncDestination string `json:"sync_dest"`
}

type Element struct {
	Id string `json:"x"`
	Payload interface{} `json:"p"`
}

func NewCollection(path, name string, files []*os.File, indexes []*Index, sd map[string]int) *Collection {
	return &Collection{name, NewConcurrentMap(path, files),
	indexes, sd, sync.Mutex{}, 0, path}
}

func (c *Collection) Sync() (err error) {
	c.mx.Lock()
	defer c.mx.Unlock()

	err = c.Map.Sync()
	if err != nil {
		return err
	}

	data, err := json.Marshal(c)
	if err != nil {
		return err
	}

	return ioutil.WriteFile(c.SyncDestination+ "/" + c.Name + ".json", data, os.ModePerm)
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