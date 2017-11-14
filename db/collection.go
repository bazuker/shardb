package db

import (
	"encoding/json"
	"sync"
	"sync/atomic"
	"errors"
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
	Name              string			`json:"name"`
	Map               *ConcurrentMap	`json:"-"`
	Indexes           []*Index

	ShardDestinations map[string]int	`json:"dests"`
	sharedDestMx      sync.Mutex		`json:"-"`

	ObjectsCounter  uint64 `json:"objects"`
	SyncDestination string `json:"sync_dest"`
}

type Element struct {
	Id string `json:"x"`
	Payload interface{} `json:"p"`
}

func NewCollection(path, name string, cm *ConcurrentMap, indexes []*Index, sd map[string]int) *Collection {
	return &Collection{name, cm,indexes, sd, sync.Mutex{}, 0, path}
}

func (c *Collection) GetRandomAliveObject() (string, interface{}, error) {
	shard := c.Map.GetRandomShard()
	if shard == nil {
		return "", nil, errors.New("collections does not have any shards")
	}
	return shard.GetRandomItem()
}

func (c *Collection) Sync() (err error) {
	err = c.Map.Flush()
	if err != nil {
		// very critical error
		return err
	}

	err = c.Map.Sync()
	if err != nil {
		return err
	}

	c.sharedDestMx.Lock()
	defer c.sharedDestMx.Unlock()

	data, err := json.Marshal(c)
	if err != nil {
		return err
	}
	p := NewCompressedPackage(c.SyncDestination+ "/" + c.Name + ".json.gzip")
	p.SetData(data)

	return p.Save()
}

func (c *Collection) Size() uint64 {
	return atomic.LoadUint64(&c.ObjectsCounter)
}

func (c *Collection) Write(payload interface{}) error {
	id, objId, err := c.Map.Set(nil, payload)
	if err != nil {
		return err
	}

	c.sharedDestMx.Lock()
	c.ShardDestinations[objId] = id
	c.sharedDestMx.Unlock()

	atomic.AddUint64(&c.ObjectsCounter, 1)

	return nil
}

func (c *Collection) FindById(id string) (*Element, error) {
	data, err := c.Map.FindById(c.Map.Shared[c.ShardDestinations[id]], id)
	if err != nil {
		return nil, err
	}
	e := new(Element)
	return e, json.Unmarshal(data, e)
}