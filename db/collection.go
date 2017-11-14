package db

import (
	"bytes"
	"encoding/gob"
	"encoding/json"
	"errors"
	"github.com/allegro/bigcache"
	"sync"
	"sync/atomic"
	"time"
)

type Index struct {
	Field  string
	Unique bool
}

type FullDataIndex struct {
	Field  string
	Data   string
	Unique bool
}

type Collection struct {
	Name    string         `json:"name"`
	Map     *ConcurrentMap `json:"-"`
	Cache   *bigcache.BigCache
	Indexes []*Index

	ShardDestinations map[string]*int `json:"dests"`
	sharedDestMx      sync.RWMutex    `json:"-"`

	ObjectsCounter  uint64 `json:"objects"`
	SyncDestination string `json:"sync_dest"`
}

type Element struct {
	Id      string      `json:"x"`
	Payload interface{} `json:"p"`
}

func NewCollectionCache() *bigcache.BigCache {
	config := bigcache.Config{
		// number of shards (must be a power of 2)
		Shards: 1024,
		// time after which entry can be evicted
		LifeWindow: 10 * time.Minute,
		// rps * lifeWindow, used only in initial memory allocation
		MaxEntriesInWindow: 1000 * 10 * 60,
		// max entry size in bytes, used only in initial memory allocation
		MaxEntrySize: 512,
		// prints information about additional memory allocation
		Verbose: true,
		// cache will not allocate more memory than this limit, value in MB
		// if value is reached then the oldest entries can be overridden for the new ones
		// 0 value means no size limit
		HardMaxCacheSize: int(GetFreeMemory() / 4),
		// callback fired when the oldest entry is removed because of its
		// expiration time or no space left for the new entry. Default value is nil which
		// means no callback and it prevents from unwrapping the oldest entry.
		OnRemove: nil,
	}

	bc, _ := bigcache.NewBigCache(config)
	return bc
}

func NewCollection(path, name string, cm *ConcurrentMap, indexes []*Index, sd map[string]*int) *Collection {
	return &Collection{name, cm, NewCollectionCache(), indexes,
		sd, sync.RWMutex{}, 0, path}
}

func (c *Collection) GetRandomAliveObject() (string, *Element, error) {
	shard := c.Map.GetRandomShard()
	if shard == nil {
		return "", nil, errors.New("collections does not have any shards")
	}

	key, offset, err := shard.GetRandomItem()
	if err != nil {
		return "", nil, err
	}

	data, err := c.Map.ReadAtOffset(shard, offset)
	if err != nil {
		return "", nil, err
	}
	e, err := c.DecodeElement(data)
	return key, e, err
}

func (c *Collection) DecodeElement(data []byte) (*Element, error) {
	e := new(Element)
	return e, gob.NewDecoder(bytes.NewReader(data)).Decode(e)
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
	p := NewCompressedPackage(c.SyncDestination + "/" + c.Name + ".json.gzip")
	p.SetData(data)

	return p.Save()
}

func (c *Collection) Size() uint64 {
	return atomic.LoadUint64(&c.ObjectsCounter)
}

func (c *Collection) Write(payload CustomStructure) error {
	destMap, err := c.Map.Set(payload.GetDataIndex(), payload)
	if err != nil {
		return err
	}

	c.sharedDestMx.Lock()
	for k, v := range destMap {
		c.ShardDestinations[k] = v
	}
	c.sharedDestMx.Unlock()

	destMap = nil
	atomic.AddUint64(&c.ObjectsCounter, 1)

	return nil
}

func (c *Collection) getShardByKey(key string) *ConcurrentMapShared {
	c.sharedDestMx.RLock()
	defer c.sharedDestMx.RUnlock()
	return c.Map.Shared[*c.ShardDestinations[key]]
}

func (c *Collection) scanDataByUniqueIndex(entry CustomStructure, index *FullDataIndex) ([]byte, error) {
	shard := c.getShardByKey(index.Field + ":" + index.Data)
	return c.Map.FindByUniqueKey(shard, index.Field, index.Data)
}

func (c *Collection) scanDataByIndex(entry CustomStructure, index *FullDataIndex, limit int) ([][]byte, error) {
	data, err := c.Map.FindByKey(index.Field, index.Data, limit)
	if err != nil {
		return nil, err
	} else if len(data) == 0 {
		return nil, errors.New("zero results")
	}
	return data, nil
}

func (c *Collection) FindById(id string) ([]byte, error) {
	data, err := c.Cache.Get("id:" + id)
	if err == nil {
		return data, nil
	}

	shard := c.getShardByKey(id)
	data, err = c.Map.FindById(shard, id)
	if err != nil {
		return nil, err
	}
	c.Cache.Set("id:"+id, data)

	return data, nil
}

func (c *Collection) Scan(entry CustomStructure) ([]byte, error) {
	indexes := entry.GetDataIndex()
	for _, ix := range indexes {
		if ix.Data == "" {
			continue
		}
		if ix.Unique {
			return c.scanDataByUniqueIndex(entry, ix)
		} else {
			data, err := c.scanDataByIndex(entry, ix, 1)
			if err != nil {
				return nil, err
			}
			return data[0], nil
		}
	}
	return nil, errors.New("no matching data")
}

func (c *Collection) ScanN(entry CustomStructure, limit int) ([][]byte, error) {
	indexes := entry.GetDataIndex()
	for _, ix := range indexes {
		if ix.Data == "" {
			continue
		}
		if ix.Unique {
			data, err := c.scanDataByUniqueIndex(entry, ix)
			if err != nil {
				return nil, err
			}
			return [][]byte{data}, nil
		} else {
			return c.scanDataByIndex(entry, ix, limit)
		}
	}
	return nil, errors.New("no matching data")
}

func (c *Collection) ScanAll(entry CustomStructure) ([][]byte, error) {
	const limit = 1000
	return c.ScanN(entry, limit)
}
