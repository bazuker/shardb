package db

import (
	"errors"
	"io/ioutil"
	"math/rand"
	"os"
	"strconv"
	"strings"
	"sync"
)

// A "thread" safe string to anything map.
type ConcurrentMapShared struct {
	Id         int                     `json:"id"`
	Items      map[string]*ShardOffset `json:"items"`
	Capacities map[string]int          `json:"enum"`
	file       *os.File                `json:"-"`

	mx sync.RWMutex // Read Write mutex, guards access to internal map.

	SyncDestination string
}

func NewConcurrentMapShared(syncDest string, id int, f *os.File) *ConcurrentMapShared {
	return &ConcurrentMapShared{Id: id, Items: make(map[string]*ShardOffset), Capacities: make(map[string]int), file: f, SyncDestination: syncDest}
}

func (shard *ConcurrentMapShared) Lock() {
	shard.mx.Lock()
}

func (shard *ConcurrentMapShared) RLock() {
	shard.mx.RLock()
}

func (shard *ConcurrentMapShared) Unlock() {
	shard.mx.Unlock()
}

func (shard *ConcurrentMapShared) RUnlock() {
	shard.mx.RUnlock()
}

func (shard *ConcurrentMapShared) Sync() error {
	shard.mx.RLock()
	defer shard.mx.RUnlock()
	p := NewEncodedCompressedPackage(shard.SyncDestination + "/shard_" + strconv.Itoa(shard.Id) + "_meta.gob.gzip")
	p.SetData(shard)
	return p.Save()
}

func (shard *ConcurrentMapShared) applyOffset(move, after int64) {
	for _, item := range shard.Items {
		if item.Start > after {
			item.Start -= move
		}
	}
}

func (shard *ConcurrentMapShared) adjustCapacity(key string) {
	fullKey := "n:" + key
	if item, ok := shard.Capacities[fullKey]; ok {
		item--
		if item <= 0 {
			shard.DeleteCapacityKey(key)
			return
		}
		shard.Capacities[fullKey] = item
	}
}

func (shard *ConcurrentMapShared) Optimize() (int64, error) {
	shard.mx.Lock()
	defer shard.mx.Unlock()

	fi, err := shard.file.Stat()
	if err != nil {
		return 0, err
	}

	_, err = shard.file.Seek(0, 0)
	if err != nil {
		return 0, err
	}

	// load the whole shard into the memory
	shardData := make([]byte, fi.Size())
	_, err = shard.file.Read(shardData)
	if err != nil {
		return 0, err
	}
	// closing the file and flushing the data
	shard.file.Close()
	buffer := NewSuperBuffer(shardData)

	// redistribute the data
	pos := 0
	counter := int64(0)
	for key, item := range shard.Items {
		if item.Deleted {
			buffer.Cut(item.Start, item.Length)
			shard.applyOffset(int64(item.Length), item.Start)
			// adjust the capacity of a set
			pos = strings.Index(key, ":")
			if pos >= 0 {
				shard.adjustCapacity(key[pos+1:])
			}
			delete(shard.Items, key)
			counter += int64(item.Length)
		}
	}

	fName := shard.SyncDestination + "/" + fi.Name()
	err = ioutil.WriteFile(fName, buffer.Bytes(), os.ModePerm)
	if err != nil {
		return 0, err
	}

	shardData = nil
	buffer = nil
	shard.file, err = os.OpenFile(fName, os.O_RDWR, os.ModePerm)
	return counter, err
}

//! Not intended to be used in production environment
func (shard *ConcurrentMapShared) GetRandomItem() (string, *ShardOffset, error) {
	ln := len(shard.Items)
	if ln <= 0 {
		return "", nil, errors.New("shard is empty")
	}
	s, i := shard.GetItemWithNumber(rand.Intn(len(shard.Items)))
	return s, i, nil
}

//! Not intended to be used in production environment
func (shard *ConcurrentMapShared) GetItemWithNumber(n int) (string, *ShardOffset) {
	i := 0
	for k, v := range shard.Items {
		if i >= n {
			return k, v
		}
		i++
	}
	return "", nil
}

func (shard *ConcurrentMapShared) GetItem(id string) *ShardOffset {
	return shard.Items[id]
}

func (shard *ConcurrentMapShared) SetCapacityKey(key string, n int) {
	shard.Capacities["n:"+key] = n
}

func (shard *ConcurrentMapShared) GetCapacityKey(key string) int {
	n, ok := shard.Capacities["n:"+key]
	if !ok {
		return 0
	}
	return n
}

func (shard *ConcurrentMapShared) DeleteCapacityKey(key string) {
	delete(shard.Capacities, "n:"+key)
}
