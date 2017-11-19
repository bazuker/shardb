package db

import (
	"errors"
	"io/ioutil"
	"math/rand"
	"os"
	"strconv"
	"sync"
)

// A "thread" safe string to anything map.
type ConcurrentMapShared struct {
	Id          int                     `json:"id"`
	Items       map[string]*ShardOffset `json:"items"`
	Enumerators map[string]int          `json:"enum"`
	file        *os.File                `json:"-"`

	mx sync.RWMutex // Read Write mutex, guards access to internal map.

	SyncDestination string
}

func NewConcurrentMapShared(syncDest string, id int, f *os.File) *ConcurrentMapShared {
	return &ConcurrentMapShared{Id: id, Items: make(map[string]*ShardOffset), Enumerators: make(map[string]int), file: f, SyncDestination: syncDest}
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
	p := NewEncodedCompressedPackage(shard.SyncDestination + "/shard_" + strconv.Itoa(shard.Id) + "_meta.gob.gzip")
	p.SetData(shard)
	err := p.Save()
	if err != nil {
		return err
	}
	shard.mx.RUnlock()

	return err
}

func (shard *ConcurrentMapShared) applyOffset(move, after int64) {
	for _, item := range shard.Items {
		if !item.Deleted && item.Start > after {
			item.Start -= move
		}
	}
}

func (shard *ConcurrentMapShared) Optimize() error {
	shard.mx.Lock()
	defer shard.mx.Unlock()

	fi, err := shard.file.Stat()
	if err != nil {
		return err
	}

	// load the whole shard into the memory
	_, err = shard.file.Seek(0, 0)
	if err != nil {
		return err
	}

	shardData := make([]byte, fi.Size())
	_, err = shard.file.Read(shardData)
	if err != nil {
		return err
	}
	shard.file.Close()

	// redistribute the data
	for key, item := range shard.Items {
		if item.Deleted {
			shardData = append(shardData[:item.Start], shardData[item.Start+int64(item.Length):]...)
			shard.applyOffset(int64(item.Length), item.Start)
			delete(shard.Items, key)
		}
	}

	fName := shard.SyncDestination + "/" + fi.Name()
	err = ioutil.WriteFile(fName, shardData, os.ModePerm)
	if err != nil {
		return err
	}

	shardData = nil
	shard.file, err = os.Open(fName)

	return err
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

func (shard *ConcurrentMapShared) SetEnumerator(key string, n int) {
	shard.Enumerators["n:"+key] = n
}

func (shard *ConcurrentMapShared) GetEnumerator(key string) int {
	n, ok := shard.Enumerators["n:"+key]
	if !ok {
		return 0
	}
	return n
}

func (shard *ConcurrentMapShared) DeleteEnumerator(key string) {
	delete(shard.Enumerators, "n:"+key)
}
