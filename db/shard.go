package db

import (
	"os"
	"sync"
	"errors"
	"math/rand"
	"io/ioutil"
	"strconv"
	"bytes"
	"encoding/gob"
)

// A "thread" safe string to anything map.
type ConcurrentMapShared struct {
	Id    int                    `json:"id"`
	Items map[string]interface{} `json:"items"`
	file  *os.File               `json:"-"`

	mx 	  sync.RWMutex // Read Write mutex, guards access to internal map.

	SyncDestination string
}

func NewConcurrentMapShared(syncDest string, id int, f *os.File) *ConcurrentMapShared {
	return &ConcurrentMapShared{Id: id, Items: make(map[string]interface{}), file: f, SyncDestination: syncDest}
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
	/*data, err := json.Marshal(shard)
	if err != nil {
		return err
	}*/
	var data bytes.Buffer
	enc := gob.NewEncoder(&data)
	err := enc.Encode(shard)
	if err != nil {
		return err
	}
	err = ioutil.WriteFile(shard.SyncDestination+ "/shard_" + strconv.Itoa(shard.Id) + "_meta.gob", data.Bytes(), os.ModePerm)
	shard.mx.RUnlock()

	return err
}

//! Not intended to be used in production environment
func (shard *ConcurrentMapShared) GetRandomItem() (string, interface{}, error) {
	ln := len(shard.Items)
	if ln <= 0 {
		return "", nil, errors.New("shard is empty")
	}

	s, i := shard.GetItemWithNumber(rand.Intn(len(shard.Items)))
	return s, i, nil
}
//! Not intended to be used in production environment
func (shard *ConcurrentMapShared) GetItemWithNumber(n int) (string, interface{}) {
	i := 0
	for k, v := range shard.Items {
		if i >= n {
			return k, v
		}
		i++
	}
	return "", nil
}

func (shard *ConcurrentMapShared) GetItem(id string) interface{} {
	return shard.Items[id]
}
