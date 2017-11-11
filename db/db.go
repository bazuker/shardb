package db

import (
	"sync"
	"errors"
	"os"
	"strconv"
	"math/rand"
	"io/ioutil"
	"strings"
	"encoding/json"
	"log"
	"bufio"
	"encoding/gob"
)

const COLLECTION_DIR_NAME = "collections"

type Database struct {
	Name            string					`json:"name"`
	collections     map[string]*Collection	`json:"-"`
	collectionMutex sync.RWMutex			`json:"-"`
}

func NewDatabase(name string) *Database {
	gob.RegisterName("so", &ShardOffset{})

	return &Database{name, make(map[string]*Collection), sync.RWMutex{}}
}

func createUniqueIdIndex() []*Index {
	indexes := make([]*Index, 1)
	indexes[0] = &Index{"id", true}
	return indexes
}

func (db *Database) ScanAndLoadData() error {
	_, err := os.Stat(COLLECTION_DIR_NAME)
	if os.IsNotExist(err) {
		return errors.New("collections folder does not exist")
	}

	collections, err := ioutil.ReadDir(COLLECTION_DIR_NAME)
	if err != nil {
		return err
	}

	for _, c := range collections {
		if c.IsDir() {
			collectionPath := COLLECTION_DIR_NAME + "/" + c.Name()

			collectionFiles, err := ioutil.ReadDir(collectionPath)
			if err != nil {
				return err
			}

			cfLen := len(collectionFiles)
			if cfLen < SHARD_COUNT {
				return errors.New("collection has invalid amount of shards " + strconv.Itoa(cfLen) + ". Expected " + strconv.Itoa(SHARD_COUNT))
			}

			var collection *Collection
			loaded := 0
			files := make([]*os.File, SHARD_COUNT)
			cm := NewConcurrentMap(collectionPath, files)
			cNameExt := c.Name() + ".json"
			mapIndexLoaded := false

			for _, f := range collectionFiles {
				fName := f.Name()
				if strings.HasPrefix(fName, "shard_") {
					// loading the shard main data
					if strings.HasSuffix(fName, ".jsonlist") {
						fi, err := os.Open(collectionPath + "/" + fName)
						if err != nil {
							return errors.New("collection (" + fName + ") shard (" + fName + ")is unavailable")
						}
						files[loaded] = fi
						// loading the meta
						fName := strings.TrimSuffix(fName, ".jsonlist") + "_meta.gob"
						fo, err := os.Open(collectionPath + "/" + fName)
						if err != nil {
							return err
						}
						var shard ConcurrentMapShared
						dec := gob.NewDecoder(fo)
						if err != nil {
							return err
						}
						err = dec.Decode(&shard)
						if err != nil {
							return errors.New("shard decoding failed " + err.Error())
						}
						shard.file = fi
						cm.Shared[shard.Id] = &shard

						loaded++
					}

				// loading the map index
				} else if f.Name() == "map.index" {
					inFile, _ := os.Open(collectionPath + "/" + fName)
					scanner := bufio.NewScanner(inFile)
					scanner.Split(bufio.ScanLines)
					// current map index
					if scanner.Scan() {
						num, err := strconv.ParseUint(scanner.Text(), 10, 64)
						if err != nil {
							return err
						}
						cm.SetCounterIndex(num)
					}
					// sync path
					if scanner.Scan() {
						cm.SyncDestination = scanner.Text()
					}
					inFile.Close()
					mapIndexLoaded = true

				// loading the collection's description
				} else if f.Name() == cNameExt {
					data, err := ioutil.ReadFile(collectionPath + "/" + cNameExt)
					if err != nil {
						return err
					}
					collection = new(Collection)
					err = json.Unmarshal(data, collection)
					if err != nil {
						return err
					}

				}
			}

			if !mapIndexLoaded {
				return errors.New("map index file was not loaded")
			}
			if collection == nil {
				return errors.New("collection description file missing")
			}

			collection.Map = cm

			db.collectionMutex.Lock()
			db.collections[c.Name()] = collection
			db.collectionMutex.Unlock()

			if loaded < SHARD_COUNT {
				return errors.New("collection " + c.Name() + " files are corrupted")
			}
		}
	}

	return nil
}

func (db *Database) Sync() error {
	db.collectionMutex.RLock()
	wg := sync.WaitGroup{}
	wg.Add(len(db.collections))
	for _, c := range db.collections {
		go func() {
			err := c.Sync()
			if err != nil {
				log.Println("Collection " + c.Name + " syncronization failed:", err.Error())
			}
			wg.Done()
		}()
	}
	db.collectionMutex.RUnlock()

	wg.Wait()

	data, err := json.Marshal(db)
	if err != nil {
		return err
	}

	return ioutil.WriteFile(db.Name + ".shardb", data, os.ModePerm)
}

func (db *Database) GetCollectionsCount() int {
	return len(db.collections)
}

func (db *Database) GetTotalObjectsCount() uint64 {
	db.collectionMutex.RLock()
	defer db.collectionMutex.RUnlock()

	total := uint64(0)
	for _, v := range db.collections {
		total += v.Size()
	}

	return total
}

func (db *Database) GetRandomCollection() *Collection {
	db.collectionMutex.RLock()
	defer db.collectionMutex.RUnlock()

	n := rand.Intn(len(db.collections))
	i := 0
	for _, v := range db.collections {
		if i == n {
			return v
		}
		i++
	}
	return nil
}

func (db *Database) GetRandomAliveObject() (string, interface{}, error) {
	c := db.GetRandomCollection()
	if c == nil {
		return "", nil, errors.New("database does not have any collections")
	}
	shard := c.Map.GetRandomShard()
	if shard == nil {
		return "", nil, errors.New("collections does not have any shards")
	}
	return shard.GetRandomItem()
}

func (db *Database) AddCollection(name string) error {
	if db.GetCollection(name) != nil {
		return errors.New("collection is already exist")
	}

	files := make([]*os.File, SHARD_COUNT)
	path := COLLECTION_DIR_NAME + "/" + name
	os.MkdirAll(path, os.ModePerm)
	for i := 0; i < SHARD_COUNT; i++ {
		f, err := os.Create(path + "/shard_" + strconv.Itoa(i) + ".jsonlist")
		if err != nil {
			return errors.New("failed to create a shard")
		}
		files[i] = f
	}

	db.collectionMutex.Lock()
	db.collections[name] = NewCollection(path, name, NewConcurrentMap(path, files), createUniqueIdIndex(), make(map[string]int))
	db.collectionMutex.Unlock()

	return nil
}

func (db *Database) GetCollection(name string) *Collection {
	db.collectionMutex.RLock()
	c := db.collections[name]
	db.collectionMutex.RUnlock()

	return c
}

func (db *Database) DropCollection(name string) {
	db.collectionMutex.Lock()
	delete(db.collections, name)
	db.collectionMutex.Unlock()
}