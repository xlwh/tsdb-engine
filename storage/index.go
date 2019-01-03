package storage

import (
	"encoding/json"
	"errors"
	"fmt"
	log "github.com/cihub/seelog"
	"github.com/syndtr/goleveldb/leveldb/opt"
	"sync"
	"time"
)

type Index struct {
	data  map[string]*IndexItem
	lock  sync.RWMutex
	WG    sync.WaitGroup
	store *Storage
}

func NewIndex(store *Storage) *Index {
	idx := &Index{
		data:  make(map[string]*IndexItem),
		store: store,
	}
	idx.WG.Add(2)
	// 加载索引数据
	return idx
}

func (i *Index) Load() {
	metaData, err := i.store.Get([]byte("meta"), nil)
	if err != nil {
		log.Warnf("No meta data")
		return
	}

	var meta []string
	err = json.Unmarshal(metaData, &meta)
	if err != nil {
		log.Warnf("Unmarshal meta data error.%v", err)
		return
	}

	for _, key := range meta {
		idxItem := NewIndexItem(key, i.store.option.UseMemCache)
		i.AddIndexItem(key, idxItem)

		idx := fmt.Sprintf("%s_index", key)
		indexData, err := i.store.Get([]byte(idx), nil)
		if err != nil {
			log.Warnf("Read index data error.%v", err)
			continue
		}

		var indexItem map[string]*BlockIndex = make(map[string]*BlockIndex)
		err = json.Unmarshal(indexData, &indexItem)
		if err != nil {
			log.Warnf("Unmarsha index data error.%v", err)
			continue
		}

		item, err := i.GetIndexItem(key)
		// 还原索引数据到内存,只更新磁盘索引
		if err == nil {
			for _, v := range indexItem {
				item.PutBlockToDisk(v.BlockName, v.STime, v.ETime)
			}
		}
	}
}

func (i *Index) GetIndexItem(uuid string) (*IndexItem, error) {
	i.lock.RLock()
	defer i.lock.RUnlock()

	if idx, found := i.data[uuid]; found {
		return idx, nil
	}

	return nil, errors.New("No Index")
}

func (i *Index) AddIndexItem(key string, idx *IndexItem) {
	i.lock.Lock()
	i.data[key] = idx
	i.lock.Unlock()
}

func (i *Index) Flush(force bool) {
	i.lock.RLock()
	defer i.lock.RUnlock()

	wo := &opt.WriteOptions{}
	// 强制刷到磁盘
	if force {
		wo.Sync = true
	}

	meta := make([]string, 0)
	for k, v := range i.data {
		data, err := v.Marshal()
		if err != nil {
			log.Warnf("Marshal index error:%v", err)
			continue
		}
		error := i.store.Put([]byte(fmt.Sprintf("%s_index", k)), []byte(data), wo)
		if err != nil {
			log.Warnf("Save index error.%v", error)
		}

		// 只数据成功写到LevelDB,才更新meta
		meta = append(meta, k)
	}

	data, err := json.Marshal(meta)
	if err != nil {
		log.Warnf("Marshal meta error:%v", err)
		return
	}

	error := i.store.Put([]byte("meta"), []byte(string(data)), wo)
	if err != nil {
		log.Warnf("Save meta error.%v", error)
	} else {
		log.Debugf("Success to write meta info")
	}

	if force {
		i.WG.Done()
	}
}

type IndexItem struct {
	Uuid      string `json:"uuid"`
	inCsStart int64
	inCsEnd   int64
	UseMemCache bool

	lock              sync.RWMutex
	memBlockIndexMap  map[string]*BlockIndex
	DiskBlockIndexMap map[string]*BlockIndex `data"`
}

type BlockIndex struct {
	BlockName string
	STime     int64
	ETime     int64
}

type PosInfo struct {
	Pos       string
	BlockName string
}

func NewIndexItem(uuid string, useCache bool) *IndexItem {
	return &IndexItem{
		Uuid:              uuid,
		inCsStart:         0,
		inCsEnd:           0,
		memBlockIndexMap:  make(map[string]*BlockIndex),
		DiskBlockIndexMap: make(map[string]*BlockIndex),
		UseMemCache: useCache,
	}
}

func (idx *IndexItem) PutBlock(name string, start, end int64) error {
	idx.lock.Lock()
	index := &BlockIndex{
		name,
		start,
		end,
	}

	if idx.UseMemCache {
		idx.memBlockIndexMap[name] = index
	}
	idx.DiskBlockIndexMap[name] = index

	idx.lock.Unlock()

	return nil
}

func (idx *IndexItem) PutBlockToDisk(name string, start, end int64) error {
	idx.lock.Lock()
	index := &BlockIndex{
		name,
		start,
		end,
	}

	idx.DiskBlockIndexMap[name] = index
	idx.lock.Unlock()

	return nil
}

func (idx *IndexItem) UpdateCsRange(t int64) {
	if t == -1 {
		idx.inCsStart = -1
		idx.inCsEnd = -1
		return
	}

	if idx.inCsStart == 0 {
		idx.inCsStart = t
	}

	if idx.inCsEnd == 0 {
		idx.inCsEnd = t
	}

	if idx.inCsStart > t {
		idx.inCsStart = t
	}

	if idx.inCsEnd < t {
		idx.inCsEnd = t
	}
}

func (idx *IndexItem) Pos(start, end int64) ([]*PosInfo, error) {
	pos := make([]*PosInfo, 0)
	if !(start > idx.inCsEnd || end < idx.inCsStart) {
		pos = append(pos, &PosInfo{Pos: "cs"})
	}

	idx.lock.RLock()
	found := make(map[string]*BlockIndex)
	for k, v := range idx.memBlockIndexMap {
		if !(start > v.ETime || end < v.STime) {
			found[k] = v
			pos = append(pos, &PosInfo{Pos: "mem", BlockName: v.BlockName})
		}
	}
	for k, v := range idx.DiskBlockIndexMap {
		if !(start > v.ETime || end < v.STime) {
			if _, ok := found[k]; !ok {
				pos = append(pos, &PosInfo{Pos: "disk", BlockName: v.BlockName})
			}
		}
	}
	idx.lock.RUnlock()
	return pos, nil
}

func (idx *IndexItem) gc(expireTime int64) {
	now := time.Now().UnixNano() / 1e6

	// Del index
	for _, v := range idx.memBlockIndexMap {
		if now >= v.ETime+expireTime * 1000 {
			delete(idx.memBlockIndexMap, v.BlockName)
			delete(idx.DiskBlockIndexMap, v.BlockName)
		}
	}
}

func (idx *IndexItem) Marshal() (string, error) {
	d, err := json.Marshal(idx.DiskBlockIndexMap)
	if err != nil {
		return "", err
	} else {
		return string(d), err
	}
}
