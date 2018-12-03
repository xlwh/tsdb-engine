package storage

import (
	"encoding/json"
	"fmt"
	log "github.com/cihub/seelog"
	"github.com/syndtr/goleveldb/leveldb"
	"github.com/xlwh/tsdb-engine/g"
	"os"
	"sync"
	"time"
)

type MemIndex struct {
	data   map[string]*g.List
	lock   sync.RWMutex
	option *g.Option
	db     *leveldb.DB
}

type IndexItem struct {
	BlockName string
	STime     int64
	ETime     int64
}

type KeyInfo struct {
	Keys []string
}

func NewMemIndex(option *g.Option, db *leveldb.DB) *MemIndex {
	index := &MemIndex{
		data:   make(map[string]*g.List),
		option: option,
		db:     db,
	}

	return index
}

func (b *MemIndex) Start() {
	// 启动加载索引
	b.loadIndex()
}

func (b *MemIndex) AddIndex(key string, sTime, eTime int64) string {
	b.lock.Lock()
	blockName := fmt.Sprintf("%s:%d-%d", key, sTime, eTime)
	newIndex := &IndexItem{blockName, sTime, eTime}

	if _, found := b.data[key]; found {
		b.data[key].PushBack(newIndex)
	} else {
		b.data[key] = g.NewList()
		b.data[key].PushBack(newIndex)
	}

	b.lock.Unlock()

	return blockName
}

func (b *MemIndex) GetBlocks(key string, sTime, eTime int64) []string {
	ret := make([]string, 0)
	b.lock.RLock()
	if index, found := b.data[key]; found {
		for e := index.Front(); e != nil; e = e.Next() {
			item, ok := (e.Value).(*IndexItem)
			if ok {
				if !(sTime > item.ETime || eTime < item.STime) {
					ret = append(ret, item.BlockName)
				}
			}
		}
	}
	b.lock.RUnlock()

	return ret
}

func (b *MemIndex) gc() []string {
	now := time.Now().UnixNano() / 1e6
	deleted := make([]string, 0)
	b.lock.Lock()
	for _, v := range b.data {

		for e := v.Front(); e != nil; e = e.Next() {
			item, ok := (e.Value).(*IndexItem)
			if ok {
				if now >= item.ETime+b.option.ExpireTime {
					v.Remove(e)
					deleted = append(deleted, item.BlockName)
				}
			}
		}
	}
	b.lock.Unlock()

	return deleted
}

func (b *MemIndex) saverIndex() {
	b.lock.RLock()

	keyInfo := &KeyInfo{
		make([]string, 0),
	}
	for k, v := range b.data {
		keyInfo.Keys = append(keyInfo.Keys, k)

		items := make([]*IndexItem, 0)
		for e := v.Front(); e != nil; e = e.Next() {
			item, ok := (e.Value).(*IndexItem)
			if ok {
				items = append(items, item)
			}
		}

		val, _ := json.Marshal(items)
		err := b.db.Put([]byte(fmt.Sprintf("idx-%s", k)), []byte(string(val)), nil)
		if err != nil {
			log.Warnf("Put index error:", err)
		}
	}

	key, _ := json.Marshal(keyInfo)
	err := b.db.Put([]byte("series"), []byte(string(key)), nil)
	if err != nil {
		log.Warnf("Put keys error")
	}

	b.lock.RUnlock()
}

func (b *MemIndex) loadIndex() {
	var keys KeyInfo
	keyData, err := b.db.Get([]byte("series"), nil)
	if err != nil {
		log.Warnf("Load index error.%v", err)
		return
	}
	err = json.Unmarshal(keyData, &keys)
	if err != nil {
		log.Warnf("Parse index error:%v", err)
		return
	}
	for _, key := range keys.Keys {
		data, err := b.db.Get([]byte(fmt.Sprintf("idx-%s", key)), nil)
		if err != nil {
			log.Warnf("Get index for %s error", key)
			continue
		}
		var items []IndexItem
		err = json.Unmarshal(data, &items)
		if err != nil {
			log.Warnf("Parse index file error:%v", err)
			continue
		}

		for _, item := range items {
			b.AddIndex(key, item.STime, item.ETime)
		}
	}
}

func (b *MemIndex) Stop() {
	// 持久化索引数据
	b.saverIndex()
}

func checkFileIsExist(filename string) bool {
	var exist = true
	if _, err := os.Stat(filename); os.IsNotExist(err) {
		exist = false
	}
	return exist
}
