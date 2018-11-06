package storage

import (
	"fmt"
	"github.com/xlwh/tsdb-engine/g"
	"sync"
	"time"
	"os"
	"bufio"
	"encoding/json"
	"strings"
)

type MemIndex struct {
	data    map[string]*g.List
	dataDir string
	lock    sync.RWMutex
	option  *g.Option
}

type IndexItem struct {
	BlockName string
	STime     int64
	ETime     int64
}

func NewMemIndex(option  *g.Option) *MemIndex {

	return nil
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
	fileName := fmt.Sprintf("%s/%s", b.dataDir, "INDEX")
	var err error
	var file *os.File

	if checkFileIsExist(fileName) {
		file, err = os.OpenFile(fileName, os.O_CREATE, 0666)
		if err != nil {
			// TODO 处理异常
			return
		}
	} else {
		file, err = os.Create(fileName)
		if err != nil {
			// TODO 处理异常
			return
		}
	}

	defer file.Close()
	writer := bufio.NewWriter(file)

	b.lock.RLock()
	for k, v := range b.data {
		items := make([]*IndexItem, 0)
		for e := v.Front(); e != nil; e = e.Next() {
			item, ok := (e.Value).(*IndexItem)
			if ok {
				items = append(items, item)
			}
		}

		val, _ := json.Marshal(items)
		data := fmt.Sprintf("%s=%s", k, string(val))

		if len(items) > 0 {
			writer.Write([]byte(data))
			writer.Write([]byte("\n"))
		}
	}
	b.lock.RUnlock()
	writer.Flush()
}

func (b *MemIndex) loadIndex() {
	fileName := fmt.Sprintf("%s/%s", b.option.DataDir, "INDEX")
	file, err := os.Open(fileName)
	if err != nil {
		// TODO 处理异常
		return
	}

	defer file.Close()
	r := bufio.NewReader(file)

	for {
		data, _, err := r.ReadLine()
		if err != nil {
			break
		}
		xs := strings.Split(string(data), "=")
		if len(xs) < 2 {
			continue
		}
		var items []IndexItem
		err = json.Unmarshal([]byte(xs[1]), &items)
		if err != nil {
			// TODO 处理异常
			continue
		}

		for _, item := range items {
			b.AddIndex(xs[0], item.STime, item.ETime)
		}
	}

	// 加载完毕就可以先删除了这个文件
	os.Remove(fileName)
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
