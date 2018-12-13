package storage

import (
	"errors"
	"fmt"
	log "github.com/cihub/seelog"
	"github.com/syndtr/goleveldb/leveldb/opt"
	"github.com/xlwh/tsdb-engine/cs/simple"
	"github.com/xlwh/tsdb-engine/cs/statistics"
	"github.com/xlwh/tsdb-engine/g"
	"strings"
	"sync"
	"time"
)

type MemTable struct {
	memData map[string]*SeriesData
	option  *g.Option

	lock sync.RWMutex
}

func NewMemtable(option *g.Option) (*MemTable, error) {
	mem := &MemTable{
		memData: make(map[string]*SeriesData),
		option:  option,
	}
	return mem, nil
}

func (m *MemTable) PutStatistics(key string, t int64, cnt, sum, max, min float64) error {
	m.lock.Lock()
	defer m.lock.Unlock()

	if series, found := m.memData[key]; found {
		return series.put(t, cnt, sum, max, min)
	} else {
		s := newSeriesData(key, m.option.PointNumEachBlock)
		m.memData[key] = s
		return s.put(t, cnt, sum, max, min)
	}
}

func (m *MemTable) PutSimple(key string, t int64, v float64) error {
	m.lock.Lock()
	defer m.lock.Unlock()

	if series, found := m.memData[key]; found {
		return series.putSimple(t, v)
	} else {
		s := newSeriesData(key, m.option.PointNumEachBlock)
		m.memData[key] = s
		return s.putSimple(t, v)
	}
}

func (m *MemTable) Sync(force bool) {
	m.lock.RLock()
	defer m.lock.RUnlock()

	// 强制把当前的流式压缩关闭
	if force {
		for _, v := range m.memData {
			v.Sync(force)
		}
	}

	// 刷新索引数据到持久化存储
	index.Flush(force)
}

func (m *MemTable) Gc() {
	m.lock.RLock()
	defer m.lock.RUnlock()

	for _, v := range m.memData {
		v.gc(m.option.ExpireTime)
	}
}

func (m *MemTable) SeriesReader(key string) (*SeriesData, error) {
	m.lock.RLock()
	defer m.lock.RUnlock()
	if series, found := m.memData[key]; found {
		return series, nil
	}

	return nil, nil
}

type SeriesData struct {
	key       string
	simpleCs  *simple.Series
	statisCs  *statistics.Series
	indexItem *IndexItem

	lock     sync.RWMutex
	blockMap map[string]*g.DataBlock

	PointNum    int64
	MaxPointNum int64

	sTime int64
	eTime int64
}

func newSeriesData(key string, maxCnt int64) *SeriesData {
	index := GetIndex()
	if index != nil {
		idx := index.AddIndexItem(key)
		return &SeriesData{
			key:         key,
			indexItem:   idx,
			PointNum:    0,
			MaxPointNum: maxCnt,
			blockMap:    make(map[string]*g.DataBlock),

			sTime: -1,
			eTime: -1,
		}
	}

	return nil
}

func (s *SeriesData) put(t int64, cnt, sum, max, min float64) error {
	if cnt == 1 && sum == max && max == min {
		return s.putSimple(t, sum)
	} else {
		return s.putStatistics(t, cnt, sum, max, min)
	}
}

func (s *SeriesData) gc(expireTime int64) {
	s.lock.Lock()
	defer s.lock.Unlock()
	now := time.Now().UnixNano() / 1e6

	toDelete := make([]string, 0)

	for k, v := range s.blockMap {
		if now >= v.ETime+expireTime {
			toDelete = append(toDelete, k)
		}
	}

	for _, name := range toDelete {
		// delete in mem
		delete(s.blockMap, name)
		// delete in disk
		error := StorageInstance.DeleteBlock(name)
		if error != nil {
			log.Warnf("Delete data in disk error.%v", error)
		}
	}

	s.indexItem.gc(expireTime)
}

func (s *SeriesData) putStatistics(t int64, cnt, sum, max, min float64) error {
	if s.Sync(false) {
		s.statisCs = statistics.New(t)
	}

	if s.statisCs == nil {
		s.statisCs = statistics.New(t)
	}

	error := s.statisCs.Push(t, cnt, sum, max, min)
	if error != nil {
		return error
	}

	if s.sTime > t {
		s.sTime = t
	}
	if s.eTime < t {
		s.eTime = t
	}
	s.PointNum++

	s.indexItem.UpdateCsRange(t)

	return nil
}

func (s *SeriesData) putSimple(t int64, v float64) error {
	if s.Sync(false) {
		s.simpleCs = simple.New(t)
	}

	if s.simpleCs == nil {
		s.simpleCs = simple.New(t)
	}

	error := s.simpleCs.Push(t, v)
	if error != nil {
		return error
	}

	if s.sTime > t || s.sTime == -1 {
		s.sTime = t
	}
	if s.eTime < t || s.eTime == -1 {
		s.eTime = t
	}
	s.PointNum++

	index := GetIndex()
	if index != nil {
		idx, err := index.GetIndexItem(s.key)
		if err == nil {
			idx.UpdateCsRange(t)
		}
	}

	//s.indexItem.UpdateCsRange(t)

	return nil
}

func (s *SeriesData) getIndex() (*IndexItem, error) {
	index := GetIndex()
	if index != nil {
		idx, err := index.GetIndexItem(s.key)
		if err == nil {
			return idx, nil
		}
	}

	return nil, errors.New("No index")
}

func (s *SeriesData) Sync(force bool) bool {
	wo := &opt.WriteOptions{}
	// 强制刷到磁盘
	if force {
		wo.Sync = true
	}

	if s.PointNum >= s.MaxPointNum || force {
		if s.statisCs != nil {
			s.statisCs.Finish()
		}

		if s.simpleCs != nil {
			s.simpleCs.Finish()
		}

		synced := true
		blocks := make([]string, 0)

		if s.simpleCs != nil && s.simpleCs.Len() > 0 {
			name := fmt.Sprintf("%s_simple_index_%d", s.key, time.Now().UnixNano()/1e6)
			data := s.simpleCs.Bytes()

			block := &g.DataBlock{
				Name:  name,
				STime: s.sTime,
				ETime: s.eTime,
				Data:  data,
			}

			s.lock.Lock()
			s.blockMap[name] = block
			s.lock.Unlock()
			err := StorageInstance.Put([]byte(name), data, wo)
			if err != nil {
				log.Warnf("Error to write block.%v", err)
				synced = false
			} else {
				blocks = append(blocks, name)
			}
		}

		if s.statisCs != nil && s.statisCs.Len() > 0 {
			name := fmt.Sprintf("%s_statis_index_%d", s.key, time.Now().UnixNano()/1e6)
			data := s.statisCs.Bytes()

			block := &g.DataBlock{
				Name:  name,
				STime: s.sTime,
				ETime: s.eTime,
				Data:  data,
			}

			s.lock.Lock()
			s.blockMap[name] = block
			s.lock.Unlock()

			err := StorageInstance.Put([]byte(name), data, wo)
			if err != nil {
				synced = false
			} else {
				blocks = append(blocks, name)
			}
		}

		// 只有数据成功写入了磁盘，才更新索引信息
		if synced {
			for _, name := range blocks {
				idx, err := s.getIndex()
				if err != nil {
					log.Warnf("Index not found")
					continue
				}
				idx.PutBlock(name, s.sTime, s.eTime)
			}
			s.indexItem.UpdateCsRange(-1)
		}

		s.PointNum = 0
		return true
	} else {
		return false
	}
}

func (s *SeriesData) ReadCsSimple(start, end int64) ([]*g.SimpleDataPoint, error) {
	it := s.simpleCs.Iter()
	points := make([]*g.SimpleDataPoint, 0)
	for it.Next() {
		t, v := it.Values()
		if t >= start && t <= end {
			points = append(points, &g.SimpleDataPoint{s.key, t, v})
		}
	}

	return points, nil
}

func (s *SeriesData) ReadSimpleBlocks(names []string, start, end int64) ([]*g.SimpleDataPoint, error) {
	s.lock.RLock()
	defer s.lock.RUnlock()
	points := make([]*g.SimpleDataPoint, 0)

	for _, name := range names {
		if block, found := s.blockMap[name]; found {
			it, err := simple.NewIterator(block.Data)
			if err != nil {
				log.Warnf("Parse block error.%v", err)
				continue
			}
			for it.Next() {
				t, v := it.Values()
				if t >= start && t <= end {
					points = append(points, &g.SimpleDataPoint{s.key, t, v})
				}
			}
		}
	}

	return points, nil
}

func (s *SeriesData) ReadCs(start, end int64) ([]*g.DataPoint, error) {
	points := make([]*g.DataPoint, 0)
	if s.simpleCs != nil {
		it := s.simpleCs.Iter()
		for it.Next() {
			t, v := it.Values()
			if t >= start && t <= end {
				points = append(points, &g.DataPoint{s.key, t, int64(v), float64(v), float64(v), float64(v)})
			}
		}
	}

	if s.statisCs != nil {
		it := s.statisCs.Iter()
		for it.Next() {
			t, cnt, sum, max, min := it.Values()
			if t >= start && t <= end {
				points = append(points, &g.DataPoint{s.key, t, int64(cnt), sum, max, min})
			}
		}
	}

	return points, nil
}

func (s *SeriesData) ReadBlocks(names []string, start, end int64) ([]*g.DataPoint, error) {
	s.lock.RLock()
	defer s.lock.RUnlock()

	points := make([]*g.DataPoint, 0)

	for _, name := range names {
		if block, found := s.blockMap[name]; found {
			if strings.Contains(block.Name, "simple") {
				it, err := simple.NewIterator(block.Data)
				if err != nil {
					log.Warnf("Parse block error.%v", err)
					continue
				}
				for it.Next() {
					t, v := it.Values()
					if t >= start && t <= end {
						points = append(points, &g.DataPoint{s.key, t, 1, float64(v), float64(v), float64(v)})
					}
				}
			} else {
				it, err := statistics.NewIterator(block.Data)
				if err != nil {
					log.Warnf("Parse block error.%v", err)
					continue
				}
				for it.Next() {
					t, cnt, sum, max, min := it.Values()
					if t >= start && t <= end {
						points = append(points, &g.DataPoint{s.key, t, int64(cnt), sum, max, min})
					}
				}
			}
		}
	}

	return points, nil
}
