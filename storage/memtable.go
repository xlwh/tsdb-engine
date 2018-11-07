package storage

import (
	"errors"
	log "github.com/cihub/seelog"
	"github.com/xlwh/tsdb-engine/g"
	"sync"
)

type MemTable struct {
	memData map[string]*SeriesData
	store   *Storage
	option  *g.Option

	lock sync.RWMutex
}

type SeriesData struct {
	key      string
	cs       *g.Series
	pointNum int64

	sTime int64
	eTime int64
}

func (s *SeriesData) put(point *g.DataPoint) error {
	if s.cs != nil {
		s.cs.Push(point.Timestamp, float64(point.Cnt), point.Sum, point.Max, point.Min)

		if s.sTime > point.Timestamp {
			s.sTime = point.Timestamp
		}

		if s.eTime < point.Timestamp {
			s.eTime = point.Timestamp
		}

		return nil
	} else {
		return errors.New("Get cs error")
	}

}

func (s *SeriesData) get(sTime, eTime int64) []*g.DataPoint {
	points := make([]*g.DataPoint, 0)

	it, err := g.NewIterator(s.cs.Bytes())
	if err == nil {
		for it.Next() {
			t, cnt, sum, max, min := it.Values()
			if t >= sTime && t <= eTime {
				p := &g.DataPoint{
					Key:       s.key,
					Timestamp: t,
					Cnt:       int64(cnt),
					Sum:       sum,
					Max:       max,
					Min:       min,
				}
				points = append(points, p)
			}
		}
	}

	return points
}

func (s *SeriesData) close(point *g.DataPoint, store *Storage) {
	s.cs.Finish()

	block := &g.DataBlock{}
	block.Key = point.Key
	block.STime = s.sTime
	block.ETime = s.eTime
	block.Data = s.cs.Bytes()

	store.Put(block)
}

func (s *SeriesData) flush(store *Storage) {
	block := &g.DataBlock{}
	block.Key = s.key
	block.STime = s.sTime
	block.ETime = s.eTime
	block.Data = s.cs.Bytes()

	err := store.Put(block)
	if err != nil {
		log.Warnf("Flush data error:%v", err)
	}
}

func NewMemtable(option *g.Option) (*MemTable, error) {
	mem := &MemTable{
		memData: make(map[string]*SeriesData),
		option:  option,
	}
	store, err := NewStorage(option)
	if err != nil {
		return nil, err
	}
	mem.store = store

	return mem, nil
}

func (m *MemTable) PutPoint(point *g.DataPoint) error {
	m.lock.Lock()
	defer m.lock.Unlock()

	if series, found := m.memData[point.Key]; found {
		if series.pointNum >= m.option.PointNumEachBlock {
			series.close(point, m.store)
		}
		return series.put(point)
	} else {
		m.memData[point.Key] = &SeriesData{
			key:      point.Key,
			cs:       g.New(point.Timestamp),
			pointNum: 0,
			sTime:    point.Timestamp,
			eTime:    point.Timestamp,
		}
		return m.memData[point.Key].put(point)
	}
	return nil
}

func (m *MemTable) getSeries(key string) *SeriesData {
	m.lock.RLock()
	defer m.lock.RUnlock()
	if series, found := m.memData[key]; found {
		return series
	} else {
		return nil
	}
}

func (m *MemTable) Get(key string, sTime, eTime int64) ([]*g.DataPoint, error) {
	series := m.getSeries(key)
	if series != nil {
		if !(sTime > series.eTime || eTime < series.sTime) {
			points := series.get(sTime, eTime)

			if len(points) == 0 {
				return m.store.Get(key, sTime, eTime)
			} else {
				return points, nil
			}
		}
	} else {
		// 尝试在磁盘上搜索
		return m.store.Get(key, sTime, eTime)
	}

	return m.store.Get(key, sTime, eTime)
}

func (m *MemTable) Start() {
	m.store.Start()
}

func (m *MemTable) flush() {
	m.lock.RLock()
	defer m.lock.RUnlock()

	for _, series := range m.memData {
		series.flush(m.store)
	}
}

func (m *MemTable) Stop() {
	m.flush()
	m.store.Stop()
}
