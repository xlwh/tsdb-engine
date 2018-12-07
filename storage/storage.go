package storage

import (
	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/opt"
	"github.com/xlwh/tsdb-engine/cs/simple"
	"github.com/xlwh/tsdb-engine/cs/statistics"
	"github.com/xlwh/tsdb-engine/g"
)

var StorageInstance *Storage

type Storage struct {
	db     *leveldb.DB
	option *g.Option
	stop   chan int
}

func NewStorage(option *g.Option) (*Storage, error) {
	s := &Storage{
		option: option,
	}

	levelDBOption := &opt.Options{}
	levelDBOption.BlockCacheCapacity = option.BlockCacheCapacity
	levelDBOption.BlockRestartInterval = option.BlockRestartInterval
	levelDBOption.BlockSize = option.BlockSize
	levelDBOption.CompactionExpandLimitFactor = option.CompactionExpandLimitFactor
	levelDBOption.CompactionGPOverlapsFactor = option.CompactionGPOverlapsFactor
	levelDBOption.CompactionL0Trigger = option.CompactionL0Trigger
	levelDBOption.CompactionSourceLimitFactor = option.CompactionSourceLimitFactor
	levelDBOption.CompactionTableSize = option.CompactionTableSize
	levelDBOption.CompactionTableSizeMultiplier = option.CompactionTableSizeMultiplier
	levelDBOption.CompactionTotalSize = option.CompactionTotalSize
	levelDBOption.CompactionTotalSizeMultiplier = option.CompactionTotalSizeMultiplier
	levelDBOption.DisableBufferPool = option.DisableBufferPool
	levelDBOption.DisableBlockCache = option.DisableBlockCache
	levelDBOption.DisableCompactionBackoff = option.DisableCompactionBackoff
	levelDBOption.DisableLargeBatchTransaction = option.DisableLargeBatchTransaction
	levelDBOption.ErrorIfExist = option.ErrorIfExist
	levelDBOption.ErrorIfMissing = option.ErrorIfMissing
	levelDBOption.IteratorSamplingRate = option.IteratorSamplingRate
	levelDBOption.NoSync = option.NoSync
	levelDBOption.NoWriteMerge = option.NoWriteMerge
	levelDBOption.OpenFilesCacheCapacity = option.OpenFilesCacheCapacity
	levelDBOption.ReadOnly = option.ReadOnly
	levelDBOption.WriteBuffer = option.WriteBuffer
	levelDBOption.WriteL0PauseTrigger = option.WriteL0PauseTrigger
	levelDBOption.WriteL0SlowdownTrigger = option.WriteL0SlowdownTrigger

	db, err := leveldb.OpenFile(option.DataDir, levelDBOption)
	if err != nil {
		return nil, err
	}
	s.db = db

	s.stop = make(chan int, 1)

	StorageInstance = s
	return s, nil
}

func (s *Storage) PutBlock(block *g.DataBlock) error {
	return s.db.Put([]byte(block.Name), block.Data, nil)
}

func (s *Storage) Put(key, value []byte, wo *opt.WriteOptions) error {
	return s.db.Put([]byte(key), value, wo)
}

func (s *Storage) Get(key []byte, ro *opt.ReadOptions) (value []byte, err error) {
	return s.db.Get(key, ro)
}

func (s *Storage) ReadSimple(key, name string, start, end int64) ([]*g.SimpleDataPoint, error) {
	data, err := s.db.Get([]byte(name), nil)
	if err != nil {
		return nil, err
	}
	it, err := simple.NewIterator(data)
	if err != nil {
		return nil, err
	}

	points := make([]*g.SimpleDataPoint, 0)
	for it.Next() {
		t, v := it.Values()
		if t >= start && t <= end {
			points = append(points, &g.SimpleDataPoint{key, t, v})
		}
	}

	return points, nil
}

func (s *Storage) Read(key, name string, start, end int64) ([]*g.DataPoint, error) {
	data, err := s.db.Get([]byte(name), nil)
	if err != nil {
		return nil, err
	}
	it, err := statistics.NewIterator(data)
	if err != nil {
		return nil, err
	}

	points := make([]*g.DataPoint, 0)
	for it.Next() {
		t, cnt, sum, max, min := it.Values()
		if t >= start && t <= end {
			points = append(points, &g.DataPoint{key, t, int64(cnt), sum, max, min})
		}
	}

	return points, nil
}

func (s *Storage) DeleteBlock(name string) error {
	return s.db.Delete([]byte(name), nil)
}

func (s *Storage) Stop() {
	s.stop <- 1
	s.db.Close()
}
