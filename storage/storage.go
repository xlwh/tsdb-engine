package storage

import (
	"github.com/syndtr/goleveldb/leveldb"
	"github.com/xlwh/tsdb-engine/g"
	"time"
)

type Storage struct {
	memIndex *MemIndex
	db       *leveldb.DB

	option *g.Option
	stop   chan int
}

func NewStorage(option *g.Option) (*Storage, error ){
	s := &Storage{}
	s.memIndex = NewMemIndex(option)
	db, err := leveldb.OpenFile(option.DataDir, nil)
	if err != nil {
		return nil, err
	}
	s.db = db
	s.stop = make(chan int, 1)

	return s, nil
}

func (s *Storage) Start() {
	s.memIndex.Start()
	go s.runGc()
}

func (s *Storage) Put(block *g.DataBlock) error {
	// 写索引
	s.memIndex.AddIndex(block.Key, block.STime, block.ETime)
	// 写leveldb
	return s.db.Put([]byte(block.Key), block.Data, nil)
}

func (s *Storage) Get(key string, sTime, eTime int64) ([]*g.DataPoint, error) {
	allBlock := s.memIndex.GetBlocks(key, sTime, eTime)
	ret := make([]*g.DataPoint, 0)
	for _, blockName := range allBlock {
		val, err := s.db.Get([]byte(blockName), nil)
		if err != nil {
			// TODO log print error
		} else {
			it, err := g.NewIterator(val)
			if err != nil {
				// TODO log print error
			} else {
				for it.Next() {
					t, cnt, sum, max, min := it.Values()
					if t >= sTime && t <= eTime {
						p := &g.DataPoint{
							Key:       key,
							Timestamp: t,
							Cnt:       int64(cnt),
							Sum:       sum,
							Max:       max,
							Min:       min,
						}
						ret = append(ret, p)
					}
				}
			}
		}
	}

	return ret, nil
}

func (s *Storage) gc() {
	allBlock := s.memIndex.gc()
	for _, blockName := range allBlock {
		err := s.db.Delete([]byte(blockName), nil)
		if err != nil {
			// TODO 处理异常
		}
	}
}

func (s *Storage) runGc() {
	nextRun := time.Now().Unix() + s.option.GcInterval
Loop:
	for {
		select {
		case <-s.stop:
			{
				break Loop
			}
		default:
			{
				if time.Now().Unix() >= nextRun {
					s.gc()
					nextRun = time.Now().Unix() + s.option.GcInterval
				}
			}
		}
		time.Sleep(time.Millisecond * 2)
	}
}

func (s *Storage) Stop() {
	s.stop <- 1
	s.db.Close()
	s.memIndex.Stop()
}
