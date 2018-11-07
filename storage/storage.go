package storage

import (
	log "github.com/cihub/seelog"
	"github.com/syndtr/goleveldb/leveldb"
	"github.com/xlwh/tsdb-engine/g"
	"time"
	"github.com/syndtr/goleveldb/leveldb/opt"
)

type Storage struct {
	memIndex *MemIndex
	db       *leveldb.DB

	option *g.Option
	stop   chan int
}

func NewStorage(option *g.Option) (*Storage, error) {
	s := &Storage{
		option: option,
	}
	s.memIndex = NewMemIndex(option)
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

	return s, nil
}

func (s *Storage) Start() {
	// 加载索引数据
	s.memIndex.Start()
	// 运行gc
	go s.runGc()
}

func (s *Storage) Put(block *g.DataBlock) error {
	// 写索引
	key := s.memIndex.AddIndex(block.Key, block.STime, block.ETime)
	// 写leveldb
	return s.db.Put([]byte(key), block.Data, nil)
}

func (s *Storage) Get(key string, sTime, eTime int64) ([]*g.DataPoint, error) {
	allBlock := s.memIndex.GetBlocks(key, sTime, eTime)
	ret := make([]*g.DataPoint, 0)
	for _, blockName := range allBlock {
		val, err := s.db.Get([]byte(blockName), nil)
		if err != nil {
			// TODO log print error
			log.Warnf("Get in disk error:%v", err)
		} else {
			it, err := g.NewIterator(val)
			if err != nil {
				log.Warnf("Get in disk error:%v", err)
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
