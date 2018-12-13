package tsengine

import (
	"errors"
	"github.com/xlwh/tsdb-engine/g"
	"github.com/xlwh/tsdb-engine/storage"
	"time"
)

type TsdbEngine struct {
	memTable *storage.MemTable
	index    *storage.Index

	opt *g.Option

	stop chan bool
}

func NewOption() *g.Option {
	o := &g.Option{}

	o.DataDir = "./data"
	o.ExpireTime = 3600
	o.PointNumEachBlock = 10
	o.GcInterval = 2
	o.FlushInterVal = 60 // 主动刷盘时间

	// 下面是LevelDB相关的一些默认配置
	o.BlockCacheCapacity = 8 * 1024 * 1024
	o.BlockRestartInterval = 16
	o.BlockSize = 4 * 1024
	o.CompactionExpandLimitFactor = 25
	o.CompactionGPOverlapsFactor = 10
	o.CompactionL0Trigger = 4
	o.CompactionSourceLimitFactor = 1
	o.CompactionTableSize = 2 * 1024 * 1024
	o.CompactionTableSizeMultiplier = 1
	o.CompactionTotalSize = 10 * 1024 * 1024
	o.CompactionTotalSizeMultiplier = 10
	o.DisableBufferPool = false
	o.DisableBlockCache = false
	o.DisableCompactionBackoff = false
	o.DisableLargeBatchTransaction = false
	o.ErrorIfExist = false
	o.ErrorIfMissing = false
	o.IteratorSamplingRate = 1 * 1024 * 1024
	o.NoSync = false
	o.NoWriteMerge = false
	o.OpenFilesCacheCapacity = 500
	o.ReadOnly = false
	o.WriteBuffer = 4 * 1024 * 1024
	o.WriteL0PauseTrigger = 12
	o.WriteL0SlowdownTrigger = 8

	return o
}

func NewPoint(key string, time int64, cnt int64, sum, max, min float64) *g.DataPoint {
	return &g.DataPoint{
		Key:       key,
		Timestamp: time,
		Cnt:       cnt,
		Sum:       sum,
		Max:       max,
		Min:       min,
	}
}

func NewDBEngine(option *g.Option) (*TsdbEngine, error) {
	if option == nil {
		option = NewOption()
	}

	_, err := storage.NewStorage(option)
	if err != nil {
		return nil, err
	}

	engine := &TsdbEngine{
		opt:   option,
		index: storage.GetIndex(),
		stop:  make(chan bool, 1),
	}
	memTable, err := storage.NewMemtable(option)
	if err != nil {
		return nil, err
	}
	engine.memTable = memTable

	return engine, nil
}

func (t *TsdbEngine) Start() {
	go t.runTask()
}

// 执行gc任务和主动刷meta数据到磁盘
func (t *TsdbEngine) runTask() {
	gcTimer := time.NewTicker(time.Duration(t.opt.GcInterval) * time.Second)
	flushTimer := time.NewTicker(time.Duration(t.opt.FlushInterVal) * time.Second)
Loop:
	for {
		select {
		case <-gcTimer.C:
			t.memTable.Gc()
		case <-flushTimer.C:
			t.memTable.Sync(false)
		case <-t.stop:
			break Loop
		}
	}
}

func (eg *TsdbEngine) Put(key string, t int64, v float64) error {
	if t < time.Now().UnixNano()/1e6-eg.opt.ExpireTime*1000 {
		return errors.New("Data expire")
	}

	return eg.memTable.PutSimple(key, t, v)
}

func (eg *TsdbEngine) Get(key string, startTime, endTime int64) ([]*g.SimpleDataPoint, error) {
	indexItem, err := eg.index.GetIndexItem(key)
	if err != nil {
		return nil, err
	}

	result := make([]*g.SimpleDataPoint, 0)

	allPos, err := indexItem.Pos(startTime, endTime)
	for _, pos := range allPos {
		points, err := eg.getSimpleByPos(key, pos, startTime, endTime)
		if err == nil {
			for _, p := range points {
				result = append(result, p)
			}
		}
	}

	g.SortSimple(result)
	return result, nil
}

func (eg *TsdbEngine) getSimpleByPos(key string, pos *storage.PosInfo, start, end int64) ([]*g.SimpleDataPoint, error) {
	reader, _ := eg.memTable.SeriesReader(key)
	blocks := make([]string, 0)

	switch pos.Pos {
	case "cs":
		if reader != nil {
			return reader.ReadCsSimple(start, end)
		}
	case "mem":
		blocks = append(blocks, pos.BlockName)
		if reader != nil {
			return reader.ReadSimpleBlocks(blocks, start, end)
		}
	case "disk":
		return storage.StorageInstance.ReadSimple(key, pos.BlockName, start, end)
	}

	return nil, nil
}

func (eg *TsdbEngine) getByPos(key string, pos *storage.PosInfo, start, end int64) ([]*g.DataPoint, error) {
	reader, _ := eg.memTable.SeriesReader(key)
	blocks := make([]string, 0)

	switch pos.Pos {
	case "cs":
		if reader != nil {
			return reader.ReadCs(start, end)
		}
	case "mem":
		blocks = append(blocks, pos.BlockName)
		if reader != nil {
			return reader.ReadBlocks(blocks, start, end)
		}
	case "disk":
		return storage.StorageInstance.Read(key, pos.BlockName, start, end)
	}

	return nil, nil
}

func (eg *TsdbEngine) PutStatics(data *g.DataPoint) error {
	if data.Timestamp < time.Now().UnixNano()/1e6-eg.opt.ExpireTime {
		return errors.New("Data expire")
	}
	return eg.memTable.PutStatistics(data.Key, data.Timestamp, float64(data.Cnt), data.Sum, data.Max, data.Min)
}

func (eg *TsdbEngine) GetStatics(key string, startTime, endTime int64) ([]*g.DataPoint, error) {
	indexItem, err := eg.index.GetIndexItem(key)
	if err != nil {
		return nil, err
	}

	result := make([]*g.DataPoint, 0)

	allPos, err := indexItem.Pos(startTime, endTime)
	for _, pos := range allPos {
		points, err := eg.getByPos(key, pos, startTime, endTime)
		if err != nil {
			for _, p := range points {
				result = append(result, p)
			}
		}
	}

	g.Sort(result)
	return result, nil
}

func (eg *TsdbEngine) Close() {
	eg.stop <- true
	if eg.memTable != nil {
		eg.memTable.Sync(true)
	}
	storage.StorageInstance.Stop()
}
