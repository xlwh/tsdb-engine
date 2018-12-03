package tsengine

import (
	"github.com/xlwh/tsdb-engine/g"
	"github.com/xlwh/tsdb-engine/storage"
)

type TsdbEngine struct {
	memTable *storage.MemTable
	opt      *g.Option
}

func NewOption() *g.Option {
	o := &g.Option{}

	o.DataDir = "./data"
	o.ExpireTime = 3600
	o.PointNumEachBlock = 10
	o.GcInterval = 2

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

	engine := &TsdbEngine{
		opt: option,
	}
	memTable, err := storage.NewMemtable(option)
	if err != nil {
		return nil, err
	}
	engine.memTable = memTable

	return engine, nil
}

func (t *TsdbEngine) Start() {
	if t.memTable != nil {
		t.memTable.Start()
	}
}

func (t *TsdbEngine) Put(point *g.DataPoint) error {
	return t.memTable.PutPoint(point)
}

func (t *TsdbEngine) Get(key string, startTime, endTime int64) ([]*g.DataPoint, error) {
	points, err := t.memTable.Get(key, startTime, endTime)
	if err != nil {
		return nil, err
	}
	g.Sort(points)

	return points, nil
}

func (t *TsdbEngine) Close() {
	t.memTable.Stop()
}
