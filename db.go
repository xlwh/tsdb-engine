package tsengine

import (
	"github.com/xlwh/tsdb-engine/g"
	"github.com/xlwh/tsdb-engine/storage"
)

type TsdbEngine struct {
	memTable *storage.MemTable
	opt *g.Option
}


func  NewOption() *g.Option {
	o := &g.Option{}

	o.DataDir = "./data"
	o.ExpireTime = 3600
	o.PointNumEachBlock = 10
	o.GcInterval = 2

	return o
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

func (t *TsdbEngine) Put(point *g.DataPoint) error {
	return t.memTable.PutPoint(point)
}

func (t *TsdbEngine) Get(key string, startTime, endTime int64) ([]*g.DataPoint, error) {
	return t.memTable.Get(key, startTime, endTime)
}

func (t *TsdbEngine) Close() {
	t.memTable.Stop()
}
