package g

import (
	"encoding/json"
)

type DataPoint struct {
	Key       string
	Timestamp int64
	Cnt       int64
	Sum       float64
	Max       float64
	Min       float64
}

type DataBlock struct {
	Key   string
	STime int64
	ETime int64
	Data  []byte
}

func (d *DataPoint) ToString() string {
	x, _ := json.Marshal(d)
	return string(x)
}
