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

type SimpleDataPoint struct {
	Key       string
	Timestamp int64
	Value     float64
}

type DataBlock struct {
	Name  string
	STime int64
	ETime int64
	Data  []byte
}

func (d *DataPoint) ToString() string {
	x, _ := json.Marshal(d)
	return string(x)
}

func Sort(data []*DataPoint) {
	for i := 0; i < len(data); i++ {
		for j := i + 1; j < len(data); j++ {
			if data[i].Timestamp > data[j].Timestamp {
				data[i], data[j] = data[j], data[i]
			}
		}
	}
}

func SortSimple(data []*SimpleDataPoint) {
	for i := 0; i < len(data); i++ {
		for j := i + 1; j < len(data); j++ {
			if data[i].Timestamp > data[j].Timestamp {
				data[i], data[j] = data[j], data[i]
			}
		}
	}
}
