package tsengine

import (
	"fmt"
	"testing"
	"time"
)

func Test_GetInMemTable(t *testing.T) {
	opt := NewOption()
	opt.DataDir = "D:/data"

	db, err := NewDBEngine(opt)
	if err != nil {
		fmt.Println(err)
		return
	}

	db.Start()

	for i := 0; i < 10; i++ {
		point := NewPoint("test", time.Now().UnixNano()/1e6+int64(i), int64(i), float64(i), float64(i), float64(i))
		err := db.Put(point)
		if err != nil {
			fmt.Printf("Put error:%v \n", err)
		}
	}

	points, err := db.Get("test", time.Now().UnixNano()/1e6, time.Now().UnixNano()/1e6+int64(10))
	for _, point := range points {
		fmt.Println(point.ToString())
	}

	db.Close()
	time.Sleep(time.Millisecond * 20)
}

func Test_load(t *testing.T) {
	opt := NewOption()
	opt.DataDir = "D:/data"

	db, err := NewDBEngine(opt)
	if err != nil {
		fmt.Println(err)
		return
	}

	db.Start()

	time.Sleep(time.Millisecond * 30000)
	db.Close()
	time.Sleep(time.Millisecond * 10000)
}

func Test_ReadDisk(t *testing.T) {
	opt := NewOption()
	opt.DataDir = "D:/data"

	db, err := NewDBEngine(opt)
	if err != nil {
		fmt.Println(err)
		return
	}

	db.Start()

	points, err := db.Get("test", 1541231085000, 1577087085000)
	//fmt.Println(len(points))
	for _, point := range points {
		fmt.Println(point.ToString())
	}

	db.Close()
}

func Test_Flush(t *testing.T) {
	opt := NewOption()
	opt.DataDir = "D:/data"
	opt.PointNumEachBlock = 5

	db, err := NewDBEngine(opt)
	if err != nil {
		fmt.Println(err)
		return
	}

	db.Start()

	for i := 0; i < 10; i++ {
		point := NewPoint("test", time.Now().UnixNano()/1e6+int64(i), int64(i), float64(i), float64(i), float64(i))
		err := db.Put(point)
		if err != nil {
			fmt.Printf("Put error:%v \n", err)
		}
	}

	points, err := db.Get("test", time.Now().UnixNano()/1e6-100000000, time.Now().UnixNano()/1e6+10000000)
	for _, point := range points {
		fmt.Println(point.ToString())
	}

	db.Close()
}
