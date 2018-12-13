package tsengine

import (
	"testing"
	"time"
	"os"
	"fmt"
)

/*
	单测覆盖的测试用例设计:
		1.简单的时序数据的读写(In mem)
		2.统计类型的时序数据读写(In mem)
		3.简单的时序数据的读写(In disk)
		5.简单的时序数据的读写(数据分布在cs,Mem,disk)
		6.数据的过期清理策略
 */

func Test_simple_write_read(t *testing.T) {
	os.RemoveAll("./data")
	opt := NewOption()
	opt.DataDir = "./data"
	opt.ExpireTime = 1800
	opt.PointNumEachBlock = 10
	opt.GcInterval = 2

	db, err := NewDBEngine(nil)
	if err != nil {
		t.Errorf("Create engine error.%v", err)
		t.Failed()
	}

	db.Start()

	err = db.Put("test", time.Now().UnixNano() / 1e6, 20)
	err = db.Put("test", time.Now().UnixNano() / 1e6 + 60000, 30)

	if err != nil {
		t.Errorf("put error:%v", err)
		t.Failed()
	}

	points, err := db.Get("test", time.Now().UnixNano() / 1e6, time.Now().UnixNano() / 1e6 + 60000)

	if err != nil {
		t.Errorf("read error:%v", err)
		t.Failed()
	}

	if len(points) != 2 {
		t.Fail()
		return
	}

	if points[1].Value != 30 {
		t.Failed()
	}

	db.Stop()
}

func Test_statics_write_read(t *testing.T) {
	os.RemoveAll("./data")
	opt := NewOption()
	opt.DataDir = "./data"
	opt.ExpireTime = 1800
	opt.PointNumEachBlock = 10
	opt.GcInterval = 2

	db, err := NewDBEngine(nil)
	if err != nil {
		t.Errorf("Create engine error.%v", err)
		t.Failed()
	}

	db.Start()

	err = db.PutStatics(NewPoint("test", time.Now().UnixNano() / 1e6, int64(1), float64(1), float64(1), float64(1)))
	err = db.PutStatics(NewPoint("test", time.Now().UnixNano() / 1e6 + 60000, int64(1), float64(2), float64(2), float64(2)))

	if err != nil {
		t.Errorf("put error:%v", err)
		t.Failed()
	}

	points, err := db.GetStatics("test", time.Now().UnixNano() / 1e6, time.Now().UnixNano() / 1e6 + 60000)

	if err != nil {
		t.Errorf("read error:%v", err)
		t.Failed()
	}

	if len(points) != 2 {
		t.Fail()
		return
	}

	if points[1].Cnt != 2 {
		t.Failed()
	}

	db.Stop()
}

func Test_write_disk(t *testing.T) {
	// 写写入数据
	os.RemoveAll("./data")
	opt := NewOption()
	opt.DataDir = "./data"
	opt.ExpireTime = 1800
	opt.PointNumEachBlock = 10
	opt.GcInterval = 2

	db, err := NewDBEngine(nil)
	if err != nil {
		t.Errorf("Create engine error.%v", err)
		t.Failed()
	}

	db.Start()

	start := time.Now().UnixNano() / 1e6
	end := time.Now().UnixNano() / 1e6 + 60000
	err = db.Put("test", start, 20)
	err = db.Put("test", end, 30)

	if err != nil {
		t.Errorf("put error:%v", err)
		t.Failed()
	}

	points, err := db.Get("test", start, end)

	if err != nil {
		t.Errorf("read error:%v", err)
		t.Failed()
	}

	if len(points) != 2 {
		t.Fail()
		return
	}

	if points[1].Value != 30 {
		t.Failed()
	}

	// 关闭引擎，以把数据写到磁盘
	db.Stop()
}

func Test_load_in_disk(t *testing.T) {
	// 重新加载数据
	start := time.Now().UnixNano() / 1e6 - 10000
	end := time.Now().UnixNano() / 1e6 + 70000
	db2, err := NewDBEngine(nil)
	if err != nil {
		t.Errorf("Create engine error.%v", err)
		t.Failed()
	}
	db2.Start()

	time.Sleep(time.Millisecond * 10)
	points, err := db2.Get("test", start, end)

	if err != nil {
		t.Errorf("read error:%v", err)
		t.Failed()
	}
	if len(points) != 2 {
		t.Fail()
		t.Errorf("Error to load,point num: %d", len(points))
		return
	}

	if points[1].Value != 30 {
		t.Failed()
	}

	db2.Stop()
}

func Test_simple_gc(t *testing.T) {
	os.RemoveAll("./data")
	opt := NewOption()
	opt.DataDir = "./data"
	opt.ExpireTime = 30
	opt.PointNumEachBlock = 10
	opt.GcInterval = 2

	db, err := NewDBEngine(nil)
	if err != nil {
		t.Errorf("Create engine error.%v", err)
		t.Failed()
	}

	db.Start()

	for i := 0; i < 100; i++ {
		db.Put("test", time.Now().UnixNano() / 1e6 + int64(i), 20)
	}

	time.Sleep(time.Minute * 2)
	db.Stop()
}

func Benchmark_simple_write(b *testing.B) {
	os.RemoveAll("./data")
	opt := NewOption()
	opt.DataDir = "./data"
	opt.ExpireTime = 30
	opt.PointNumEachBlock = 10
	opt.GcInterval = 2

	db, err := NewDBEngine(nil)
	if err != nil {
		b.Errorf("Create engine error.%v", err)
		b.Failed()
	}

	db.Start()
	b.StartTimer()
	for i := 0; i < b.N; i++ {
		db.Put("test", time.Now().UnixNano() / 1e6 + int64(i), 20)
	}

	b.ReportAllocs()
	b.ResetTimer()

	fmt.Println("Total write num", b.N)

	db.Stop()
}
