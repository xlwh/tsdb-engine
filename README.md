这是一个基于针对时序数据，在LevelDB上进行封装的一个磁盘存储引擎。

# Installation
go get github.com/xlwh/tsdb-engine

# Usage
	opt := NewOption()
	opt.DataDir = "D:/data"

	db, err := tsengine.NewDBEngine(opt)
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
