这是一个基于针对时序数据，在LevelDB上进行封装的一个磁盘存储引擎。

# Installation
go get github.com/xlwh/tsdb-engine

# Usage
	opt := tsengine.NewOption()
	opt.DataDir = "D:/data"   // 数据存储目录
	opt.ExpireTime = 1800     // 数据过期时间，单位秒
	opt.PointNumEachBlock = 10   // 在memTable中的最近的数据点的个数，大于这个点的数据将会被刷写到磁盘
	opt.GcInterval = 2           // 执行过期数据回收检查的时间间隔，单位秒

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
