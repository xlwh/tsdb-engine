这是一个基于针对时序数据，在LevelDB上进行封装的一个磁盘存储引擎。

# Installation
go get github.com/xlwh/tsdb-engine

# Usage
	opt := tsengine.NewOption()
	opt.DataDir = "D:/data"   // 数据存储目录
	opt.ExpireTime = 1800     // 数据过期时间，单位秒
	opt.PointNumEachBlock = 10   // 在memTable中的最近的数据点的个数，大于这个点的数据将会被刷写到磁盘
	opt.GcInterval = 2           // 执行过期数据回收检查的时间间隔，单位秒
	opt.FlushInterval = 60       // 自动刷新内存索引数据到磁盘的周期

	db, err := tsengine.NewDBEngine(opt)
	if err != nil {
		fmt.Println(err)
		return
	}

	db.Start()
    
    // 读写统计值:数据模型为key,timestamp,cnt,sum,max,min
	for i := 0; i < 10; i++ {
		point := NewPoint("test", time.Now().UnixNano()/1e6+int64(i), int64(i), float64(i), float64(i), float64(i))
		err := db.PutStatics(point)
		if err != nil {
			fmt.Printf("Put error:%v \n", err)
		}
	}

	points, err := db.GetStatics("test", time.Now().UnixNano()/1e6, time.Now().UnixNano()/1e6+int64(10))
	for _, point := range points {
		fmt.Println(point.ToString())
	}
	
	// 读写简单的时序数据,数据模型:key,timestamp,value
	for i := 0; i < 10; i++ {
    	err := db.Put("test", time.Now().UnixNano()/1e6+int64(i), float64(i))
    	if err != nil {
    		fmt.Printf("Put error:%v \n", err)
    	}
    }
    
    points, err := db.Get("test", time.Now().UnixNano()/1e6, time.Now().UnixNano()/1e6+int64(10))
    for _, point := range points {
    	fmt.Println(point.ToString())
    }
	
	db.Close()

# 底层LevelDB的一些配置选项
	// 下面是底层LevelDB的配置参数

	// BlockCacheCapacity defines the capacity of the 'sorted table' block caching.
	// Use -1 for zero, this has same effect as specifying NoCacher to BlockCacher.
	//
	// The default value is 8MiB.
	BlockCacheCapacity int

	// BlockRestartInterval is the number of keys between restart points for
	// delta encoding of keys.
	//
	// The default value is 16.
	BlockRestartInterval int

	// BlockSize is the minimum uncompressed size in bytes of each 'sorted table'
	// block.
	//
	// The default value is 4KiB.
	BlockSize int

	// CompactionExpandLimitFactor limits compaction size after expanded.
	// This will be multiplied by table size limit at compaction target level.
	//
	// The default value is 25.
	CompactionExpandLimitFactor int

	// CompactionGPOverlapsFactor limits overlaps in grandparent (Level + 2) that a
	// single 'sorted table' generates.
	// This will be multiplied by table size limit at grandparent level.
	//
	// The default value is 10.
	CompactionGPOverlapsFactor int

	// CompactionL0Trigger defines number of 'sorted table' at level-0 that will
	// trigger compaction.
	//
	// The default value is 4.
	CompactionL0Trigger int

	// CompactionSourceLimitFactor limits compaction source size. This doesn't apply to
	// level-0.
	// This will be multiplied by table size limit at compaction target level.
	//
	// The default value is 1.
	CompactionSourceLimitFactor int

	// CompactionTableSize limits size of 'sorted table' that compaction generates.
	// The limits for each level will be calculated as:
	//   CompactionTableSize * (CompactionTableSizeMultiplier ^ Level)
	// The multiplier for each level can also fine-tuned using CompactionTableSizeMultiplierPerLevel.
	//
	// The default value is 2MiB.
	CompactionTableSize int

	// CompactionTableSizeMultiplier defines multiplier for CompactionTableSize.
	//
	// The default value is 1.
	CompactionTableSizeMultiplier float64

	// CompactionTotalSize limits total size of 'sorted table' for each level.
	// The limits for each level will be calculated as:
	//   CompactionTotalSize * (CompactionTotalSizeMultiplier ^ Level)
	// The multiplier for each level can also fine-tuned using
	// CompactionTotalSizeMultiplierPerLevel.
	//
	// The default value is 10MiB.
	CompactionTotalSize int

	// CompactionTotalSizeMultiplier defines multiplier for CompactionTotalSize.
	//
	// The default value is 10.
	CompactionTotalSizeMultiplier float64

	// DisableBufferPool allows disable use of util.BufferPool functionality.
	//
	// The default value is false.
	DisableBufferPool bool

	// DisableBlockCache allows disable use of cache.Cache functionality on
	// 'sorted table' block.
	//
	// The default value is false.
	DisableBlockCache bool

	// DisableCompactionBackoff allows disable compaction retry backoff.
	//
	// The default value is false.
	DisableCompactionBackoff bool

	// DisableLargeBatchTransaction allows disabling switch-to-transaction mode
	// on large batch write. If enable batch writes large than WriteBuffer will
	// use transaction.
	//
	// The default is false.
	DisableLargeBatchTransaction bool

	// ErrorIfExist defines whether an error should returned if the DB already
	// exist.
	//
	// The default value is false.
	ErrorIfExist bool

	// ErrorIfMissing defines whether an error should returned if the DB is
	// missing. If false then the database will be created if missing, otherwise
	// an error will be returned.
	//
	// The default value is false.
	ErrorIfMissing bool

	// IteratorSamplingRate defines approximate gap (in bytes) between read
	// sampling of an iterator. The samples will be used to determine when
	// compaction should be triggered.
	//
	// The default is 1MiB.
	IteratorSamplingRate int

	// NoSync allows completely disable fsync.
	//
	// The default is false.
	NoSync bool

	// NoWriteMerge allows disabling write merge.
	//
	// The default is false.
	NoWriteMerge bool

	// OpenFilesCacheCapacity defines the capacity of the open files caching.
	// Use -1 for zero, this has same effect as specifying NoCacher to OpenFilesCacher.
	//
	// The default value is 500.
	OpenFilesCacheCapacity int

	// If true then opens DB in read-only mode.
	//
	// The default value is false.
	ReadOnly bool

	// WriteBuffer defines maximum size of a 'memdb' before flushed to
	// 'sorted table'. 'memdb' is an in-memory DB backed by an on-disk
	// unsorted journal.
	//
	// LevelDB may held up to two 'memdb' at the same time.
	//
	// The default value is 4MiB.
	WriteBuffer int

	// WriteL0StopTrigger defines number of 'sorted table' at level-0 that will
	// pause write.
	//
	// The default value is 12.
	WriteL0PauseTrigger int

	// WriteL0SlowdownTrigger defines number of 'sorted table' at level-0 that
	// will trigger write slowdown.
	//
	// The default value is 8.
	WriteL0SlowdownTrigger int
