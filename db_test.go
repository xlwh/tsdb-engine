package tsengine

import (
	"fmt"
	"testing"
	"time"
)

func Test_write(t *testing.T) {
	db, err := NewDBEngine(nil)
	if err != nil {
		fmt.Println("Create engine error.", err.Error())
		return
	}

	db.Start()

	err = db.Put("test", 1644668878000, 20)
	err = db.Put("test", 1644668879000, 30)

	fmt.Println("Put ret:", err)

	time.Sleep(time.Millisecond * 10)

	points, err := db.Get("test", 1644668878000, 1644668879000)

	if err != nil {
		fmt.Println("get error:", err.Error())
		return
	}

	for _, p := range points {
		fmt.Println(p.Timestamp, p.Value)
	}

	db.Close()
}

func Test_read(t *testing.T) {
	db, err := NewDBEngine(nil)
	if err != nil {
		fmt.Println("Create engine error.", err.Error())
		return
	}

	db.Start()
	points, err := db.Get("test", 1644668878000, 1644668879000)

	if err != nil {
		fmt.Println("get error:", err.Error())
		return
	}

	for _, p := range points {
		fmt.Println(p.Timestamp, p.Value)
	}

	db.Close()
}
