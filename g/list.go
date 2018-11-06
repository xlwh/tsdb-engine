package g

import (
	"container/list"
	"sync"
)

type List struct {
	mu   sync.RWMutex
	list *list.List
}

func NewList() *List {
	return &List{list: list.New()}
}

func (this *List) PushFront(v interface{}) *list.Element {
	this.mu.Lock()
	e := this.list.PushFront(v)
	this.mu.Unlock()
	return e
}

func (this *List) PushBack(v interface{}) *list.Element {
	this.mu.Lock()
	r := this.list.PushBack(v)
	this.mu.Unlock()
	return r
}

func (this *List) InsertAfter(v interface{}, mark *list.Element) *list.Element {
	this.mu.Lock()
	r := this.list.InsertAfter(v, mark)
	this.mu.Unlock()
	return r
}

func (this *List) InsertBefore(v interface{}, mark *list.Element) *list.Element {
	this.mu.Lock()
	r := this.list.InsertBefore(v, mark)
	this.mu.Unlock()
	return r
}

func (this *List) BatchPushFront(vs []interface{}) {
	this.mu.Lock()
	for _, item := range vs {
		this.list.PushFront(item)
	}
	this.mu.Unlock()
}

func (this *List) PopBack() interface{} {
	this.mu.Lock()
	if elem := this.list.Back(); elem != nil {
		item := this.list.Remove(elem)
		this.mu.Unlock()
		return item
	}
	this.mu.Unlock()
	return nil
}

func (this *List) PopFront() interface{} {
	this.mu.Lock()
	if elem := this.list.Front(); elem != nil {
		item := this.list.Remove(elem)
		this.mu.Unlock()
		return item
	}
	this.mu.Unlock()
	return nil
}

func (this *List) BatchPopBack(max int) []interface{} {
	this.mu.Lock()
	count := this.list.Len()
	if count == 0 {
		this.mu.Unlock()
		return []interface{}{}
	}

	if count > max {
		count = max
	}
	items := make([]interface{}, count)
	for i := 0; i < count; i++ {
		items[i] = this.list.Remove(this.list.Back())
	}
	this.mu.Unlock()
	return items
}

func (this *List) BatchPopFront(max int) []interface{} {
	this.mu.Lock()
	count := this.list.Len()
	if count == 0 {
		this.mu.Unlock()
		return []interface{}{}
	}

	if count > max {
		count = max
	}
	items := make([]interface{}, count)
	for i := 0; i < count; i++ {
		items[i] = this.list.Remove(this.list.Front())
	}
	this.mu.Unlock()
	return items
}

func (this *List) PopBackAll() []interface{} {
	this.mu.Lock()
	count := this.list.Len()
	if count == 0 {
		this.mu.Unlock()
		return []interface{}{}
	}
	items := make([]interface{}, count)
	for i := 0; i < count; i++ {
		items[i] = this.list.Remove(this.list.Back())
	}
	this.mu.Unlock()
	return items
}

func (this *List) PopFrontAll() []interface{} {
	this.mu.Lock()
	count := this.list.Len()
	if count == 0 {
		this.mu.Unlock()
		return []interface{}{}
	}
	items := make([]interface{}, count)
	for i := 0; i < count; i++ {
		items[i] = this.list.Remove(this.list.Front())
	}
	this.mu.Unlock()
	return items
}

func (this *List) Remove(e *list.Element) interface{} {
	this.mu.Lock()
	r := this.list.Remove(e)
	this.mu.Unlock()
	return r
}

func (this *List) RemoveAll() {
	this.mu.Lock()
	this.list = list.New()
	this.mu.Unlock()
}

func (this *List) FrontAll() []interface{} {
	this.mu.RLock()
	count := this.list.Len()
	if count == 0 {
		this.mu.RUnlock()
		return []interface{}{}
	}

	items := make([]interface{}, 0, count)
	for e := this.list.Front(); e != nil; e = e.Next() {
		items = append(items, e.Value)
	}
	this.mu.RUnlock()
	return items
}

func (this *List) BackAll() []interface{} {
	this.mu.RLock()
	count := this.list.Len()
	if count == 0 {
		this.mu.RUnlock()
		return []interface{}{}
	}

	items := make([]interface{}, 0, count)
	for e := this.list.Back(); e != nil; e = e.Prev() {
		items = append(items, e.Value)
	}
	this.mu.RUnlock()
	return items
}

func (this *List) FrontItem() interface{} {
	this.mu.RLock()
	if f := this.list.Front(); f != nil {
		this.mu.RUnlock()
		return f.Value
	}

	this.mu.RUnlock()
	return nil
}

func (this *List) BackItem() interface{} {
	this.mu.RLock()
	if f := this.list.Back(); f != nil {
		this.mu.RUnlock()
		return f.Value
	}

	this.mu.RUnlock()
	return nil
}

func (this *List) Front() *list.Element {
	this.mu.RLock()
	r := this.list.Front()
	this.mu.RUnlock()
	return r
}

func (this *List) Back() *list.Element {
	this.mu.RLock()
	r := this.list.Back()
	this.mu.RUnlock()
	return r
}

func (this *List) Len() int {
	this.mu.RLock()
	length := this.list.Len()
	this.mu.RUnlock()
	return length
}
