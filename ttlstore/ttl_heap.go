package ttlstore

import (
	"container/heap"
	"errors"
	"fmt"
	"sync"
)

type TtlHeap struct {
	items map[string]*Item
	queue minHeap
	lock  sync.Mutex
}

func NewTtlHeap() *TtlHeap {
	return &TtlHeap{
		items: make(map[string]*Item),
		queue: make(minHeap, 0),
	}
}

func (t *TtlHeap) Size() int {
	t.lock.Lock()
	defer t.lock.Unlock()
	return len(t.queue)
}

func (t *TtlHeap) Push(key string, time int64) error {
	t.lock.Lock()
	defer t.lock.Unlock()

	if _, ok := t.items[key]; ok {
		return fmt.Errorf("key %v already exists", key)
	}

	item := &Item{key, time, 0}
	t.items[key] = item
	t.queue.Push(item)
	return nil
}

func (t *TtlHeap) Pop() (*Item, error) {
	t.lock.Lock()
	defer t.lock.Unlock()

	if len(t.queue) == 0 {
		return nil, errors.New("heap is empty")
	}

	item := t.queue.Pop().(*Item)
	delete(t.items, item.Key)
	return item, nil
}

func (t *TtlHeap) Peek() (Item, error) {
	t.lock.Lock()
	defer t.lock.Unlock()

	if len(t.queue) == 0 {
		return Item{}, errors.New("heap is empty")
	}

	return *(t.queue[0]), nil
}

func (t *TtlHeap) Contains(key string) bool {
	t.lock.Lock()
	defer t.lock.Unlock()

	_, ok := t.items[key]
	return ok
}

func (t *TtlHeap) Update(key string, time int64) error {
	t.lock.Lock()
	defer t.lock.Unlock()

	if item, ok := t.items[key]; ok {
		t.queue.update(item, time)
		return nil
	}

	return fmt.Errorf("heap doesn't contain key %v", key)
}

func (t *TtlHeap) Remove(key string) error {
	t.lock.Lock()
	defer t.lock.Unlock()

	if item, ok := t.items[key]; ok {
		heap.Remove(&t.queue, item.index)
		delete(t.items, key)
		return nil
	}

	return fmt.Errorf("heap doesn't contain key %v", key)
}

type Item struct {
	Key   string // The value of the Item; arbitrary.
	Time  int64  // The priority of the Item in the queue.
	index int    // The index of the Item in the heap. Maintained by heap operations.
}

// A minHeap implements heap.Interface and holds Items.
type minHeap []*Item

func (m minHeap) Len() int { return len(m) }

func (m minHeap) Less(i, j int) bool {
	// We want Pop to give us the highest, not lowest, priority so we use greater than here.
	return m[i].Time < m[j].Time
}

func (m minHeap) Swap(i, j int) {
	m[i], m[j] = m[j], m[i]
	m[i].index = i
	m[j].index = j
}

func (m *minHeap) Push(x interface{}) {
	n := len(*m)
	item := x.(*Item)
	item.index = n
	*m = append(*m, item)
}

func (m *minHeap) Pop() interface{} {
	old := *m
	n := len(old)
	item := old[n-1]
	item.index = -1 // for safety
	*m = old[0 : n-1]
	return item
}

// update modifies the priority and value of an Item in the queue.
func (m *minHeap) update(item *Item, time int64) {
	item.Time = time
	heap.Fix(m, item.index)
}
