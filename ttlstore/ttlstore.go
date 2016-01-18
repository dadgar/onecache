package ttlstore

import (
	"errors"
	"fmt"
	"log"
	"math/rand"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/hashicorp/go-immutable-radix"
)

// DataStore is a key store that supports TTL's and LRU eviction.
type DataStore struct {
	store    *iradix.Tree
	rw       sync.RWMutex
	lru      *TtlHeap
	ttl      *TtlHeap
	usage    int64
	maxUsage int64
	rand     *rand.Rand
	quit     chan bool
	c        *sync.Cond
	logger   *log.Logger
}

type KeyData struct {
	Data  []byte
	Flags int32
	Size  int64
	Cas   int64
	Exp   int64
}

// Builder interface for KeyData
func convertKeyData(i interface{}) *KeyData {
	return i.(*KeyData)
}

func newKeyData() *KeyData {
	return new(KeyData)
}

func (k *KeyData) setData(d []byte) *KeyData {
	k.Data = d
	return k
}

func (k *KeyData) sizeDiff(new int64) int64 {
	return new - k.Size
}

func (k *KeyData) setSize(s int64) *KeyData {
	k.Size = s
	return k
}

func (k *KeyData) setExp(e int64) *KeyData {
	k.Exp = e
	return k
}

func (k *KeyData) setFlags(f int32) *KeyData {
	k.Flags = f
	return k
}

func (k *KeyData) setCas(r *rand.Rand) *KeyData {
	k.Cas = r.Int63()
	return k
}

func New(size int64, logger *log.Logger) (*DataStore, error) {
	if size <= 0 {
		return nil, fmt.Errorf("size must be a positive value: %d", size)
	}

	var mu sync.Mutex
	d := &DataStore{
		store:    iradix.New(),
		lru:      NewTtlHeap(),
		ttl:      NewTtlHeap(),
		maxUsage: size,
		rand:     rand.New(NewLockedSource(time.Now().UnixNano())),
		quit:     make(chan bool),
		c:        sync.NewCond(&mu),
		logger:   logger,
	}

	go d.ttlWatcher()
	return d, nil
}

// Destroy removes all temp files and cleanly exits all goroutines created.
func (d *DataStore) Destroy() error {
	close(d.quit)
	return nil
}

// ttlWatcher watches a TtlHeap for expirations. On an expiration it will reap key from either disk or memory,
// determined by the passed location value.
func (d *DataStore) ttlWatcher() {
	key := ""
	var exp int64
	var abort chan bool
	for {
		// Wait for a new key to ttl.
		d.c.L.Lock()
		for !d.change(key, exp) {
			select {
			case <-d.quit:
				d.c.L.Unlock()
				return
			default:
			}

			d.c.Wait()
		}

		// If it has already been reaped contine, otherwise tell the current reaper to cancel.
		select {
		case abort <- true:
		default:
		}

		// Update the next key to ttl.
		i, err := d.ttl.Peek()
		if err != nil {
			key = ""
			exp = 0
			d.c.L.Unlock()
			continue
		}
		key = i.Key
		exp = i.Time

		// Start the reaper.
		abort = make(chan bool)
		go d.reap(key, exp, abort)

		d.c.L.Unlock()
	}
}

// change returns whether the top of the ttl heap has changed from the passed
// values.
func (d *DataStore) change(oldKey string, oldTime int64) bool {
	if d.ttl.Size() == 0 {
		return oldKey != ""
	}

	i, _ := d.ttl.Peek()
	return i.Key != oldKey || i.Time != oldTime
}

// reap deletes the passed key after the experiation and is abortable from the
// passed channel.
func (d *DataStore) reap(key string, exp int64, abort <-chan bool) {
	expUnix := time.Unix(exp, 0)
	now := time.Now()
	dur := expUnix.Sub(now)

	select {
	case <-abort:
		d.logger.Printf("[DEBUG] onecache.ttlstore: aborting reaping of %v\n", key)
		return
	case <-time.After(dur):
		d.c.L.Lock()
		defer d.c.L.Unlock()
		defer d.c.Broadcast()
		if err := d.Delete(key); err != nil {
			d.logger.Printf("[ERROR] onecache.ttlstore: failed to reap key %v: %v\n", key, err)
		}
	}
}

// lruFree deletes keys until there is enough space to fit a key of the passed
// size or returns an error.
func (d *DataStore) lruFree(size int64) error {
	if d.maxUsage < size {
		return fmt.Errorf("memory available (%d) is less than key size %d", d.maxUsage, size)
	}

	// Evict from the LRU til there is enough space.
	for size > d.maxUsage-atomic.LoadInt64(&d.usage) {
		item, err := d.lru.Peek()
		if err != nil {
			return errors.New("can't free enough space to store in memory")
		}

		if err = d.deleteLocked(item.Key); err != nil {
			return err
		}
	}

	return nil
}

// List returns the set of keys stored.
func (d *DataStore) List() []string {
	d.rw.RLock()
	defer d.rw.RUnlock()

	i := d.store.Txn().Root().Iterator()
	var keys []string
	for {
		key, _, ok := i.Next()
		if !ok {
			break
		}

		keys = append(keys, string(key))
	}
	return keys
}

// Set stores the passed value and key metadata.
func (d *DataStore) Set(key string, value []byte, exp int64, flags int32) error {
	d.rw.Lock()
	defer d.rw.Unlock()

	size := int64(len(value))
	bKey := []byte(key)
	var data *KeyData
	var sizeUpdate int64

	txn := d.store.Txn()
	i, ok := txn.Get(bKey)
	if !ok {
		data = newKeyData().setData(value).setSize(size).setExp(exp).setFlags(flags).setCas(d.rand)
		sizeUpdate = size
	} else {
		data = convertKeyData(i)
		sizeUpdate = data.sizeDiff(size)
		data.setData(value).setCas(d.rand)
	}

	if err := d.lruFree(sizeUpdate); err != nil {
		return err
	}

	txn.Insert(bKey, data)
	d.store = txn.Commit()

	atomic.AddInt64(&d.usage, sizeUpdate)
	return d.touch(key, exp)
}

// Cas will set the key to the passed value if the CAS value is correct.
func (d *DataStore) Cas(key string, value []byte, exp, cas int64, flags int32) error {
	d.rw.Lock()
	defer d.rw.Unlock()

	bKey := []byte(key)
	txn := d.store.Txn()
	i, ok := txn.Get(bKey)
	if !ok {
		return fmt.Errorf("cas operation invalid before key is set: %v", key)
	}

	data := convertKeyData(i)
	if data.Cas != cas {
		return errors.New("cas value did not match")
	}

	size := int64(len(value))
	sizeUpdate := data.sizeDiff(size)
	data.setData(value).setSize(size).setExp(exp).setFlags(flags).setCas(d.rand)
	txn.Insert(bKey, data)

	if err := d.lruFree(sizeUpdate); err != nil {
		return err
	}
	d.store = txn.Commit()

	atomic.AddInt64(&d.usage, sizeUpdate)
	return d.touch(key, exp)
}

// Add stores the passed value and key metadata only if the key doesn't exist.
func (d *DataStore) Add(key string, value []byte, exp int64, flags int32) error {
	d.rw.Lock()
	defer d.rw.Unlock()

	size := int64(len(value))
	if err := d.lruFree(size); err != nil {
		return err
	}

	bKey := []byte(key)
	txn := d.store.Txn()
	_, ok := txn.Get(bKey)
	if ok {
		return errors.New("key is already set")
	}

	data := newKeyData().setData(value).setSize(size).setExp(exp).setFlags(flags)
	txn.Insert(bKey, data)
	d.store = txn.Commit()

	atomic.AddInt64(&d.usage, size)
	return d.touch(key, exp)
}

// Replace stores the passed value and key metadata only if the key exist.
func (d *DataStore) Replace(key string, value []byte, exp int64, flags int32) error {
	d.rw.Lock()
	defer d.rw.Unlock()

	bKey := []byte(key)
	txn := d.store.Txn()
	i, ok := txn.Get(bKey)
	if !ok {
		return errors.New("key doesn't exist")
	}

	size := int64(len(value))
	data := convertKeyData(i)
	sizeUpdate := data.sizeDiff(size)
	data.setData(value).setSize(size).setExp(exp).setFlags(flags)
	txn.Insert(bKey, data)

	if err := d.lruFree(sizeUpdate); err != nil {
		return err
	}
	d.store = txn.Commit()

	atomic.AddInt64(&d.usage, sizeUpdate)
	return d.touch(key, exp)
}

// Append the value to an existing value.
func (d *DataStore) Append(key string, value []byte) error {
	d.rw.Lock()
	defer d.rw.Unlock()

	size := int64(len(value))
	if err := d.lruFree(size); err != nil {
		return err
	}

	bKey := []byte(key)
	txn := d.store.Txn()
	i, ok := txn.Get(bKey)
	if !ok {
		return errors.New("key doesn't exist")
	}

	data := convertKeyData(i)
	data.setData(append(data.Data, value...)).setSize(data.Size + size)
	txn.Insert(bKey, data)
	d.store = txn.Commit()

	atomic.AddInt64(&d.usage, size)
	return d.updateLru(key)
}

// Prepend the value to an existing value.
func (d *DataStore) Prepend(key string, value []byte) error {
	d.rw.Lock()
	defer d.rw.Unlock()

	size := int64(len(value))
	if err := d.lruFree(size); err != nil {
		return err
	}

	bKey := []byte(key)
	txn := d.store.Txn()
	i, ok := txn.Get(bKey)
	if !ok {
		return errors.New("key doesn't exist")
	}

	data := convertKeyData(i)
	data.setData(append(value, data.Data...)).setSize(data.Size + size)
	txn.Insert(bKey, data)
	d.store = txn.Commit()

	atomic.AddInt64(&d.usage, size)
	return d.updateLru(key)
}

// Get returns the key value or an error if it is not set.
func (d *DataStore) Get(key string) (*KeyData, error) {
	d.rw.RLock()
	defer d.rw.RUnlock()

	i, ok := d.store.Get([]byte(key))
	if !ok {
		return nil, fmt.Errorf("unknown key: %v", key)
	}

	var k KeyData
	k = *i.(*KeyData)

	if err := d.updateLru(key); err != nil {
		return nil, err
	}

	return &k, nil
}

// Delete removes the key if it was previously set or returns an error.
func (d *DataStore) Delete(key string) error {
	d.rw.Lock()
	defer d.rw.Unlock()
	return d.deleteLocked(key)
}

func (d *DataStore) deleteLocked(key string) error {
	defer d.c.Broadcast()

	root, i, ok := d.store.Delete([]byte(key))
	d.store = root
	if !ok {
		return fmt.Errorf("key not found: %v", key)
	}

	k := i.(*KeyData)
	atomic.AddInt64(&d.usage, -1*k.Size)

	// Remove from lru and ttl.
	if d.ttl.Contains(key) {
		if err := d.ttl.Remove(key); err != nil {
			return fmt.Errorf("failed to remove key from ttl tracker: %v", err)
		}
	}

	if err := d.lru.Remove(key); err != nil {
		return fmt.Errorf("failed to remove key from lru tracker: %v", err)
	}

	return nil
}

// Increment the key if it was previously set and is a number. Returns the new
// value or an error.
func (d *DataStore) Increment(key string, amount int64) (int64, error) {
	d.rw.Lock()
	defer d.rw.Unlock()
	defer d.c.Broadcast()

	bKey := []byte(key)
	txn := d.store.Txn()
	i, ok := txn.Get(bKey)
	if !ok {
		return 0, errors.New("key doesn't exist")
	}

	data := convertKeyData(i)
	v, err := strconv.Atoi(string(data.Data))
	if err != nil {
		return 0, fmt.Errorf("value couldn't be converted to integer: %v", v)
	}

	// Increment and convert to a string.
	incr := int64(v) + amount
	var value []byte
	value = strconv.AppendInt(value, incr, 10)
	size := int64(len(value))
	sizeUpdate := data.sizeDiff(size)
	data.setData(value).setSize(size)

	if err := d.lruFree(sizeUpdate); err != nil {
		return 0, err
	}
	txn.Insert(bKey, data)
	d.store = txn.Commit()

	atomic.AddInt64(&d.usage, sizeUpdate)
	if err := d.updateLru(key); err != nil {
		return 0, err
	}

	return incr, nil
}

// Decrement the key if it was previously set and is a number, will not go below
// zero. Returns the new value or an error.
func (d *DataStore) Decrement(key string, amount int64) (int64, error) {
	d.rw.Lock()
	defer d.rw.Unlock()
	defer d.c.Broadcast()

	bKey := []byte(key)
	txn := d.store.Txn()
	i, ok := txn.Get(bKey)
	if !ok {
		return 0, errors.New("key doesn't exist")
	}

	data := convertKeyData(i)
	v, err := strconv.Atoi(string(data.Data))
	if err != nil {
		return 0, fmt.Errorf("value couldn't be converted to integer: %v", v)
	}

	// Decrment and convert to a string.
	decr := int64(v) + amount
	if decr < 0 {
		decr = 0
	}

	var value []byte
	value = strconv.AppendInt(value, decr, 10)
	size := int64(len(value))
	sizeUpdate := data.sizeDiff(size)
	data.setData(value).setSize(size)
	txn.Insert(bKey, data)
	d.store = txn.Commit()

	atomic.AddInt64(&d.usage, sizeUpdate)
	if err := d.updateLru(key); err != nil {
		return 0, err
	}

	return decr, nil
}

// Touch updates the expiration time of a key. The expiration time is in Unix
// seconds, but if zero is passed the key will not expire. An error is returned
// if the key does not exist.
func (d *DataStore) Touch(key string, exp int64) error {
	d.rw.Lock()
	defer d.rw.Unlock()

	bKey := []byte(key)
	txn := d.store.Txn()
	i, ok := txn.Get(bKey)
	if !ok {
		return errors.New("key doesn't exist")
	}

	data := convertKeyData(i).setExp(exp)
	txn.Insert(bKey, data)
	d.store = txn.Commit()

	return d.touch(key, exp)
}

// touch updates the ttl and lru for the key and assumes a lock is held.
func (d *DataStore) touch(key string, exp int64) error {
	if err := d.updateTtl(key, exp); err != nil {
		return err
	}

	return d.updateLru(key)
}

// updateTtl modifies the TtlHeap according to the expiration. Returns an error
// if the key isn't found.
func (d *DataStore) updateTtl(key string, exp int64) error {
	defer d.c.Broadcast()

	var err error
	if exp != 0 {
		if d.ttl.Contains(key) {
			err = d.ttl.Update(key, exp)
		} else {
			err = d.ttl.Push(key, exp)
		}
	} else if d.ttl.Contains(key) {
		err = d.ttl.Remove(key)
	}

	if err != nil {
		return fmt.Errorf("failed to update ttl for key %v: %v", key, err)
	}

	return nil
}

// updateLru moves the passed key to the top of the lru.
func (d *DataStore) updateLru(key string) error {
	now := time.Now().Unix()
	var err error
	if d.lru.Contains(key) {
		err = d.lru.Update(key, now)
	} else {
		err = d.lru.Push(key, now)
	}

	if err != nil {
		return fmt.Errorf("failed to update lru for key %v: %v", key, err)
	}

	return nil
}

// Contains returns whether the key is stored.
func (d *DataStore) Contains(key string) bool {
	d.rw.RLock()
	defer d.rw.RUnlock()

	_, ok := d.store.Get([]byte(key))
	return ok
}
