package ttlstore

import (
	"log"
	"os"
	"reflect"
	"testing"
	"time"
)

const (
	testKey            = "foo"
	testFlags          = 1
	testNoExpire int64 = 0
)

var (
	testValue = []byte("bar")
	testSize  = int64(len(testValue))
)

var (
	logger = log.New(os.Stderr, "", log.LstdFlags)
)

func TestNew(t *testing.T) {
	t.Parallel()
	var max int64 = 100
	if _, err := New(max, logger); err != nil {
		t.Error("New(%v) failed: %v", max, err)
	}

	max = -100
	if _, err := New(max, logger); err == nil {
		t.Error("New(%v) should have failed: %v", max, err)
	}
}

func testDataStore(t testing.TB, max int64) *DataStore {
	d, err := New(max, logger)
	if err != nil {
		t.Fatal("New(%v) failed: %v", max, err)
	}
	return d
}

func TestGetInvalidKey(t *testing.T) {
	t.Parallel()
	// Get an invalid key.
	d := testDataStore(t, 500)
	defer d.Destroy()
	k := "foo"
	if _, e := d.Get(k); e == nil {
		t.Errorf("Get(%v) of an invalid key should have failed", k)
	}
}

func TestSetAndGet(t *testing.T) {
	t.Parallel()
	d := testDataStore(t, 500)
	defer d.Destroy()
	if err := d.Set(testKey, testValue, testNoExpire, testFlags); err != nil {
		t.Errorf("Set(%v, %v, %v, %v) failed: %v", testKey, testValue, testNoExpire, testFlags, err)
	}

	data, err := d.Get(testKey)
	if err != nil {
		t.Errorf("Get(%v) failed: %v", testKey, err)
	}

	// Compare expected to actual.
	if !reflect.DeepEqual(data.Data, testValue) {
		t.Errorf("Get(%v) returned key %v; want %v", testKey, data.Data, testValue)
	}

	if data.Size != testSize {
		t.Errorf("Get(%v) returned size %v; want %v", testKey, data.Size, testSize)
	}

	if data.Flags != testFlags {
		t.Errorf("Get(%v) returned flags %v; want %v", testKey, data.Flags, testFlags)
	}
}

func TestSetLRU(t *testing.T) {
	t.Parallel()
	// Only allocate 5 bytes of memory so that two inserts force an LRU eviction.
	d := testDataStore(t, 5)
	defer d.Destroy()
	if err := d.Set(testKey, testValue, testNoExpire, testFlags); err != nil {
		t.Fatalf("Set(%v, %v, %v, %v) failed: %v", testKey, testValue, testNoExpire, testFlags, err)
	}

	// Check the key is in memory.
	if _, err := d.Get(testKey); err != nil {
		t.Errorf("Get(%v) failed: %v", testKey, err)
	}

	// Add the second key and expect the first to be LRU evicted.
	k2 := "bar"
	expV := []byte("baz")
	expS := int64(len(expV))
	var expF int32 = 2
	if err := d.Set(k2, expV, testNoExpire, expF); err != nil {
		t.Errorf("Set(%v, %v, %v, %v) failed: %v", k2, expV, testNoExpire, expF, err)
	}

	// Check the first key is not in memory but the second is.
	if _, err := d.Get(testKey); err != nil {
		t.Error("first key still in stored")
	}

	if _, err := d.Get(k2); err != nil {
		t.Error("second key not stored")
	}

	data, err := d.Get(k2)
	if err != nil {
		t.Errorf("Get(%v) failed: %v", k2, err)
	}

	// Compare expected to actual.
	if !reflect.DeepEqual(data.Data, expV) {
		t.Errorf("Get(%v) returned key %v; want %v", k2, data.Data, expV)
	}

	if data.Size != expS {
		t.Errorf("Get(%v) returned size %v; want %v", k2, data.Size, expS)
	}

	if data.Flags != expF {
		t.Errorf("Get(%v) returned flags %v; want %v", k2, data.Flags, expF)
	}
}

func TestSetTooLarge(t *testing.T) {
	t.Parallel()
	d := testDataStore(t, 5)
	defer d.Destroy()
	v := []byte("this is way too large")
	if err := d.Set(testKey, v, testNoExpire, testFlags); err == nil {
		t.Errorf("Set(%v, %v, %v, %v) should have failed for being too large", testKey, v, testNoExpire, testFlags)
	}
}

func TestCasValid(t *testing.T) {
	t.Parallel()
	d := testDataStore(t, 500)
	defer d.Destroy()
	if err := d.Set(testKey, testValue, testNoExpire, testFlags); err != nil {
		t.Fatalf("Set(%v, %v, %v, %v) failed: %v", testKey, testValue, testNoExpire, testFlags, err)
	}

	data, err := d.Get(testKey)
	if err != nil {
		t.Fatalf("Get(%v) failed: %v", testKey, err)
	}

	expV := []byte("cas")
	cas := data.Cas
	if err := d.Cas(testKey, expV, testNoExpire, cas, testFlags); err != nil {
		t.Errorf("Cas(%v, %v, %v, %v, %v) failed: %v", testKey, expV, testNoExpire, cas, testFlags, err)
	}

	data, err = d.Get(testKey)
	if err != nil {
		t.Errorf("Get(%v) failed: %v", testKey, err)
	}

	// Compare epected to actual.
	if !reflect.DeepEqual(data.Data, expV) {
		t.Errorf("Get(%v) returned key %v; want %v", testKey, data.Data, expV)
	}

	if data.Cas == cas {
		t.Errorf("Get(%v) returned same cas value %v", testKey, cas)
	}
}

func TestCasInvalid(t *testing.T) {
	t.Parallel()
	d := testDataStore(t, 500)
	defer d.Destroy()
	if err := d.Set(testKey, testValue, testNoExpire, testFlags); err != nil {
		t.Fatalf("Set(%v, %v, %v, %v) failed: %v", testKey, testValue, testNoExpire, testFlags, err)
	}

	data, err := d.Get(testKey)
	if err != nil {
		t.Fatalf("Get(%v) failed: %v", testKey, err)
	}

	expV := []byte("cas")
	cas := data.Cas - 1
	if err := d.Cas(testKey, expV, testNoExpire, cas, testFlags); err == nil {
		t.Errorf("Cas(%v, %v, %v, %v, %v) with incorrect cas should have failed", testKey, expV, testNoExpire, cas, testFlags)
	}
}

func TestContainsMemory(t *testing.T) {
	t.Parallel()
	// Force writes to go to memory.
	d := testDataStore(t, 50)
	defer d.Destroy()
	if err := d.Set(testKey, testValue, testNoExpire, testFlags); err != nil {
		t.Fatalf("Set(%v, %v, %v, %v) failed: %v", testKey, testValue, testNoExpire, testFlags, err)
	}

	if c := d.Contains(testKey); !c {
		t.Errorf("Contains(%v) should return true as it is on disk", testKey)
	}
}

func TestContainsEmpty(t *testing.T) {
	t.Parallel()
	d := testDataStore(t, 500)
	defer d.Destroy()
	if d.Contains(testKey) {
		t.Errorf("Contains(%v) on empty DataStore returned true", testKey)
	}
}

func TestRemoveEmpty(t *testing.T) {
	t.Parallel()
	d := testDataStore(t, 500)
	defer d.Destroy()
	if err := d.Delete(testKey); err == nil {
		t.Errorf("Remove(%v) on empty DataStore should return error", testKey)
	}
}

func TestRemove(t *testing.T) {
	t.Parallel()
	// Force writes to go into memory.
	d := testDataStore(t, 50)
	defer d.Destroy()
	if err := d.Set(testKey, testValue, testNoExpire, testFlags); err != nil {
		t.Fatalf("Set(%v, %v, %v, %v) failed: %v", testKey, testValue, testNoExpire, testFlags, err)
	}

	if c := d.Contains(testKey); !c {
		t.Fatalf("Contains(%v) should return true as it is in memory", testKey)
	}

	if err := d.Delete(testKey); err != nil {
		t.Errorf("Remove(%v) failed: %v", testKey, err)
	}

	if c := d.Contains(testKey); c {
		t.Errorf("Contains(%v) should return false as the key has been removed", testKey)
	}
}

func TestTtl(t *testing.T) {
	t.Parallel()
	// Force writes to go into memory.
	d := testDataStore(t, 50)
	defer d.Destroy()
	e := time.Now().Add(1 * time.Second).Unix()
	if err := d.Set(testKey, testValue, e, testFlags); err != nil {
		t.Fatalf("Set(%v, %v, %v, %v) failed: %v", testKey, testValue, e, testFlags, err)
	}

	if c := d.Contains(testKey); !c {
		t.Fatalf("Contains(%v) should return true as it is in memory", testKey)
	}

	// Sleep pass the TTL time.
	time.Sleep(1500 * time.Millisecond)

	if c := d.Contains(testKey); c {
		t.Errorf("Contains(%v) should return false as the key should be TTL'd", testKey)
	}
}

func TestTouchInvalidTtl(t *testing.T) {
	t.Parallel()
	d := testDataStore(t, 50)
	defer d.Destroy()
	e := time.Now().Add(2 * time.Second).Unix()
	if err := d.Touch(testKey, e); err == nil {
		t.Errorf("Touch(%v, %v) should have failed as key does not exist", testKey, e)
	}
}

func TestTouchValidTtl(t *testing.T) {
	t.Parallel()
	// Force writes to go into memory.
	d := testDataStore(t, 50)
	defer d.Destroy()
	now := time.Now()
	e := now.Add(2 * time.Second).Unix()
	e2 := now.Add(4 * time.Second).Unix()
	if err := d.Set(testKey, testValue, e, testFlags); err != nil {
		t.Fatalf("Set(%v, %v, %v, %v) failed: %v", testKey, testValue, e, testFlags, err)
	}

	time.Sleep(500 * time.Millisecond)

	if err := d.Touch(testKey, e2); err != nil {
		t.Errorf("Touch(%v, %v) failed: %v", testKey, e2, err)
	}

	// Wake up at the original expiration and assert it is still there.
	time.Sleep(1800 * time.Millisecond)
	if c := d.Contains(testKey); !c {
		t.Errorf("Contains(%v) should be true as the key was touched", testKey)
	}

	// Wake up past the touch expiration time.
	time.Sleep(2 * time.Second)
	if c := d.Contains(testKey); c {
		t.Errorf("Contains(%v) should be false as the key was TTL'd", testKey)
	}
}

func TestTtlMultiple(t *testing.T) {
	t.Parallel()
	// Force writes to go into memory.
	d := testDataStore(t, 50)
	defer d.Destroy()
	now := time.Now()
	e := now.Add(1 * time.Second).Unix()
	e2 := now.Add(2 * time.Second).Unix()
	if err := d.Set(testKey, testValue, e, testFlags); err != nil {
		t.Fatalf("Set(%v, %v, %v, %v) failed: %v", testKey, testValue, e, testFlags, err)
	}

	keyTwo := "keyTwo"
	if err := d.Set(keyTwo, testValue, e2, testFlags); err != nil {
		t.Fatalf("Set(%v, %v, %v, %v) failed: %v", testKey, testValue, e, testFlags, err)
	}

	// Wake up after both should have been ttl'd.
	time.Sleep(3 * time.Second)
	if c := d.Contains(testKey); c {
		t.Errorf("Contains(%v) should be false as the key has expired", testKey)
	}

	if c := d.Contains(keyTwo); c {
		t.Errorf("Contains(%v) should be false as the key has expired", keyTwo)
	}
}

func TestTtlAbort(t *testing.T) {
	t.Parallel()
	// Force writes to go into memory.
	d := testDataStore(t, 50)
	defer d.Destroy()
	now := time.Now()
	e := now.Add(1 * time.Second).Unix()
	if err := d.Set(testKey, testValue, e, testFlags); err != nil {
		t.Fatalf("Set(%v, %v, %v, %v) failed: %v", testKey, testValue, e, testFlags, err)
	}

	// Touch the key to abort.
	if err := d.Touch(testKey, 0); err != nil {
		t.Fatalf("Touch(%v, 0) failed: %v", testKey, err)
	}

	// Wake up after both should have been ttl'd.
	time.Sleep(2 * time.Second)
	if c := d.Contains(testKey); !c {
		t.Errorf("Contains(%v) should be true as the key was touched", testKey)
	}
}
