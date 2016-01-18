package ttlstore

import (
	"math/rand"
	"sync"
)

type lockedSource struct {
	lk  sync.Mutex
	src rand.Source
}

func NewLockedSource(seed int64) *lockedSource {
	return &lockedSource{src: rand.NewSource(seed)}
}

func (r *lockedSource) Int63() (n int64) {
	r.lk.Lock()
	n = r.src.Int63()
	r.lk.Unlock()
	return
}

func (r *lockedSource) Seed(seed int64) {
	r.lk.Lock()
	r.src.Seed(seed)
	r.lk.Unlock()
}
