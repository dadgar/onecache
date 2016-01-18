package onecache

import (
	"errors"
	"fmt"
	"sync"
	"time"
)

type dataReplicator interface {
	// Stop stops replication.
	Stop()

	// NodesRemoved should be called when a node is removed from the ring to stop
	// replication.
	NodesRemoved(nodes []string)

	// Marks a key's value and metadata as dirty, causing it to be replicated.
	MarkDirty(key string)

	// Marks a key as being touched. Causes the new experation time to be replicated.
	MarkTouched(key string)

	// Marks a key as deleted and replicates the deletion.
	MarkDeleted(key string)

	// Marks a peer as dirty. Causes all keys to be replicated to the node. An error is returned if the peer doesn't
	// exist.
	MarkPeersDirty(peers []string) error

	// IsReplicating returns whether the passed key is being replicated.
	IsReplicating(key string) bool
}

// noOpReplicator is to be used when the node is running singularly and is not part of a cluster.
type noOpReplicator struct{}

func (nop *noOpReplicator) Stop()                               {}
func (nop *noOpReplicator) IsReplicating(key string) bool       { return false }
func (nop *noOpReplicator) MarkDirty(key string)                {}
func (nop *noOpReplicator) MarkTouched(key string)              {}
func (nop *noOpReplicator) MarkDeleted(key string)              {}
func (nop *noOpReplicator) MarkPeersDirty(peers []string) error { return nil }
func (nop *noOpReplicator) NodesRemoved(nodes []string)         {}

// replicator replicates data to peers as they are dirtied.
type replicator struct {
	n     *Node
	peers map[string]*peerSync
	quit  chan struct{}
	lock  sync.Mutex
}

// replicationTask identifies what replication is needed.
type replicationTask byte

const (
	task_DELETE      replicationTask = iota // Key deleted
	task_DIRTY_VALUE                 = iota // Key value changed
	task_TOUCH                       = iota // Key touched
	task_BACKFILL                    = iota // Key has new leader
)

// NewReplicator returns a replicator for the passed node.
func newReplicator(n *Node) *replicator {
	return &replicator{
		n:     n,
		peers: make(map[string]*peerSync),
		quit:  make(chan struct{}),
	}
}

// Stop stops replication.
func (r *replicator) Stop() {
	close(r.quit)
}

// IsReplicating returns if the passed key is being replicated.
func (r *replicator) IsReplicating(key string) bool {
	r.lock.Lock()
	defer r.lock.Unlock()

	for _, peer := range r.peers {
		if peer.isReplicating(key) {
			return true
		}
	}

	return false
}

// NodesRemoved removes the nodes from being replicated to.
func (r *replicator) NodesRemoved(nodes []string) {
	r.lock.Lock()
	defer r.lock.Unlock()

	for _, node := range nodes {
		peer, ok := r.peers[node]
		if !ok {
			return
		}

		peer.stop()
		delete(r.peers, node)
	}
}

// Marks a key's value and metadata as dirty for eventually replication to peers.
func (r *replicator) MarkDirty(key string) {
	for _, p := range r.n.ring.replicaPeers(key) {
		if p == r.n.name {
			continue
		}
		r.getPeerSync(p).markKey(key, task_DIRTY_VALUE)
	}
}

// Marks a key's experation as dirty for eventually replication to peers.
func (r *replicator) MarkTouched(key string) {
	for _, p := range r.n.ring.replicaPeers(key) {
		if p == r.n.name {
			continue
		}
		r.getPeerSync(p).markKey(key, task_TOUCH)
	}
}

// Marks a key as deleted for eventually replication to peers.
func (r *replicator) MarkDeleted(key string) {
	for _, p := range r.n.ring.replicaPeers(key) {
		if p == r.n.name {
			continue
		}
		r.getPeerSync(p).markKey(key, task_DELETE)
	}
}

// Marks peers as dirty causing all keys owned by this node to be replicated to
// it if it is a replica, otherwise it is a no-op. Returns an error if a peer is
// unknown.
func (r *replicator) MarkPeersDirty(peers []string) error {
	// Determine what keys where owned by this node before the peers were added.
	oldRing := r.n.ring.ringCopy()
	for _, p := range peers {
		oldRing.remove(p)
	}
	owned := oldRing.getOwnedKeys(r.n.data.List())

	// Fast path
	if len(owned) == 0 {
		return nil
	}

	for _, peer := range peers {
		if peer == r.n.name {
			continue
		}

		r.n.lock.Lock()
		if _, ok := r.n.nodes[peer]; !ok {
			r.n.lock.Unlock()
			return fmt.Errorf("peer %v not found in ring", peer)
		}
		r.n.lock.Unlock()

		// Replicate all keys if peer is part of this nodes replica peers.
		if r.isReplicaPeer(peer) {
			r.getPeerSync(peer).markKeys(owned, task_DIRTY_VALUE)
		}

		// If the peer is taking ownership of some of our keys, we send the set
		// of keys it owns and mark those for replication.
		if taken := r.takesOwnership(peer); len(taken) != 0 {
			r.getPeerSync(peer).markKeys(taken, task_BACKFILL)
		}
	}

	return nil
}

// isReplicaPeer returns whether the passed peer is part of the replica set.
func (r *replicator) isReplicaPeer(peer string) bool {
	for _, r := range r.n.ring.replicaPeers(r.n.name) {
		if r == peer {
			return true
		}
	}

	return false
}

// takesOwnership returns the keys in which the passed peer is now the owner
// of. If the returned list is empty, the peer didn't take ownership of any keys.
func (r *replicator) takesOwnership(peer string) []string {
	// Check that we are its successor.
	if r.n.ring.successor(peer) != r.n.name {
		return nil
	}

	return r.n.ring.ownedKeysByNode(r.n.data.List(), peer)
}

// getPeerSync returns the peer if it has been creaated, and other wise creates
// it.
func (r *replicator) getPeerSync(peer string) *peerSync {
	r.lock.Lock()
	defer r.lock.Unlock()
	p, ok := r.peers[peer]
	if ok {
		return p
	}

	p = newPeerSync(peer, r)
	r.peers[peer] = p
	return p
}

// peerSync is used to replicate keys to a particular peer
type peerSync struct {
	name        string
	replicating string
	tasks       map[string][]replicationTask // A map of dirty keys to an operations log.
	queue       chan string
	endSync     chan struct{}
	r           *replicator
	lock        sync.Mutex
}

// newPeerSync returns a peerSync for replicating to the passed peer.
func newPeerSync(peer string, r *replicator) *peerSync {
	p := &peerSync{
		name:    peer,
		tasks:   make(map[string][]replicationTask),
		queue:   make(chan string, 100),
		endSync: make(chan struct{}),
		r:       r,
	}
	go p.replicate()
	return p
}

// Stop stops replication to the peer.
func (p *peerSync) stop() {
	close(p.endSync)
}

// isReplicating returns if the key is being replicated to the passed peer.
func (p *peerSync) isReplicating(key string) bool {
	p.lock.Lock()
	defer p.lock.Unlock()
	_, ok := p.tasks[key]
	return ok || p.replicating == key
}

// markKey marks the key for replication.
func (p *peerSync) markKey(key string, status replicationTask) {
	p.lock.Lock()
	p.tasks[key] = append(p.tasks[key], status)
	p.lock.Unlock()
	p.queue <- key
}

// markKeys marks the passed keysfor replication.
func (p *peerSync) markKeys(keys []string, status replicationTask) {
	p.lock.Lock()
	for _, k := range keys {
		p.tasks[k] = append(p.tasks[k], status)
	}
	p.lock.Unlock()

	for _, k := range keys {
		p.queue <- k
	}
}

// replicate is a long lived process that replicates keys to the peer.
func (p *peerSync) replicate() {
	for {
		select {
		case <-p.endSync:
			return
		case <-p.r.quit:
			return
		case key := <-p.queue:
			p.lock.Lock()
			log, ok := p.tasks[key]
			delete(p.tasks, key)
			p.lock.Unlock()
			if !ok {
				continue
			}

			task, err := collapseOpLog(log)
			if err != nil {
				p.r.n.logger.Printf("[ERROR] onecache.replicator: key %v tasks invalid: %v\n", key, err)
				continue
			}

			// Ensure we are still the owner of the key.
			if keys := p.r.n.ring.getOwnedKeys([]string{key}); len(keys) == 0 {
				continue
			}

			p.lock.Lock()
			p.replicating = key
			p.lock.Unlock()

			switch task {
			case task_BACKFILL:
				p.replicateBackfill(key, p.name)
			case task_DIRTY_VALUE:
				p.replicateDirtyValue(key, p.name)
			case task_TOUCH:
				p.replicateTouch(key, p.name)
			case task_DELETE:
				p.replicateDelete(key, p.name)
			}

			p.lock.Lock()
			p.replicating = ""
			p.lock.Unlock()
		}
	}
}

// replicateDirtyValue replicates a key due to a dirty value.
func (p *peerSync) replicateDirtyValue(key, peer string) error {

	// Get the data to transmit.
	data, err := p.r.n.data.Get(key)
	if err != nil {
		return err
	}

	var resp struct{}
	sargs := StorageArgs{
		Key:          key,
		Value:        data.Data,
		Flags:        data.Flags,
		Exp:          data.Exp,
		Cas:          data.Cas,
		ExplicitPeer: peer,
	}
	v := ReplicaVersion{
		Peer:    p.name,
		Version: time.Now().UnixNano(),
	}
	args := ReplicaStorageArgs{sargs, v}

	if err := p.r.n.server.ReplicaSet(args, &resp); err != nil {
		return err
	}

	return nil
}

// replicateBackfill replicates a key due to leadership changes where a backfill
// is necessary.
func (p *peerSync) replicateBackfill(key, peer string) error {

	// Get the data to transmit.
	data, err := p.r.n.data.Get(key)
	if err != nil {
		return err
	}

	var resp struct{}
	args := StorageArgs{
		Key:          key,
		Value:        data.Data,
		Flags:        data.Flags,
		Exp:          data.Exp,
		Cas:          data.Cas,
		ExplicitPeer: peer,
	}

	if err := p.r.n.server.Add(args, &resp); err != nil {
		return err
	}

	return nil
}

// replicateTouch replicates a key due to it being touched.
func (p *peerSync) replicateTouch(key, peer string) error {

	// Get the experation time.
	data, err := p.r.n.data.Get(key)
	if err != nil {
		return err
	}

	var resp struct{}
	sargs := StorageArgs{
		Key:          key,
		Exp:          data.Exp,
		ExplicitPeer: peer,
	}
	v := ReplicaVersion{
		Peer:    p.name,
		Version: time.Now().UnixNano(),
	}
	args := ReplicaStorageArgs{sargs, v}

	return p.r.n.server.ReplicaTouch(args, &resp)
}

// replicateDelete replicates the deletion of a key.
func (p *peerSync) replicateDelete(key, peer string) error {
	var resp struct{}
	args := StorageArgs{
		Key:          key,
		ExplicitPeer: peer,
	}

	return p.r.n.server.Remove(args, &resp)
}

// Collapses the passed operation log returning the final replication task.
func collapseOpLog(log []replicationTask) (replicationTask, error) {
	if len(log) == 0 {
		return task_DIRTY_VALUE, errors.New("can not collapse empty operation log")
	}

	op := log[0]
	for _, s := range log[1:] {
		switch s {
		case task_DELETE:
			if op == task_DELETE {
				return task_DIRTY_VALUE, errors.New("invalid op log: two deletes in a row")
			}

			op = s
		case task_DIRTY_VALUE:
			op = s
		case task_TOUCH:
			if op == task_DELETE {
				return task_DIRTY_VALUE, errors.New("invalid op log: metadata can not be marked dirty after the key has been deleted")
			}
		case task_BACKFILL:
			op = s
		}
	}

	return op, nil
}
