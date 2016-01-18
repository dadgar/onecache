package onecache

import (
	"sync"

	"stathat.com/c/consistent"
)

// ring is an interface for a consistent hash ring that provides helpers for
// accessing the owner and peers of keys.
type ring interface {
	// add a peer to the ring.
	add(node string)

	// remove a peer from the ring.
	remove(node string)

	// replicaPeers returns the peers of a node.
	replicaPeers(node string) []string

	// successor returns the next node after the passed one in the ring.
	successor(node string) string

	// getOwnedKeys takes in a set of keys are returns the subset that is owned
	// by this key.
	getOwnedKeys(keys []string) []string

	// ownedKeysByNode takes in a set of keys are returns the subset that is owned
	// by the passed node.
	ownedKeysByNode(keys []string, node string) []string

	// ringCopy returns a copy of this node.
	ringCopy() ring
}

// consistentRing is a wrapper around a consistent hash ring that provides
// helper methods for determining successors and replication peers.
type consistentRing struct {
	self     string
	ring     *consistent.Consistent
	replicas int
	lock     sync.Mutex
}

// newConsistentRing returns a constinent ring and takes the name of this node
// and the number of its replicas.
func newConsistentRing(name string, replicas int) *consistentRing {
	return &consistentRing{
		self:     name,
		ring:     consistent.New(),
		replicas: replicas,
	}
}

// getOwnedKeys returns the set of keys owned by this node.
func (c *consistentRing) getOwnedKeys(keys []string) []string {
	return c.ownedKeysByNode(keys, c.self)
}

// getKeysByNodes returns the set of keys owned by the passed node.
func (c *consistentRing) ownedKeysByNode(keys []string, node string) []string {
	var owned []string
	for _, key := range keys {
		replicas := c.replicaPeers(key)
		if len(replicas) > 0 && replicas[0] == node {
			owned = append(owned, key)
		}
	}

	return owned
}

// successor returns the successor of the passed node and if there is none, an
// empty string is returned.
func (c *consistentRing) successor(node string) string {
	m := c.ring.Members()
	if len(m) == 1 {
		return m[0]
	}

	s, s2, err := c.ring.GetTwo(node)
	if err != nil {
		return ""
	}

	if s == node {
		return s2
	}

	return s
}

// replicaPeers returns the peers that this node should replicate to.
func (c *consistentRing) replicaPeers(node string) []string {
	c.lock.Lock()
	defer c.lock.Unlock()

	peers, err := c.ring.GetN(node, c.replicas+1)
	if err != nil {
		return nil
	}

	for i, peer := range peers {
		if peer == node {
			peers = append(peers[:i], peers[i+1:]...)
			break
		}
	}

	return peers
}

// ringCopy returns a copy of the consistentRing.
func (c *consistentRing) ringCopy() ring {
	c.lock.Lock()
	defer c.lock.Unlock()

	ring := &consistentRing{
		self:     c.self,
		ring:     consistent.New(),
		replicas: c.replicas,
	}

	m := c.ring.Members()
	ring.ring.Set(m)
	return ring
}

// add adds a node to the consistent ring.
func (c *consistentRing) add(node string) {
	c.lock.Lock()
	defer c.lock.Unlock()

	c.ring.Add(node)
}

// remove removes a node from the consistent ring.
func (c *consistentRing) remove(node string) {
	c.lock.Lock()
	defer c.lock.Unlock()

	c.ring.Remove(node)
}
