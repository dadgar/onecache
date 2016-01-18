package onecache

import (
	"fmt"
	"testing"
	"time"
)

// mockReplicator can be used by tests to mock the replicator
type mockReplicator struct {
	isReplicating func(key string) bool
}

// newMockReplicator returns a mock replicator that can be used by tests.
// The passed function replaces the IsReplicating function.
func newMockReplicator(isReplicating func(key string) bool) *mockReplicator {
	return &mockReplicator{isReplicating: isReplicating}
}

func (m *mockReplicator) Stop()                         {}
func (m *mockReplicator) NodesRemoved([]string)         {}
func (m *mockReplicator) MarkDirty(string)              {}
func (m *mockReplicator) MarkTouched(key string)        {}
func (m *mockReplicator) MarkDeleted(key string)        {}
func (m *mockReplicator) MarkPeersDirty([]string) error { return nil }
func (m *mockReplicator) IsReplicating(key string) bool { return m.isReplicating(key) }

// Fills a node with key/value pairs with no flags and no expiration. Returns a list of the inserted data. Each entry
// represents a KV pair since keys equal their values.
func fillNode(t *testing.T, n *Node, numKeys int) []string {
	var keysAndValues []string
	for i := 0; i < numKeys; i++ {
		d := fmt.Sprintf("%d", i)
		keysAndValues = append(keysAndValues, d)

		if err := n.data.Set(d, []byte(d), 0, 0); err != nil {
			t.Fatalf("Set(%v) failed: %v", d, err)
		}
	}

	return keysAndValues
}

func TestMarkPeersDirtySingle(t *testing.T) {
	nodes, err := connectedTestNodes(2, 2)
	defer tearDownNodes(nodes)
	if err != nil {
		t.Fatalf("connectedTestNodes(2, 2) failed: %v", err)
	}

	n1 := nodes[0]
	n2 := nodes[1]

	// Fill node one with data.
	keys := fillNode(t, n1, 50)

	// Mark the second node as dirty.
	if err := n1.replicator.MarkPeersDirty([]string{n2.name}); err != nil {
		t.Errorf("MarkPeersDirty(%v) failed: %v", n2.name, err)
	}

	// Check all keys were replicated correctly.
	waitForResult(func() (bool, error) {
		for _, d := range n1.ring.getOwnedKeys(keys) {
			data, err := n2.data.Get(d)
			if err != nil {
				return false, err
			}

			act := string(data.Data)
			if act != d {
				return false, nil
			}
		}
		return true, nil
	}, func(e error) {
		t.Errorf("Replication failed: %v", e)
	})
}

func TestMarkPeersDirtyRepeatedly(t *testing.T) {
	nodes, err := connectedTestNodes(2, 2)
	defer tearDownNodes(nodes)
	if err != nil {
		t.Fatalf("connectedTestNodes(2, 2) failed: %v", err)
	}

	n1 := nodes[0]
	n2 := nodes[1]

	// Fill node one with data.
	keys := fillNode(t, n1, 10)

	for i := 0; i < 5; i++ {
		// Mark the second node as dirty.
		if err := n1.replicator.MarkPeersDirty([]string{n2.name}); err != nil {
			t.Errorf("MarkPeersDirty(%v) failed: %v", n2.name, err)
		}
		time.Sleep(10 * time.Millisecond)
	}

	// Check all keys were replicated correctly.
	waitForResult(func() (bool, error) {
		for _, d := range n1.ring.getOwnedKeys(keys) {
			data, err := n2.data.Get(d)
			if err != nil {
				return false, err
			}

			act := string(data.Data)
			if act != d {
				return false, nil
			}
		}
		return true, nil
	}, func(e error) {
		t.Errorf("Replication failed: %v", e)
	})
}

func TestMarkPeersDirtyMultiple(t *testing.T) {
	nodes, err := connectedTestNodes(4, 4)
	defer tearDownNodes(nodes)
	if err != nil {
		t.Fatalf("connectedTestNodes(4, 4) failed: %v", err)
	}

	n1 := nodes[0]

	// Fill node one with data.
	keys := fillNode(t, n1, 10)

	// Mark the other nodes as dirty.
	var peers []string
	for _, n := range nodes[1:] {
		peers = append(peers, n.name)
	}

	if err := n1.replicator.MarkPeersDirty(peers); err != nil {
		t.Errorf("MarkPeersDirty(%v) failed: %v", peers, err)
	}

	// Check all keys were replicated correctly.
	waitForResult(func() (bool, error) {
		for _, n := range nodes[1:] {
			for _, d := range n1.ring.getOwnedKeys(keys) {
				data, err := n.data.Get(d)
				if err != nil {
					return false, err
				}

				act := string(data.Data)
				if act != d {
					return false, nil
				}
			}
		}
		return true, nil
	}, func(e error) {
		t.Errorf("Replication failed: %v", e)
	})
}

func TestMarkPeersDirtyInvalid(t *testing.T) {
	nodes, err := connectedTestNodes(2, 2)
	defer tearDownNodes(nodes)
	if err != nil {
		t.Fatalf("connectedTestNodes(2, 2) failed: %v", err)
	}

	// Fill node one with data.
	n1 := nodes[0]
	_ = fillNode(t, n1, 100)

	inv := "invalid"
	if err := nodes[0].replicator.MarkPeersDirty([]string{inv}); err == nil {
		t.Errorf("MarkPeersDirty(%v) should have returned an error", inv)
	}
}

// getOwner returns the owner and the original set of nodes without the owner.
func getOwner(key string, nodes []*Node) (*Node, []*Node) {
	if len(nodes) == 0 {
		return nil, nil
	}

	// Get a copy of the ring and clear it.
	r := nodes[0].ring.ringCopy()
	for _, m := range r.(*consistentRing).ring.Members() {
		r.remove(m)
	}

	lookup := make(map[string]*Node, len(nodes))
	for _, n := range nodes {
		lookup[n.name] = n
		r.add(n.name)
	}

	// Get the successor.
	s := r.successor(key)
	owner := lookup[s]

	var followers []*Node
	for name, node := range lookup {
		if name != s {
			followers = append(followers, node)
		}
	}

	return owner, followers
}

func TestMarkDirtyValue(t *testing.T) {
	nodes, err := connectedTestNodes(2, 2)
	defer tearDownNodes(nodes)
	if err != nil {
		t.Fatalf("connectedTestNodes(2, 2) failed: %v", err)
	}

	// Store a value on the owner node.
	k := "foo"
	expV := "bar"
	owner, followers := getOwner(k, nodes)
	if err := owner.data.Set(k, []byte(expV), 0, 0); err != nil {
		t.Fatalf("Set(%v) failed: %v", k, err)
	}
	owner.replicator.MarkDirty(k)

	// Wait for replication.
	var act string
	waitForResult(func() (bool, error) {
		data, err := followers[0].data.Get(k)
		if err != nil {
			return false, err
		}

		act = string(data.Data)
		if act != expV {
			return false, nil
		}
		return true, nil
	}, func(e error) {
		t.Errorf("Stored incorrect value; got %v; want %v", act, expV)
	})
}

func TestMarkDirtyValueRepeated(t *testing.T) {
	nodes, err := connectedTestNodes(2, 2)
	defer tearDownNodes(nodes)
	if err != nil {
		t.Fatalf("connectedTestNodes(2, 2) failed: %v", err)
	}

	// Store a value on the owner node.
	k := "foo"
	var expV string
	owner, followers := getOwner(k, nodes)
	for i := 0; i < 20; i++ {
		expV = fmt.Sprintf("%d", i)
		if err := owner.data.Set(k, []byte(expV), 0, 0); err != nil {
			t.Fatalf("Set(%v) failed: %v", k, err)
		}
		owner.replicator.MarkDirty(k)
	}

	follower := followers[0]

	var act string
	waitForResult(func() (bool, error) {
		data, err := follower.data.Get(k)
		if err != nil {
			return false, err
		}

		act = string(data.Data)
		if act != expV {
			return false, nil
		}
		return true, nil
	}, func(e error) {
		t.Errorf("Stored incorrect value; got %v; want %v", act, expV)
	})
}

func TestMarkDeleted(t *testing.T) {
	nodes, err := connectedTestNodes(2, 2)
	defer tearDownNodes(nodes)
	if err != nil {
		t.Fatalf("connectedTestNodes(2, 2) failed: %v", err)
	}

	// Store a value on the nodes.
	k := "foo"
	for _, node := range nodes {
		if err := node.data.Set(k, []byte("bar"), 0, 0); err != nil {
			t.Fatalf("Set(%v) failed: %v", k, err)
		}
	}

	owner, followers := getOwner(k, nodes)
	owner.replicator.MarkDeleted(k)

	// Check the delete happened
	waitForResult(func() (bool, error) {
		return !followers[0].data.Contains(k), nil
	}, func(e error) {
		t.Errorf("Replication failed: %v", e)
	})
}

func TestMarkTouched(t *testing.T) {
	nodes, err := connectedTestNodes(2, 2)
	defer tearDownNodes(nodes)
	if err != nil {
		t.Fatalf("connectedTestNodes(2, 2) failed: %v", err)
	}

	// Store a key on both nodes but with different experations.
	k := "foo"
	owner, followers := getOwner(k, nodes)
	follower := followers[0]
	exp := time.Now().Add(1 * time.Hour).Unix()
	if err := owner.data.Set(k, []byte("bar"), exp, 0); err != nil {
		t.Fatalf("Set(%v) failed: %v", k, err)
	}

	if err := follower.data.Set(k, []byte("bar"), 0, 0); err != nil {
		t.Fatalf("Set(%v) failed: %v", k, err)
	}

	owner.replicator.MarkTouched(k)

	// Check the key expiration is updated.
	waitForResult(func() (bool, error) {
		data, err := follower.data.Get(k)
		if err != nil {
			return false, nil
		}

		if data.Exp != exp {
			return false, nil
		}
		return true, nil
	}, func(e error) {
		t.Errorf("Replication failed: %v", e)
	})
}

func TestCollapseLogValueTouch(t *testing.T) {
	in := []replicationTask{task_DIRTY_VALUE, task_TOUCH}
	var exp replicationTask = task_DIRTY_VALUE
	task, err := collapseOpLog(in)
	if err != nil {
		t.Errorf("collapseOpLog(%v) returned error: %v", err)
	}

	if task != exp {
		t.Errorf("collapseOpLog(%v) returned %v; want", task, exp)
	}
}

func TestCollapseLogValueDelete(t *testing.T) {
	in := []replicationTask{task_DIRTY_VALUE, task_DELETE}
	var exp replicationTask = task_DELETE
	task, err := collapseOpLog(in)
	if err != nil {
		t.Errorf("collapseOpLog(%v) returned error: %v", err)
	}

	if task != exp {
		t.Errorf("collapseOpLog(%v) returned %v; want", task, exp)
	}
}

func TestCollapseLogDeleteValue(t *testing.T) {
	in := []replicationTask{task_DELETE, task_DIRTY_VALUE}
	var exp replicationTask = task_DIRTY_VALUE
	task, err := collapseOpLog(in)
	if err != nil {
		t.Errorf("collapseOpLog(%v) returned error: %v", err)
	}

	if task != exp {
		t.Errorf("collapseOpLog(%v) returned %v; want", task, exp)
	}
}

func TestCollapseLogeDeleteDelete(t *testing.T) {
	in := []replicationTask{task_DELETE, task_DELETE}
	if _, err := collapseOpLog(in); err == nil {
		t.Errorf("collapseOpLog(%v) should have returned an error")
	}
}

func TestCollapseLogeDeleteTouch(t *testing.T) {
	in := []replicationTask{task_DELETE, task_TOUCH}
	if _, err := collapseOpLog(in); err == nil {
		t.Errorf("collapseOpLog(%v) should have returned an error")
	}
}
