package onecache

import (
	"reflect"
	"testing"
)

type mockRing struct {
	addImpl             func(node string)
	removeImpl          func(node string)
	replicaPeersImpl    func(string) []string
	successorImpl       func(string) string
	getOwnedKeysImpl    func([]string) []string
	ownedKeysByNodeImpl func([]string, string) []string
}

func (m *mockRing) replicaPeers(node string) []string {
	return m.replicaPeersImpl(node)
}

func (m *mockRing) successor(node string) string {
	return m.successorImpl(node)
}

func (m *mockRing) getOwnedKeys(keys []string) []string {
	return m.getOwnedKeysImpl(keys)
}

func (m *mockRing) ownedKeysByNode(keys []string, node string) []string {
	return m.ownedKeysByNodeImpl(keys, node)
}

func (m *mockRing) add(node string)    { m.addImpl(node) }
func (m *mockRing) remove(node string) { m.removeImpl(node) }

func (m *mockRing) ringCopy() ring {
	c := *m
	return &c
}

func TestSuccessorSingle(t *testing.T) {
	n, err := defaultTestNode()
	if err != nil {
		t.Fatalf("defaultTestNode() failed: %v", err)
	}

	// Get the successor and assert it is itself.
	s := n.ring.successor(n.name)
	if s != n.name {
		t.Errorf("successor() returned %v; want %v", s, n.name)
	}
}

func TestSuccessorValid(t *testing.T) {
	nodes, err := connectedTestNodes(2, 1)
	defer tearDownNodes(nodes)
	if err != nil {
		t.Fatalf("connectedTestNodes(2, 1) failed: %v", err)
	}

	// Get the successor and assert there is an error.
	n := nodes[0]
	s := n.ring.successor(n.name)
	expected := nodes[1].name
	if s != expected {
		t.Errorf("successor() returned %v; want %v", s, expected)
	}
}

func TestReplicaPeersKeySingleNode(t *testing.T) {
	node, err := defaultTestNode()
	defer node.Exit()
	if err != nil {
		t.Fatalf("connectedTestNodes(1, 0) failed: %v", err)
	}

	key := "foo"
	peers := node.ring.replicaPeers(key)
	if len(peers) != 1 {
		t.Errorf("replicaPeers(%v) should have one element: %v", key, peers)
		t.FailNow()
	}

	expected := node.name
	actual := peers[0]
	if expected != actual {
		t.Errorf("replicaPeers returned [%v]; expected [%v]", expected, actual)
	}
}

func TestReplicaPeersKeyMultiple(t *testing.T) {
	nodes, err := connectedTestNodes(2, 2)
	defer tearDownNodes(nodes)
	if err != nil {
		t.Fatalf("connectedTestNodes(2, 2) failed: %v", err)
	}

	n1 := nodes[0]
	key := "foo"
	peers := n1.ring.replicaPeers(key)
	if len(peers) != 2 {
		t.Errorf("replicaPeers(%v) should have two elements: %v", key, peers)
		t.FailNow()
	}
}

func TestReplicaPeersSelfSingle(t *testing.T) {
	nodes, err := connectedTestNodes(2, 2)
	defer tearDownNodes(nodes)
	if err != nil {
		t.Fatalf("connectedTestNodes(2, 2) failed: %v", err)
	}

	n1 := nodes[0]
	peers := n1.ring.replicaPeers(n1.name)
	if len(peers) != 1 {
		t.Errorf("replicaPeers(%v) should have one element: %v", n1.name, peers)
		t.FailNow()
	}

	expected := nodes[1].name
	actual := peers[0]
	if expected != actual {
		t.Errorf("replicaPeers returned [%v]; expected [%v]", expected, actual)
	}
}

func TestReplicaSelfPeers(t *testing.T) {
	nodes, err := connectedTestNodes(3, 2)
	defer tearDownNodes(nodes)
	if err != nil {
		t.Fatalf("conenctedTestNodes(3, 2) failed: %v", err)
	}
	n1 := nodes[0]

	peers := n1.ring.replicaPeers(n1.name)
	if len(peers) != 2 {
		t.Errorf("replicaPeers(%v) should have two elements: %v", n1.name, peers)
	}

	expected := []string{nodes[1].name, nodes[2].name}
	for _, e := range expected {
		match := false
		for _, a := range peers {
			if a == e {
				match = true
			}
		}

		if !match {
			t.Errorf("replicaPeers(%v) didn't include expected peer %v", n1.name, e)
		}
	}

	// Get successors a second time and ensure ordering is the same
	peers2 := n1.ring.replicaPeers(n1.name)
	if !reflect.DeepEqual(peers, peers2) {
		t.Errorf("ordering of replicaPeers(%v) should be the same over calls: %v != %v", n1.name, peers, peers2)
	}
}
