package onecache

import (
	"fmt"
	"reflect"
	"testing"
	"time"
)

func TestCreateInvalid(t *testing.T) {
	if _, err := Create(nil); err == nil {
		t.Error("Create(nil) should have failed")
	}
}

func TestNewRpcServerAnyPort(t *testing.T) {
	n := &Node{}

	// A zero value port allows any port binding.
	err := n.initRpcServer(0)

	if err != nil {
		t.Error(err)
	}

	if n.listener == nil {
		t.Error("newRpcServer(0) did not create a listener")
	}

	if n.port <= 0 {
		t.Error("newRpcServer(0) did not bind to an available port")
	}
}

func TestStartRpcServerSpecificPort(t *testing.T) {
	n := &Node{}

	// A zero value port allows any port binding.
	port := 9100
	err := n.initRpcServer(port)

	if err != nil {
		t.Error(err)
	}

	if n.listener == nil {
		t.Errorf("newRpcServer(%v) did not create a listener", port)
	}

	if n.port != port {
		t.Errorf("newRpcServer(%v) bound to %v; want %v", port, n.port, port)
	}
}

func TestStartRpcServerNegativePort(t *testing.T) {
	n := &Node{}

	// A zero value port allows any port binding.
	if err := n.initRpcServer(-1); err == nil {
		t.Error("newRpcServer(-1) should return an error")
	}
}

func TestExit(t *testing.T) {
	n, err := defaultTestNode()
	if err != nil {
		t.Fatalf("defaultTestNode() failed: %v", err)
	}

	if err := n.Exit(); err != nil {
		t.Errorf("Exit() returned an error: %v", err)
	}
}

func TestJoinInvalid(t *testing.T) {
	n, err := defaultTestNode()
	if err != nil {
		t.Fatalf("defaultTestNode() failed: %v", err)
	}

	others := []string{"junk:7337"}
	if err := n.Join(others); err == nil {
		t.Errorf("Join(%v) should have failed", others)
	}
}

func TestJoinValid(t *testing.T) {
	// Create two Nodes
	config := buildTestConfig(2, 0, 0)
	n, err := Create(config)
	if err != nil {
		t.Fatalf("Create(%+v) failed: %v", config, err)
	}

	config2 := buildTestConfig(2, 0, 0)
	n2, err := Create(config2)
	if err != nil {
		t.Fatalf("Create(%+v) failed: %v", config, err)
	}

	time.Sleep(10 * time.Millisecond)

	if len(n.serf.Members()) != 1 {
		t.Fatalf("s1 members: %d", len(n.serf.Members()))
	}

	if len(n2.serf.Members()) != 1 {
		t.Fatalf("s2 members: %d", len(n2.serf.Members()))
	}

	others := []string{fmt.Sprintf("127.0.0.1:%d", n2.serf.LocalMember().Port)}
	if err := n.Join(others); err != nil {
		t.Errorf("Join(%v) failed: %v", others, err)
	}

	time.Sleep(10 * time.Millisecond)

	if len(n.serf.Members()) != 2 {
		t.Errorf("s1 members: %d", len(n.serf.Members()))
	}

	if len(n2.serf.Members()) != 2 {
		t.Errorf("s2 members: %d", len(n2.serf.Members()))
	}
}

func TestBuildPeerNameValid(t *testing.T) {
	n, err := defaultTestNode()
	if err != nil {
		t.Fatalf("defaultTestNode() failed: %v", err)
	}

	if len(n.serf.Members()) != 1 {
		t.Fatalf("serf members: %d", len(n.serf.Members()))
	}

	// Get the Serf member.
	m := n.serf.Members()[0]

	expected := peerNode{
		name: n.name,
		addr: m.Addr.String(),
		port: n.port,
	}

	peer, err := buildPeerNode(m)
	if err != nil {
		t.Errorf("buildPeerNode(%v) failed: %v", m, err)
	}

	if !reflect.DeepEqual(expected, *peer) {
		t.Errorf("buildPeerNode(%v) returned %v; want %v", m, *peer, expected)
	}
}

func TestNode_GarbageCollection(t *testing.T) {
	n, err := defaultTestNode()
	if err != nil {
		t.Fatalf("defaultTestNode() failed: %v", err)
	}

	// Inject a key that should be GC'd, shouldn't be and those that are
	// replicating.
	keys := []string{"foo", "bar", "baz"}
	for _, k := range keys {
		if err := n.data.Set(k, []byte(k), 0, 0); err != nil {
			t.Fatalf("Set() failed: %v", err)
		}
	}

	// Mock the replicator such that the first key is being replicated.
	n.replicator = newMockReplicator(func(key string) bool {
		if key == keys[0] {
			return true
		}
		return false
	})

	// Mock the ring such that the second key shouldn't be replicated.
	n.ring = &mockRing{
		replicaPeersImpl: func(key string) []string {
			if key == keys[1] {
				return nil
			}

			return []string{n.name}
		},
	}

	gc, replicating := n.getGCKeys()
	if !replicating {
		t.Fatal("getGCKeys() should indicate that a follow up gc is needed")
	}

	if len(gc) != 1 || gc[0] != keys[2] {
		t.Fatalf("getGCKeys() returned wrong key")
	}
}
