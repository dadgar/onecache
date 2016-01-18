package onecache

import (
	"reflect"
	"testing"
	"time"

	"github.com/dadgar/onecache/ttlstore"
)

func TestStoreRemote(t *testing.T) {
	nodes, err := connectedTestNodes(2, 1)
	defer tearDownNodes(nodes)
	if err != nil {
		t.Fatalf("connectedTestNodes(2, 1) failed: %v", err)
	}

	n1 := nodes[0]
	n2 := nodes[1]

	expV := []byte("bar")
	args := StorageArgs{
		Key:          "foo",
		Value:        expV,
		Flags:        0,
		Exp:          0,
		ExplicitPeer: n2.name,
	}

	var resp struct{}
	if err := n1.server.Set(args, &resp); err != nil {
		t.Errorf("Set(%+v) returned an error: %v", args, err)
	}

	// Check the key is actually stored.
	data, err := n2.data.Get(args.Key)
	if err != nil {
		t.Errorf("n2.data.Get(%v) returned error: %v", args.Key, err)
		t.FailNow()
	}

	if !reflect.DeepEqual(data.Data, expV) {
		t.Errorf("Stored incorrect value: want %v; got %v", expV, data.Data)
	}
}

func TestCasRemoteInvalidRemote(t *testing.T) {
	nodes, err := connectedTestNodes(2, 1)
	defer tearDownNodes(nodes)
	if err != nil {
		t.Fatalf("connectedTestNodes(2, 1) failed: %v", err)
	}

	n1 := nodes[0]
	n2 := nodes[1]

	expV := []byte("bar")
	args := StorageArgs{
		Key:          "foo",
		Value:        expV,
		Cas:          10,
		Flags:        0,
		Exp:          0,
		ExplicitPeer: n2.name,
	}

	var resp struct{}
	if err := n1.server.Cas(args, &resp); err == nil {
		t.Errorf("Cas(%+v) returned %v; expected error", args)
	}
}

func TestContainsRemote(t *testing.T) {
	nodes, err := connectedTestNodes(2, 1)
	defer tearDownNodes(nodes)
	if err != nil {
		t.Fatalf("connectedTestNodes(2, 1) failed: %v", err)
	}

	n1 := nodes[0]
	n2 := nodes[1]

	// Store a value on the second node
	k := "foo"
	if err := n2.data.Set(k, []byte("bar"), 0, 0); err != nil {
		t.Fatalf("Set(%v) failed: %v", k, err)
	}

	var resp bool
	args := StorageArgs{Key: k, ExplicitPeer: n2.name}
	if err := n1.server.Contains(args, &resp); err != nil {
		t.Errorf("Contains(%+v) returned an error %v", args, err)
	} else if !resp {
		t.Errorf("Contains(%+v) returned false; want true (%v)", args, resp)
	}
}

func TestContainsRemoteInvalid(t *testing.T) {
	nodes, err := connectedTestNodes(2, 1)
	defer tearDownNodes(nodes)
	if err != nil {
		t.Fatalf("connectedTestNodes(2, 1) failed: %v", err)
	}

	n1 := nodes[0]
	n2 := nodes[1]

	var resp bool
	args := StorageArgs{Key: "foo", ExplicitPeer: n2.name}
	if err := n1.server.Contains(args, &resp); err != nil {
		t.Fatalf("Contains(%+v) returned an error: %v", args, err)
	}

	if resp {
		t.Errorf("Contains(%+v) returned %v; want false", args, resp)
	}
}

func TestTouchRemote(t *testing.T) {
	nodes, err := connectedTestNodes(2, 1)
	defer tearDownNodes(nodes)
	if err != nil {
		t.Fatalf("connectedTestNodes(2, 1) failed: %v", err)
	}

	n1 := nodes[0]
	n2 := nodes[1]

	// Store a value on the second node
	k := "foo"
	if err := n2.data.Set(k, []byte("bar"), 0, 0); err != nil {
		t.Fatalf("Set(%v) failed: %v", k, err)
	}

	var resp struct{}
	var exp int64 = time.Now().Add(1 * time.Hour).Unix()
	args := StorageArgs{Key: k, Exp: exp, ExplicitPeer: n2.name}
	if err := n1.server.Touch(args, &resp); err != nil {
		t.Errorf("Touch(%+v) returned an error %v", args, err)
	}

	// Check the new experation is correct.
	data, err := n2.data.Get(k)
	if err != nil {
		t.Errorf("Get(%v) failed: %v", k, err)
		t.FailNow()
	}

	if data.Exp != exp {
		t.Errorf("Experation is %v; want %v", data.Exp, exp)
	}
}

func TestTouchRemoteInvalid(t *testing.T) {
	nodes, err := connectedTestNodes(2, 1)
	defer tearDownNodes(nodes)
	if err != nil {
		t.Fatalf("connectedTestNodes(2, 1) failed: %v", err)
	}

	n1 := nodes[0]
	n2 := nodes[1]

	var resp struct{}
	args := StorageArgs{Key: "foo", Exp: 10, ExplicitPeer: n2.name}
	if err := n1.server.Touch(args, &resp); err == nil {
		t.Errorf("Touch(%+v) didn't return error", args)
	}
}

func TestRemoveRemote(t *testing.T) {
	nodes, err := connectedTestNodes(2, 1)
	defer tearDownNodes(nodes)
	if err != nil {
		t.Fatalf("connectedTestNodes(2, 1) failed: %v", err)
	}

	n1 := nodes[0]
	n2 := nodes[1]

	// Store a value on the second node
	k := "foo"
	if err := n2.data.Set(k, []byte("bar"), 0, 0); err != nil {
		t.Fatalf("Set(%v) failed: %v", k, err)
	}

	var resp struct{}
	args := StorageArgs{Key: k, ExplicitPeer: n2.name}
	if err := n1.server.Remove(args, &resp); err != nil {
		t.Errorf("Remove(%+v) returned an error %v", args, err)
	}

	// Check the key is actually deleted.
	if n2.data.Contains(k) {
		t.Errorf("Contains(%v) returned true; want false", k)
	}
}

func TestRemoveRemoteInvalid(t *testing.T) {
	nodes, err := connectedTestNodes(2, 1)
	defer tearDownNodes(nodes)
	if err != nil {
		t.Fatalf("connectedTestNodes(2, 1) failed: %v", err)
	}

	n1 := nodes[0]
	n2 := nodes[1]

	var resp struct{}
	args := StorageArgs{Key: "foo", ExplicitPeer: n2.name}
	if err := n1.server.Remove(args, &resp); err == nil {
		t.Errorf("Remove(%+v) didn't return error", args)
	}
}

func TestGetRemote(t *testing.T) {
	nodes, err := connectedTestNodes(2, 1)
	defer tearDownNodes(nodes)
	if err != nil {
		t.Fatalf("connectedTestNodes(2, 1) failed: %v", err)
	}

	n1 := nodes[0]
	n2 := nodes[1]

	// Store a value on the second node
	k := "foo"
	expV := []byte("bar")
	if err := n2.data.Set(k, expV, 0, 0); err != nil {
		t.Fatalf("Set(%v) failed: %v", k, err)
	}

	var resp ttlstore.KeyData
	args := StorageArgs{Key: k, ExplicitPeer: n2.name}
	if n1.server.Get(args, &resp); err != nil {
		t.Errorf("Get(%+v) returned an error %v", args, err)
		t.FailNow()
	}

	// Check the value returned is correct.
	if !reflect.DeepEqual(resp.Data, expV) {
		t.Errorf("Get(%v) returned %v; want %v", args, resp.Data, expV)
	}
}

func TestGetRemoteInvalid(t *testing.T) {
	nodes, err := connectedTestNodes(2, 1)
	defer tearDownNodes(nodes)
	if err != nil {
		t.Fatalf("connectedTestNodes(2, 1) failed: %v", err)
	}

	n1 := nodes[0]
	n2 := nodes[1]

	var resp ttlstore.KeyData
	args := StorageArgs{Key: "foo", ExplicitPeer: n2.name}
	if err := n1.server.Get(args, &resp); err == nil {
		t.Errorf("Get(%+v) didn't return error", args)
	}
}
