package onecache

import (
	"fmt"
	"io"
	"net"
	"net/rpc"
	"sync"
	"time"

	"github.com/dadgar/onecache/ttlstore"
	"github.com/hashicorp/net-rpc-msgpackrpc"
	"github.com/hashicorp/yamux"
	"github.com/ugorji/go/codec"
)

const (
	// nodeRPCCache controls how long we keep an idle connection open to a node
	nodeRPCCache = 2 * time.Minute

	// nodeMaxStreams controsl how many idle streams we keep open to a node
	nodeMaxStreams = 64
)

var (
	mh codec.Handle = new(codec.MsgpackHandle)
)

// rpcServer handles rpc connections between nodes.
type rpcServer struct {
	n        *Node
	l        sync.Mutex
	versions map[string]*ReplicaVersion
	rpc      *rpc.Server

	// Connection pool to other nodes
	connPool *connPool
}

// newRpcServer returns an rpc server suitable for handling RPC connections for
// the passed node.
func newRpcServer(n *Node) *rpcServer {
	s := &rpcServer{
		n:        n,
		versions: make(map[string]*ReplicaVersion),
		rpc:      rpc.NewServer(),
		connPool: newPool(n.config.LogOutput, nodeRPCCache, nodeMaxStreams),
	}

	s.rpc.RegisterName("Node", s)
	return s
}

// shutdown stops the rpc server from handling new connections.
func (r *rpcServer) shutdown() error {
	return r.connPool.Shutdown()
}

// handleMultiplex handles a multiplexed connection.
func (r *rpcServer) handleMultiplex(conn net.Conn) {
	defer conn.Close()
	conf := yamux.DefaultConfig()
	conf.LogOutput = r.n.config.LogOutput
	server, _ := yamux.Server(conn, conf)
	for {
		sub, err := server.Accept()
		if err != nil {
			if err != io.EOF {
				r.n.logger.Printf("[ERR] onecache.rpc: multiplex conn accept failed: %v", err)
			}
			return
		}
		go r.handleConn(sub)
	}
}

// handleConn handles a non-multiplexed connection.
func (r *rpcServer) handleConn(conn net.Conn) {
	rpcCodec := msgpackrpc.NewServerCodec(conn)
	for {
		select {
		case <-r.n.quitCh:
			return
		default:
			break
		}

		r.rpc.ServeRequest(rpcCodec)
	}
}

// targetNode takes the key and the explicitPeer value from the RPC and returns
// the target node for the connection.
func (r *rpcServer) targetNode(key, explictPeer string) string {
	if explictPeer != "" {
		return explictPeer
	}

	return r.n.ring.successor(key)
}

// forward forwards the rpc to the passed target node.
func (r *rpcServer) forward(target, method string, args interface{}, reply interface{}) error {
	r.n.lock.Lock()
	p, ok := r.n.nodes[target]
	r.n.lock.Unlock()
	if !ok {
		return fmt.Errorf("could not dial unknown peer %v", target)
	}

	addr, err := net.ResolveTCPAddr("tcp", fmt.Sprintf("%s:%d", p.addr, p.port))
	if err != nil {
		return fmt.Errorf("Failed to resolve tcp address of peer %v: %v", target, err)
	}

	err = r.connPool.RPC(addr, method, args, reply)
	return err
}

// StorageArgs is the struct used to make data related RPC calls.
type StorageArgs struct {
	Key   string
	Value []byte
	Flags int32
	Exp   int64
	Cas   int64

	// Used for increment and decrement.
	Delta int64

	// Used internally as a means to determine when to forward.
	ExplicitPeer string
}

// markClientVersion is used to mark that a key was set by a client and not by
// replication.
func (r *rpcServer) markClientVersion(key string) {
	r.l.Lock()
	defer r.l.Unlock()
	delete(r.versions, key)
}

// Set is used to set the value of a key.
func (r *rpcServer) Set(args StorageArgs, resp *struct{}) error {
	target := r.targetNode(args.Key, args.ExplicitPeer)
	if target != r.n.name {
		return r.forward(target, "Node.Set", args, resp)
	}

	if err := r.n.data.Set(args.Key, args.Value, args.Exp, args.Flags); err != nil {
		return err
	}

	r.n.replicator.MarkDirty(args.Key)
	r.markClientVersion(args.Key)
	return nil
}

// Cas is used to set the value of a key using a check and set.
func (r *rpcServer) Cas(args StorageArgs, resp *struct{}) error {
	target := r.targetNode(args.Key, args.ExplicitPeer)
	if target != r.n.name {
		return r.forward(target, "Node.Cas", args, resp)
	}

	if err := r.n.data.Cas(args.Key, args.Value, args.Exp, args.Cas, args.Flags); err != nil {
		return err
	}

	r.n.replicator.MarkDirty(args.Key)
	r.markClientVersion(args.Key)
	return nil
}

// Add adds a key only if it doesn't currently exist.
func (r *rpcServer) Add(args StorageArgs, resp *struct{}) error {
	target := r.targetNode(args.Key, args.ExplicitPeer)
	if target != r.n.name {
		return r.forward(target, "Node.Add", args, resp)
	}

	if err := r.n.data.Add(args.Key, args.Value, args.Exp, args.Flags); err != nil {
		return err
	}

	r.n.replicator.MarkDirty(args.Key)
	r.markClientVersion(args.Key)
	return nil
}

// Replace sets the content of a key only if it exists.
func (r *rpcServer) Replace(args StorageArgs, resp *struct{}) error {
	target := r.targetNode(args.Key, args.ExplicitPeer)
	if target != r.n.name {
		return r.forward(target, "Node.Replace", args, resp)
	}

	if err := r.n.data.Replace(args.Key, args.Value, args.Exp, args.Flags); err != nil {
		return err
	}

	r.n.replicator.MarkDirty(args.Key)
	r.markClientVersion(args.Key)
	return nil
}

// Append appends to the contents of a key.
func (r *rpcServer) Append(args StorageArgs, resp *struct{}) error {
	target := r.targetNode(args.Key, args.ExplicitPeer)
	if target != r.n.name {
		return r.forward(target, "Node.Append", args, resp)
	}

	if err := r.n.data.Append(args.Key, args.Value); err != nil {
		return err
	}

	r.n.replicator.MarkDirty(args.Key)
	r.markClientVersion(args.Key)
	return nil
}

// Prepend prepends to the contents of a key.
func (r *rpcServer) Prepend(args StorageArgs, resp *struct{}) error {
	target := r.targetNode(args.Key, args.ExplicitPeer)
	if target != r.n.name {
		return r.forward(target, "Node.Prepend", args, resp)
	}

	if err := r.n.data.Prepend(args.Key, args.Value); err != nil {
		return err
	}

	r.n.replicator.MarkDirty(args.Key)
	r.markClientVersion(args.Key)
	return nil
}

// Increment increments the value of a key.
func (r *rpcServer) Increment(args StorageArgs, resp *int64) error {
	target := r.targetNode(args.Key, args.ExplicitPeer)
	if target != r.n.name {
		return r.forward(target, "Node.Increment", args, resp)
	}

	incr, err := r.n.data.Increment(args.Key, args.Delta)
	if err != nil {
		return err
	}
	*resp = incr

	r.n.replicator.MarkDirty(args.Key)
	r.markClientVersion(args.Key)
	return nil
}

// Decrement decrements the value of a key.
func (r *rpcServer) Decrement(args StorageArgs, resp *int64) error {
	target := r.targetNode(args.Key, args.ExplicitPeer)
	if target != r.n.name {
		return r.forward(target, "Node.Decrement", args, resp)
	}

	decr, err := r.n.data.Decrement(args.Key, args.Delta)
	if err != nil {
		return err
	}
	*resp = decr

	r.n.replicator.MarkDirty(args.Key)
	r.markClientVersion(args.Key)
	return nil
}

// Contains returns whether a key exists.
func (r *rpcServer) Contains(args StorageArgs, resp *bool) error {
	target := r.targetNode(args.Key, args.ExplicitPeer)
	if target != r.n.name {
		return r.forward(target, "Node.Contains", args, resp)
	}

	*resp = r.n.data.Contains(args.Key)
	r.markClientVersion(args.Key)
	return nil
}

// Touch is used to set the experation of a key.
func (r *rpcServer) Touch(args StorageArgs, resp *struct{}) error {
	target := r.targetNode(args.Key, args.ExplicitPeer)
	if target != r.n.name {
		return r.forward(target, "Node.Touch", args, resp)
	}

	if err := r.n.data.Touch(args.Key, args.Exp); err != nil {
		return err
	}

	r.n.replicator.MarkTouched(args.Key)
	r.markClientVersion(args.Key)
	return nil
}

// Remove removes the key.
func (r *rpcServer) Remove(args StorageArgs, resp *struct{}) error {
	target := r.targetNode(args.Key, args.ExplicitPeer)
	if target != r.n.name {
		return r.forward(target, "Node.Remove", args, resp)
	}

	if err := r.n.data.Delete(args.Key); err != nil {
		return err
	}

	r.n.replicator.MarkDeleted(args.Key)
	r.markClientVersion(args.Key)
	return nil
}

// Get returns the value of the key.
func (r *rpcServer) Get(args StorageArgs, resp *ttlstore.KeyData) error {
	target := r.targetNode(args.Key, args.ExplicitPeer)
	if target != r.n.name {
		return r.forward(target, "Node.Get", args, resp)
	}

	data, err := r.n.data.Get(args.Key)
	if err != nil {
		return err
	}

	*resp = *data
	return nil
}

// ReplicaStorageArgs is a versioned key set by a replica peer.
type ReplicaStorageArgs struct {
	StorageArgs
	ReplicaVersion
}

// ReplicaVersion is used to version replicated data of a key from a peer.
type ReplicaVersion struct {
	// The peer sending the replica information
	Peer string

	// A version which should only increment
	Version int64
}

// ReplicaSet is used for setting the value of a key to a replica. It allows
// setting the version of the key so that only the most updated version of the
// value is stored.
func (r *rpcServer) ReplicaSet(args ReplicaStorageArgs, resp *struct{}) error {
	target := r.targetNode(args.Key, args.ExplicitPeer)
	if target != r.n.name {
		return r.forward(target, "Node.ReplicaSet", args, resp)
	}

	r.l.Lock()
	defer r.l.Unlock()
	v := r.versions[args.Key]

	// Check if we are trying to set an old version.
	if v != nil && v.Peer == args.Peer && args.Version <= v.Version {
		return nil
	}

	if err := r.n.data.Set(args.Key, args.Value, args.Exp, args.Flags); err != nil {
		return err
	}

	// Update the replica version.
	r.versions[args.Key] = &args.ReplicaVersion
	return nil
}

// ReplicaTouch is used for setting the expiration of a key on a replica. It
// allows setting the version of the key so that only the most updated version
// of the value is stored.
func (r *rpcServer) ReplicaTouch(args ReplicaStorageArgs, resp *struct{}) error {
	target := r.targetNode(args.Key, args.ExplicitPeer)
	if target != r.n.name {
		return r.forward(target, "Node.ReplicaTouch", args, resp)
	}

	r.l.Lock()
	defer r.l.Unlock()
	v := r.versions[args.Key]

	// Check if we are trying to set an old version.
	if v != nil && v.Peer == args.Peer && args.Version <= v.Version {
		return nil
	}

	if err := r.n.data.Touch(args.Key, args.Exp); err != nil {
		return err
	}

	// Update the replica version.
	r.versions[args.Key] = &args.ReplicaVersion
	return nil
}
