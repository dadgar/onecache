package onecache

import (
	"errors"
	"fmt"
	"log"
	"net"
	"strconv"
	"sync"
	"time"

	"os"

	"github.com/dadgar/onecache/ttlstore"
	"github.com/hashicorp/serf/serf"
	"github.com/pborman/uuid"
)

const (
	// externalRpcTag is used to create a serf tag to broadcast the port being
	// used for RPC.
	externalRpcTag = "externalRpcTag"
)

// peerNode holds the required information for sharding and forwarding RPCs to a
// peer node in the ring.
type peerNode struct {
	name string
	addr string
	port int
}

// Node holds all the necessary information to store data, handle client
// connections, forward among the ring and replicate.
type Node struct {
	name       string
	logger     *log.Logger
	serf       *serf.Serf
	eventCh    chan serf.Event
	quitCh     chan bool
	ring       ring
	replicas   int
	nodes      map[string]*peerNode
	data       *ttlstore.DataStore
	replicator dataReplicator
	port       int
	listener   net.Listener
	shutdown   bool
	server     *rpcServer
	lock       sync.Mutex
	config     *NodeConfig
	gc         *time.Timer
}

// Create creates a new Node instance and initializes all resources necessary to
// be a member of the hash ring.
func Create(config *NodeConfig) (*Node, error) {
	if err := configValidator(config); err != nil {
		return nil, err
	}

	node := &Node{
		name:     uuid.New(),
		eventCh:  make(chan serf.Event),
		quitCh:   make(chan bool),
		replicas: config.Replicas,
		nodes:    make(map[string]*peerNode),
		config:   config,
	}

	// RPC Endpoint
	node.server = newRpcServer(node)

	// Initialize the ring.
	node.ring = newConsistentRing(node.name, config.Replicas)

	// Set up logging.
	if config.LogOutput == nil {
		config.LogOutput = os.Stderr
	}
	node.logger = log.New(config.LogOutput, "", log.LstdFlags)

	// Initialize the data storage.
	d, err := ttlstore.New(config.MaxMemoryUsage, node.logger)
	if err != nil {
		return nil, fmt.Errorf("DataStore initialization failed: %v", err)
	}
	node.data = d

	// Initialize replication
	if config.Replicas == 1 {
		node.replicator = &noOpReplicator{}
	} else {
		node.replicator = newReplicator(node)
	}

	// Initialize garbage collection.
	node.gc = time.AfterFunc(10*time.Minute, node.garbageCollect)
	node.gc.Stop()

	// Initialize the RPC Server
	if err := node.initRpcServer(config.RpcPort); err != nil {
		return nil, fmt.Errorf("RPC server initialization failed: %v", err)
	}

	if err := node.initSerf(config.SerfConfig); err != nil {
		return nil, fmt.Errorf("serf initialization failed: %v", err)
	}

	return node, nil
}

// Start starts handling connections from other rings in the cluster as well as
// clients.
func (n *Node) Start() {
	go n.serfEventHandler()
	go n.listen()
}

// configValidator ensures that the configuration is valid and returns an error
// if it is not.
func configValidator(config *NodeConfig) error {
	if config == nil {
		return errors.New("must supply a non-nil config")
	}

	if config.Replicas < 1 {
		return errors.New("must have at least one replica")
	}

	return nil
}

// initSerf initializes serf and advertizes this nodes RPC port to other nodes.
func (n *Node) initSerf(config *serf.Config) error {
	config.NodeName = n.name
	config.EventCh = n.eventCh

	// Advertize the RPC port
	tags := map[string]string{
		externalRpcTag: strconv.Itoa(n.port),
	}

	config.Tags = tags

	// Start serf for discovery
	s, err := serf.Create(config)
	if err != nil {
		return err
	}

	n.serf = s
	return nil
}

// serfEventHandler is a dispatch method on Serf Events. Should be started in a
// goroutine.
func (n *Node) serfEventHandler() {
	for {
		select {
		case <-n.quitCh:
			return
		case e := <-n.eventCh:
			switch t := e.(type) {
			case serf.MemberEvent:
				switch t.EventType() {
				case serf.EventMemberJoin:
					go n.handleNodeJoins(t.Members)
				case serf.EventMemberLeave, serf.EventMemberFailed:
					go n.handleNodeLeaves(t.Members)
				default:
					// Don't handle any other event type.
					continue
				}
			default:
				n.logger.Printf("[ERROR] onecache: unexpected Serf event type %v\n", t)
				continue
			}
		}
	}
}

// Exit removes this Node from the ring, kills background tasks and frees any
// used resources. Once this is called the Node will no longer accept
// connections and can not be used.
func (n *Node) Exit() error {
	close(n.quitCh)
	n.replicator.Stop()

	// Clean up the datastore.
	if err := n.data.Destroy(); err != nil {
		return err
	}

	// Close the listener and RPC Server.
	n.shutdown = true
	if err := n.listener.Close(); err != nil {
		return err
	}

	if err := n.server.shutdown(); err != nil {
		return err
	}

	if err := n.serf.Leave(); err != nil {
		return err
	}

	if err := n.serf.Shutdown(); err != nil {
		return err
	}

	return nil
}

// Join adds this Node to an existing set ring. Nodes is a list of ip:port port pairs
// or DNS addresses.
func (n *Node) Join(nodes []string) error {
	_, err := n.serf.Join(nodes, true)
	return err
}

func (n *Node) handleNodeJoins(members []serf.Member) {
	n.memberStateUpdates(members, false)
}

func (n *Node) handleNodeLeaves(members []serf.Member) {
	n.memberStateUpdates(members, true)
}

// Members should be called when serf members join or leave. Remove is set to
// true if nodes left. This method updates state, including the ring to reflect
// Nodes joining/leaving.
func (n *Node) memberStateUpdates(members []serf.Member, remove bool) {
	n.lock.Lock()

	var peers []string
	for _, m := range members {
		p, err := buildPeerNode(m)
		if err != nil {
			n.logger.Printf("[ERROR] onecache: buildPeerNode(%v) failed: %v\n", m, err)
			continue
		}
		peers = append(peers, p.name)

		if err := n.memberStateUpdate(p, remove); err != nil {
			n.logger.Printf("[ERROR] onecache: memberStateUpdate(%v, %v) failed: %v\n", m, remove, err)
		}
	}
	n.lock.Unlock()

	// Want to mark after all state updates have been applied because the state of replicas can be in flux.
	if remove {
		n.replicator.NodesRemoved(peers)
	} else {
		if err := n.replicator.MarkPeersDirty(peers); err != nil {
			n.logger.Printf("[ERROR] onecache.replicator: %v", err)
		}
	}

	// The set of Nodes has changed so trigger garbage collection
	n.gc.Reset(10 * time.Minute)
}

// memberStateUpdate handles a single node joining or leaving.
func (n *Node) memberStateUpdate(p *peerNode, remove bool) error {
	if remove {
		if _, exists := n.nodes[p.name]; !exists {
			return fmt.Errorf("delete of non-existent peer %+v", p)
		}

		n.logger.Printf("[INFO] onecache: removed peer node %v", p)
		delete(n.nodes, p.name)
		n.ring.remove(p.name)
		return nil
	}

	n.logger.Printf("[INFO] onecache: added peer node %v", p)
	n.nodes[p.name] = p
	n.ring.add(p.name)
	return nil
}

// buildPeerNode takes a serf member and returns a peerNode
func buildPeerNode(m serf.Member) (*peerNode, error) {
	name := m.Name
	portString, ok := m.Tags[externalRpcTag]
	if !ok {
		return nil, fmt.Errorf("member joined without the rpc port set: %+v", m)
	}

	port, err := strconv.ParseInt(portString, 0, 0)
	if err != nil {
		return nil, fmt.Errorf("couldn't parse rpc port string %v for member %+v", portString, m)
	}

	return &peerNode{
		name: name,
		addr: m.Addr.String(),
		port: int(port),
	}, nil
}

// garbageCollect scans for stored keys that this node doesn't own or should not
// be replicated to it and removes them.
func (n *Node) garbageCollect() {
	keys, replicating := n.getGCKeys()
	for _, key := range keys {
		n.data.Delete(key)
	}

	// Queue another round of GC since we were replicating unowned keys.
	if replicating {
		n.gc.Reset(10 * time.Minute)
	}
}

// getGCKeys returns keys that should be GC'd and whether there should be a
// follow up GC because keys are being replicated.
func (n *Node) getGCKeys() ([]string, bool) {
	keys := n.data.List()
	var gc []string
	var replicating bool
	for _, key := range keys {
		replicas := n.ring.replicaPeers(key)
		for _, replica := range replicas {
			if replica == n.name {
				if n.replicator.IsReplicating(key) {
					replicating = true
					continue
				}
				gc = append(gc, key)
				break
			}
		}
	}

	return gc, replicating
}
