package onecache

import (
	"fmt"
	"io"
	"net"
	"strconv"
	"time"
)

const (
	// Prepend internal RPC calls with this to distinguish from incoming memcache protocol rpc's
	rpcInternal byte = 0xFF

	// timeout for dialing another node.
	timeout = 10 * time.Second
)

// initRpcServer intializes the listener for rpc requests on the passed port.
func (n *Node) initRpcServer(port int) error {
	l, err := net.Listen("tcp", ":"+strconv.Itoa(port))
	if err != nil {
		return fmt.Errorf("couldn't listen on port %d: %v", port, err)
	}

	n.port = l.Addr().(*net.TCPAddr).Port
	n.listener = l
	n.shutdown = false
	return nil
}

func (n *Node) listen() {
	for {
		conn, err := n.listener.Accept()
		if err != nil {
			select {
			case <-n.quitCh:
				return
			default:
				break
			}

			n.logger.Printf("[WARN] onecache: rpc accept failed: %v\n", err.Error())
			continue
		}

		conn.(*net.TCPConn).SetNoDelay(true)
		conn.(*net.TCPConn).SetKeepAlive(true)
		go n.dispatchConn(conn)
	}
}

// dispatchConn redirects the connection to either the memcache layer or to the
// internal RPC layer.
func (n *Node) dispatchConn(conn net.Conn) {
	buf := make([]byte, 1)
	if _, err := conn.Read(buf); err != nil {
		if err != io.EOF {
			n.logger.Printf("[ERR] onecache: failed to read first byte: %v", err)
		}
		conn.Close()
		return
	}

	switch buf[0] {
	case rpcInternal:
		n.server.handleMultiplex(conn)
	default:
		n.handleMemcacheClient(conn, buf[0])
	}

	return
}

// dialNode takes a peer and dails it, returning the connection for RPC use.
func (n *Node) dialNode(peer string) (net.Conn, error) {
	n.lock.Lock()
	p, ok := n.nodes[peer]
	n.lock.Unlock()
	if !ok {
		return nil, fmt.Errorf("could not dial unknown peer %v", peer)
	}

	conn, err := net.DialTimeout("tcp", fmt.Sprintf("%s:%d", p.addr, p.port), timeout)
	if err != nil {
		return nil, err
	}

	_, err = conn.Write([]byte{rpcInternal})
	if err != nil {
		conn.Close()
		return nil, err
	}

	return conn, nil
}
