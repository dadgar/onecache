package onecache

import (
	"fmt"
	"time"
)

// Returns a test configuration suitable for unit tests. If port parameters are set to zero
// an open port is chosen.
func buildTestConfig(replicas, rpcPort, discoveryPort int) *NodeConfig {
	config := DefaultNodeConfig()
	config.SerfConfig.MemberlistConfig.BindAddr = "127.0.0.1"
	config.Replicas = replicas
	config.RpcPort = rpcPort
	config.SerfConfig.MemberlistConfig.BindPort = discoveryPort
	return config
}

// Returns a single test node with replica parameter of 2.
func defaultTestNode() (*Node, error) {
	n, err := Create(buildTestConfig(2, 0, 0))
	if err != nil {
		return nil, err
	}

	// Allow the node to setup.
	n.Start()
	time.Sleep(10 * time.Millisecond)

	return n, nil
}

// Returns a list of n Node's that have joined each other.
func connectedTestNodes(n int, replicas int) ([]*Node, error) {
	configs := make([]*NodeConfig, n)
	for i := 0; i < n; i++ {
		configs[i] = buildTestConfig(replicas, 0, 0)
	}
	return buildConnectedNodes(configs)
}

// Returns a list of Node's created using the passed configuration and joined with each other.
func buildConnectedNodes(configs []*NodeConfig) ([]*Node, error) {
	nodes := make([]*Node, len(configs))
	for i, c := range configs {
		n, err := Create(c)
		if err != nil {
			return nil, err
		}

		n.Start()
		nodes[i] = n
	}

	// Allow the nodes to setup.
	time.Sleep(10 * time.Millisecond)

	if len(nodes) == 1 {
		return nodes, nil
	}

	// Create a slice containing the ports.
	ip := make([]string, len(nodes))
	for i, n := range nodes {
		ip[i] = fmt.Sprintf("127.0.0.1:%d", n.serf.LocalMember().Port)
	}

	// Allow the nodes to join.
	for _, node := range nodes {
		node.Join(ip)
	}

	waitForResult(func() (bool, error) {
		for _, node := range nodes {
			if len(node.nodes) != len(nodes) {
				return false, nil
			}
		}
		return true, nil
	}, func(e error) {})

	return nodes, nil
}

func tearDownNodes(nodes []*Node) {
	for _, n := range nodes {
		// Speeds up the serf leave.
		n.serf.Shutdown()
		n.Exit()
	}
}

type testFn func() (bool, error)
type errorFn func(error)

func waitForResult(test testFn, error errorFn) {
	retries := 1000

	for retries > 0 {
		time.Sleep(10 * time.Millisecond)
		retries--

		success, err := test()
		if success {
			return
		}

		if retries == 0 {
			error(err)
		}
	}
}
