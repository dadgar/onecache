package onecache

import (
	"io"

	"github.com/hashicorp/serf/serf"
)

// NodeConfig is the configuration for a DHT Node instance.
type NodeConfig struct {
	// The number of key/value replicas.
	Replicas int

	// The port to accept RPC connections on.
	RpcPort int

	// The space alloted to store key/values in bytes.
	MaxMemoryUsage int64

	// Routes log output to the writer. If nil, output will be sent to stderr.
	LogOutput io.Writer

	// Serf configuration used to fine tune Node discovery, define whether the DHT is running in a LAN or WAN and more.
	// The name does not need to be set as it will be overriden with a UUID.
	SerfConfig *serf.Config
}

// DefaultNodeConfig returns a reasonable default to use.
func DefaultNodeConfig() *NodeConfig {
	return &NodeConfig{
		Replicas:       2,
		RpcPort:        11211,
		SerfConfig:     serf.DefaultConfig(),
		MaxMemoryUsage: 1 << 30, // 1 GB
	}
}
