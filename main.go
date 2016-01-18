package main

import (
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"time"

	"github.com/dadgar/onecache/onecache"

	_ "net/http/pprof"
)

var (
	defaultConfig  = onecache.DefaultNodeConfig()
	join           = flag.String("join", "", "A comma seperated list of peers (ip:port/dns) to join")
	retry_join     = flag.String("retry_join", "", "A comma seperated list of peers (ip:port/dns) to join with retries enabled")
	retry_interval = flag.Duration("retry_interval", 30*time.Second, "The interval to try retrying a join if there was an error")
	port           = flag.Int("port", defaultConfig.RpcPort, "Port to bind to for memcache connections")
	gossipPort     = flag.Int("gossip_port", defaultConfig.SerfConfig.MemberlistConfig.BindPort, "Port to use for membership gossiping")
	addr           = flag.String("addr", "", "Address to bind to")
	replicas       = flag.Int("replicas", defaultConfig.Replicas, "Number of replicas")
	maxMemory      = flag.Int64("max_memory", defaultConfig.MaxMemoryUsage, "Maximum memory to use (in bytes)")
	pprofEnabled   = flag.Bool("pprof", false, "Enable pprof for debugging")
	pprofPort      = flag.Int("pprof_port", 6060, "Pprof http port")
)

func main() {
	flag.Parse()
	config := buildConfig()
	logger := log.New(config.LogOutput, "", log.LstdFlags)
	node, err := onecache.Create(config)
	if err != nil {
		logger.Printf("[ERROR] onecache: could not create node: %v\n", err)
		os.Exit(1)
	}

	// Handle interupts.
	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt)

	node.Start()

	if *join != "" {
		peerNodes := strings.Split(*join, ",")
		if err := node.Join(peerNodes); err != nil {
			logger.Printf("[ERROR] onecache: could not join peers %v: %v", peerNodes, err)
			node.Exit()
			return
		}
	}

	if *retry_join != "" {
		var retry *time.Timer
		retryNodes := strings.Split(*retry_join, ",")
		retry = time.AfterFunc(0, func() {
			if err := node.Join(retryNodes); err != nil {
				logger.Printf("[DEBUG] onecache: could not join peers %v: %v\n", retryNodes, err)
				logger.Printf("[DEBUG] onecache: retrying in %v", *retry_interval)
				retry.Reset(*retry_interval)
			} else {
				retry.Stop()
			}
		})
	}

	if *pprofEnabled {
		go func() {
			logger.Println(http.ListenAndServe(fmt.Sprintf(":%d", *pprofPort), nil))
		}()
	}

	// Exit the node upon an interupt.
	for range c {
		node.Exit()
		return
	}
}

func buildConfig() *onecache.NodeConfig {
	config := onecache.DefaultNodeConfig()
	config.LogOutput = os.Stderr

	if *port != 0 {
		config.RpcPort = *port
	}

	if *gossipPort != 0 {
		config.SerfConfig.MemberlistConfig.BindPort = *gossipPort
	}

	if *addr != "" {
		config.SerfConfig.MemberlistConfig.BindAddr = *addr
	}

	if *replicas != 0 {
		config.Replicas = *replicas
	}

	if *maxMemory != -1 {
		config.MaxMemoryUsage = *maxMemory
	}

	return config
}
