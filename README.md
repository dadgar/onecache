# OneCache
OneCache is a distributed key/value store that is accessible via the memcached
protocol. OneCache nodes natively support clustering and best effort
replication. Adding nodes into the cluster effectively increases the in-memory
storage capability of the cache. Replication of keys minimizes the impact of a
single node failure as requests will transparently be forwarded to the new key
leader.

OneCache's architecture is built around the concept of a one-hop distributed
hash table. Many DHT algorithms are designed for a very large number of
members, such as bitorrent DHTs, and trade lookup efficiency for per-node
storage and communication. However, OneCache is built for a much smaller
cluster size (&lt;100) and as such, every node holds the full state of the
underlying consistent hash ring. This is advantageous because every node is
capable of forwarding requests to the correct node, which is why it receives
the term one-hop.

OneCache's name then becomes a play on its one-hop DHT architecture and memcached protocol
support.

Download pre-compiled binaries by browsing the [releases.](https://github.com/dadgar/onecache/releases)

## Usage
### Single Node
To start a single OneCache node listening on memcached's default port, simply
run:
```
$> ./onecache
```

### Clustered
To start a cluster of OneCache nodes on a single machine (for ease of getting
started) we must bind the nodes to non-default ports to avoid collision. If
clustering on different machines, the default ports can be used.

Start the first node in one terminal:
```
$> ./onecache -gossip_port=7946 -port=11211
```

In another, start the second and have it join. The output will show it added a
peer node.
```
$> ./onecache -gossip_port=7947 -port=11212 -join="127.0.0.1:7946"
2016/01/18 12:04:30 [INFO] onecache: added peer node &{c3f48bd0-4106-43d4-9044-15cf552037f0 192.168.1.10 11211}
```

## Usage
Standard memcached libraries can be used, which make OneCache an easy drop in
replacement for memcached as code doesn't have to be changed. OneCache is also
accessible via the terminal:

```
$> telnet 127.0.0.1 11211
Trying 127.0.0.1...
Connected to localhost.
Escape character is '^]'.
set foo 0 0 12
hello, world
STORED
```
