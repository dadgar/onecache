package onecache

import (
	"bufio"
	"fmt"
	"math/rand"
	"net"
	"testing"
)

func BenchmarkMemcacheSetGet(b *testing.B) {
	n, err := defaultTestNode()
	if err != nil {
		b.Fatalf("defaultTestNode() failed: %v", err)
	}
	defer tearDownNodes([]*Node{n})

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		conn, err := net.Dial("tcp", n.listener.Addr().String())
		if err != nil {
			b.Fatalf("couldn't make connection to node: %v", err)
		}
		defer conn.Close()
		buf := bufio.NewReader(conn)

		for pb.Next() {
			r := rand.Int63()
			kv := fmt.Sprintf("%d", r%10000)

			if r%2 == 0 {
				fmt.Fprintf(conn, "GET %v\r\n", kv)
				if _, _, err := buf.ReadLine(); err != nil {
					b.Errorf("couldn't read response: %v", err)
				}
			} else {
				f := r % 10000
				ttl := r % 100
				s := len(kv)
				fmt.Fprintf(conn, "SET %v %d %d %d\r\n%v\r\n", kv, f, ttl, s, kv)
				if _, _, err := buf.ReadLine(); err != nil {
					b.Errorf("couldn't read response: %v", err)
				}
			}
		}
	})
}

func BenchmarkMemcacheSetGetReplicated(b *testing.B) {
	nodes, err := connectedTestNodes(2, 2)
	defer tearDownNodes(nodes)
	if err != nil {
		b.Fatalf("connectedTestNodes(2, 2) failed: %v", err)
	}

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		conn1, err := net.Dial("tcp", nodes[0].listener.Addr().String())
		if err != nil {
			b.Fatalf("couldn't make connection to node: %v", err)
		}
		defer conn1.Close()
		buf1 := bufio.NewReader(conn1)

		conn2, err := net.Dial("tcp", nodes[1].listener.Addr().String())
		if err != nil {
			b.Fatalf("couldn't make connection to node: %v", err)
		}
		defer conn2.Close()
		buf2 := bufio.NewReader(conn2)

		var conn net.Conn
		var buf *bufio.Reader
		for pb.Next() {
			r := rand.Int63()
			r2 := rand.Int()
			kv := fmt.Sprintf("%d", r%10000)

			if r2%2 == 0 {
				conn = conn1
				buf = buf1
			} else {
				conn = conn2
				buf = buf2
			}

			if r%2 == 0 {
				fmt.Fprintf(conn, "GET %v\r\n", kv)
				if _, _, err := buf.ReadLine(); err != nil {
					b.Errorf("couldn't read response: %v", err)
				}
			} else {
				f := r % 10000
				ttl := r % 100
				s := len(kv)
				fmt.Fprintf(conn, "SET %v %d %d %d\r\n%v\r\n", kv, f, ttl, s, kv)
				if _, _, err := buf.ReadLine(); err != nil {
					b.Errorf("couldn't read response: %v", err)
				}
			}
		}
	})
}
