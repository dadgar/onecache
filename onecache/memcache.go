package onecache

import (
	"bufio"
	"fmt"
	"io"
	"net"
	"strings"
)

var handlers map[string]commandHandler

// getHandlers returns the memcache command handlers.
func getHandlers(n *Node) map[string]commandHandler {
	if len(handlers) == 0 {
		handlers = map[string]commandHandler{
			"add":     &addHandler{n: n},
			"append":  &appendHandler{n: n, back: false},
			"cas":     &casHandler{n: n},
			"decr":    &incrementHandler{n: n, negative: true},
			"delete":  &deleteHandler{n: n},
			"get":     &getHandler{n: n, cas: false},
			"gets":    &getHandler{n: n, cas: true},
			"incr":    &incrementHandler{n: n, negative: false},
			"prepend": &appendHandler{n: n, back: true},
			"replace": &replaceHandler{n: n},
			"set":     &setHandler{n: n},
			"touch":   &touchHandler{n: n},
		}
	}

	return handlers
}

// wrappedByteReader is a reader that first returns the wrapped byte and
// subsequently reads from the reader.
type wrappedByteReader struct {
	first byte
	r     io.Reader
	sent  bool
}

// newWrappedByteReader returns a reader that first returns the passed byte and
// then reads from the reader.
func newWrappedByteReader(b byte, r io.Reader) *wrappedByteReader {
	return &wrappedByteReader{
		first: b,
		r:     r,
		sent:  false,
	}
}

// Read first reads the wrapped byte and then reads from the reader.
func (w *wrappedByteReader) Read(p []byte) (n int, err error) {
	if len(p) == 0 {
		return 0, fmt.Errorf("passed empty byte slice")
	}

	if !w.sent {
		w.sent = true
		p[0] = w.first
		n, err := w.r.Read(p[1:])
		n++
		return n, err
	}

	return w.r.Read(p)
}

// handleMemcacheClient parses and responds to memcache queries. It is passed
// the first byte of the stream.
func (n *Node) handleMemcacheClient(conn net.Conn, first byte) {
	// Setup the handlers.
	handlers := getHandlers(n)

	// Create a buffered connection
	r := bufio.NewReader(newWrappedByteReader(first, conn))
	w := bufio.NewWriter(conn)
	buf := bufio.NewReadWriter(r, w)

	for {
		s, err := buf.ReadString('\n')
		if err != nil {
			if err := sendClientError(buf, "invalid input: couldn't read till end of line"); err != nil {
				buf.Flush()
				conn.Close()
				return
			}

			buf.Flush()
			continue
		}

		s = strings.TrimSuffix(s, "\r\n")
		input := strings.Split(s, " ")
		if len(input) == 1 && input[0] == "" {
			if err := sendClientError(buf, "invalid input: sent no commands"); err != nil {
				buf.Flush()
				conn.Close()
				return
			}

			buf.Flush()
			continue
		}

		cmd := input[0]
		handler, ok := handlers[cmd]
		if !ok {
			if err := sendError(buf, fmt.Sprintf("unknown command %v", cmd)); err != nil {
				buf.Flush()
				conn.Close()
				return
			}

			buf.Flush()
			continue
		}

		if err := handler.parse(input[1:]); err != nil {
			if err := sendClientError(buf, err.Error()); err != nil {
				buf.Flush()
				conn.Close()
				return
			}

			buf.Flush()
			continue
		}

		b := handler.readBytes()
		var data []byte
		if b > 0 {
			data = make([]byte, b)
			if _, err := buf.Read(data); err != nil {
				if err := sendClientError(buf, err.Error()); err != nil {
					buf.Flush()
					conn.Close()
					return
				}
				buf.Flush()
				continue
			}
		}

		if err := handler.run(buf, data); err != nil {
			if err := sendServerError(buf, err.Error()); err != nil {
				buf.Flush()
				conn.Close()
				return
			}
			buf.Flush()
			conn.Close()
			return
		}

		buf.Flush()

		// Strip the remaining terminal.
		if b > 0 {
			if _, err := buf.Discard(2); err != nil {
				if err := sendServerError(buf, err.Error()); err != nil {
					buf.Flush()
					conn.Close()
					return
				}
				buf.Flush()
				conn.Close()
				return
			}
		}
	}
}

// Helpers to send error messages back to the client.
func sendError(w io.Writer, msg string) error {
	return sendGenericError(w, "ERROR", msg)
}

func sendClientError(w io.Writer, msg string) error {
	return sendGenericError(w, "CLIENT_ERROR", msg)
}

func sendServerError(w io.Writer, msg string) error {
	return sendGenericError(w, "SERVER_ERROR", msg)
}

func sendGenericError(w io.Writer, prefix, msg string) error {
	errMsg := fmt.Sprintf("%v %v\r\n", prefix, msg)
	if _, err := io.WriteString(w, errMsg); err != nil {
		return err
	}
	return nil
}

// sendMsg writes the message, appending the correct delimiter.
func sendMsg(w io.Writer, msg string) error {
	delimited := fmt.Sprintf("%v\r\n", msg)
	if _, err := io.WriteString(w, delimited); err != nil {
		return err
	}
	return nil
}
