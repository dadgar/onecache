package onecache

import (
	"fmt"
	"io"
	"strconv"
	"time"

	"github.com/dadgar/onecache/ttlstore"
)

const (
	noreply = "noreply"

	expectedSetInput    = "<key> <flags> <exptime> <bytes> [noreply]\r\n"
	expectedCasInput    = "<key> <flags> <exptime> <bytes> <cas unique> [noreply]\r\n"
	expectedIncrInput   = "<key> <value> [noreply]\r\n"
	expectedGetInput    = "<key>*\r\n"
	expectedDeleteInput = "<key> [noreply]\r\n"
	expectedTouchInput  = "<key> <exptime> [noreply]\r\n"

	// The maximum value in which expirations are considered as seconds from now.
	secondMax = 60 * 60 * 24 * 30
)

// commandHandler is the interface to handle a memcache command.
type commandHandler interface {
	// Returns an error if the input is not properly formatted.
	parse(flags []string) error

	// Returns how many bytes need to be read off the connection before calling Run(). -1 indicates that no additional
	// reading is necessary.
	readBytes() int64

	// Run the command returning the results to w. Must be called after Parse. Required input will be passed through data.
	// If an unrecoverable error occurs, error will not be nil and the connection to the client will be closed.
	run(w io.Writer, data []byte) error
}

func convertMemcacheExpToUnix(memExp int32) int64 {
	exp := int64(memExp)
	if memExp == 0 {
		return 0
	} else if exp < secondMax {
		return time.Now().Add(time.Duration(exp) * time.Second).Unix()
	}

	return exp
}

type setHandler struct {
	n       *Node
	args    StorageArgs
	size    int64
	noReply bool
}

func (h *setHandler) parse(flags []string) error {
	l := len(flags)
	if l != 4 && l != 5 {
		return fmt.Errorf("received an unexpected number of flags (%d); expect %v", l, expectedSetInput)
	}

	h.args.Key = flags[0]

	flag, err := strconv.ParseInt(flags[1], 10, 32)
	if err != nil {
		return fmt.Errorf("could not parse flag field: %v", err)
	}
	h.args.Flags = int32(flag)

	exp, err := strconv.ParseInt(flags[2], 10, 32)
	if err != nil {
		return fmt.Errorf("could not parse expiration field: %v", err)
		return err
	}
	h.args.Exp = convertMemcacheExpToUnix(int32(exp))

	bytes, err := strconv.ParseInt(flags[3], 10, 64)
	if err != nil {
		return fmt.Errorf("could not parse bytes field: %v", err)
	}
	h.size = bytes

	if l == 5 {
		if flags[4] != noreply {
			return fmt.Errorf("was expecting %v; got %v", noreply, flags[4])
		}

		h.noReply = true
	}

	return nil
}

func (h *setHandler) readBytes() int64 {
	return h.size
}

func (h *setHandler) run(w io.Writer, data []byte) error {
	var resp struct{}
	h.args.Value = data
	err := h.n.server.Set(h.args, &resp)
	if h.noReply {
		return nil
	}

	if err != nil {
		return sendMsg(w, "NOT_STORED")
	}

	return sendMsg(w, "STORED")
}

type casHandler struct {
	n       *Node
	args    StorageArgs
	size    int64
	noReply bool
}

func (h *casHandler) parse(flags []string) error {
	l := len(flags)
	if l != 5 && l != 6 {
		return fmt.Errorf("received an unexpected number of flags (%d); expect %v", l, expectedCasInput)
	}

	h.args.Key = flags[0]

	flag, err := strconv.ParseInt(flags[1], 10, 32)
	if err != nil {
		return fmt.Errorf("could not parse flag field: %v", err)
	}
	h.args.Flags = int32(flag)

	exp, err := strconv.ParseInt(flags[2], 10, 32)
	if err != nil {
		return fmt.Errorf("could not parse expiration field: %v", err)
		return err
	}
	h.args.Exp = convertMemcacheExpToUnix(int32(exp))

	bytes, err := strconv.ParseInt(flags[3], 10, 64)
	if err != nil {
		return fmt.Errorf("could not parse bytes field: %v", err)
	}
	h.size = bytes

	cas, err := strconv.ParseInt(flags[4], 10, 64)
	if err != nil {
		return fmt.Errorf("could not parse cas field: %v", err)
	}
	h.args.Cas = cas

	if l == 6 {
		if flags[5] != noreply {
			return fmt.Errorf("was expecting %v; got %v", noreply, flags[5])
		}

		h.noReply = true
	}

	return nil
}

func (h *casHandler) readBytes() int64 {
	return h.size
}

func (h *casHandler) run(w io.Writer, data []byte) error {
	h.args.Value = data
	var resp struct{}
	err := h.n.server.Cas(h.args, &resp)
	if h.noReply {
		return nil
	}

	if err != nil {
		return sendMsg(w, "NOT_STORED")
	}

	return sendMsg(w, "STORED")
}

type addHandler struct {
	n       *Node
	args    StorageArgs
	size    int64
	noReply bool
}

func (h *addHandler) parse(flags []string) error {
	l := len(flags)
	if l != 4 && l != 5 {
		return fmt.Errorf("received an unexpected number of flags (%d); expect %v", l, expectedSetInput)
	}

	h.args.Key = flags[0]

	flag, err := strconv.ParseInt(flags[1], 10, 32)
	if err != nil {
		return fmt.Errorf("could not parse flag field: %v", err)
	}
	h.args.Flags = int32(flag)

	exp, err := strconv.ParseInt(flags[2], 10, 32)
	if err != nil {
		return fmt.Errorf("could not parse expiration field: %v", err)
		return err
	}
	h.args.Exp = convertMemcacheExpToUnix(int32(exp))

	bytes, err := strconv.ParseInt(flags[3], 10, 64)
	if err != nil {
		return fmt.Errorf("could not parse bytes field: %v", err)
	}
	h.size = bytes

	if l == 5 {
		if flags[4] != noreply {
			return fmt.Errorf("was expecting %v; got %v", noreply, flags[4])
		}

		h.noReply = true
	}

	return nil
}

func (h *addHandler) readBytes() int64 {
	return h.size
}

func (h *addHandler) run(w io.Writer, data []byte) error {
	var resp struct{}
	h.args.Value = data
	err := h.n.server.Add(h.args, &resp)
	if h.noReply {
		return nil
	}

	if err != nil {
		return sendMsg(w, "NOT_STORED")
	}

	return sendMsg(w, "STORED")
}

type replaceHandler struct {
	n       *Node
	args    StorageArgs
	size    int64
	noReply bool
}

func (h *replaceHandler) parse(flags []string) error {
	l := len(flags)
	if l != 4 && l != 5 {
		return fmt.Errorf("received an unexpected number of flags (%d); expect %v", l, expectedSetInput)
	}

	h.args.Key = flags[0]

	flag, err := strconv.ParseInt(flags[1], 10, 32)
	if err != nil {
		return fmt.Errorf("could not parse flag field: %v", err)
	}
	h.args.Flags = int32(flag)

	exp, err := strconv.ParseInt(flags[2], 10, 32)
	if err != nil {
		return fmt.Errorf("could not parse expiration field: %v", err)
		return err
	}
	h.args.Exp = convertMemcacheExpToUnix(int32(exp))

	bytes, err := strconv.ParseInt(flags[3], 10, 64)
	if err != nil {
		return fmt.Errorf("could not parse bytes field: %v", err)
	}
	h.size = bytes

	if l == 5 {
		if flags[4] != noreply {
			return fmt.Errorf("was expecting %v; got %v", noreply, flags[4])
		}

		h.noReply = true
	}

	return nil
}

func (h *replaceHandler) readBytes() int64 {
	return h.size
}

func (h *replaceHandler) run(w io.Writer, data []byte) error {
	var resp struct{}
	h.args.Value = data
	err := h.n.server.Replace(h.args, &resp)
	if h.noReply {
		return nil
	}

	if err != nil {
		return sendMsg(w, "NOT_STORED")
	}

	return sendMsg(w, "STORED")
}

type appendHandler struct {
	n       *Node
	back    bool
	args    StorageArgs
	size    int64
	noReply bool
}

func (h *appendHandler) parse(flags []string) error {
	l := len(flags)
	if l != 4 && l != 5 {
		return fmt.Errorf("received an unexpected number of flags (%d); expect %v", l, expectedSetInput)
	}

	h.args.Key = flags[0]

	bytes, err := strconv.ParseInt(flags[3], 10, 64)
	if err != nil {
		return fmt.Errorf("could not parse bytes field: %v", err)
	}
	h.size = bytes

	if l == 5 {
		if flags[4] != noreply {
			return fmt.Errorf("was expecting %v; got %v", noreply, flags[4])
		}

		h.noReply = true
	}

	return nil
}

func (h *appendHandler) readBytes() int64 {
	return h.size
}

func (h *appendHandler) run(w io.Writer, data []byte) error {
	var resp struct{}
	var err error
	h.args.Value = data
	if h.back {
		err = h.n.server.Prepend(h.args, &resp)
	} else {
		err = h.n.server.Append(h.args, &resp)
	}

	if h.noReply {
		return nil
	}

	if err != nil {
		return sendMsg(w, "NOT_STORED")
	}

	return sendMsg(w, "STORED")
}

type incrementHandler struct {
	n        *Node
	negative bool
	args     StorageArgs
	noReply  bool
}

func (h *incrementHandler) parse(flags []string) error {
	l := len(flags)
	if l != 2 && l != 3 {
		return fmt.Errorf("received an unexpected number of flags (%d); expect %v", l, expectedIncrInput)
	}

	h.args.Key = flags[0]

	delta, err := strconv.ParseInt(flags[1], 10, 64)
	if err != nil {
		return fmt.Errorf("could not parse value: %v", err)
	}
	h.args.Delta = delta

	if l == 3 {
		if flags[2] != noreply {
			return fmt.Errorf("was expecting %v; got %v", noreply, flags[2])
		}

		h.noReply = true
	}

	return nil
}

func (h *incrementHandler) readBytes() int64 {
	return 0
}

func (h *incrementHandler) run(w io.Writer, data []byte) error {
	var resp int64
	var err error
	if h.negative {
		err = h.n.server.Decrement(h.args, &resp)
	} else {
		err = h.n.server.Increment(h.args, &resp)
	}

	if h.noReply {
		return nil
	}

	if err != nil {
		return sendMsg(w, "NOT_FOUND")
	}

	return sendMsg(w, fmt.Sprintf("%d", resp))
}

type getHandler struct {
	n    *Node
	cas  bool
	keys []string
}

func (h *getHandler) parse(flags []string) error {
	l := len(flags)
	if l == 0 {
		return fmt.Errorf("received an unexpected number of flags (%d); expect %v", l, expectedGetInput)
	}

	h.keys = flags
	return nil
}

func (h *getHandler) readBytes() int64 {
	return -1
}

func (h *getHandler) run(w io.Writer, data []byte) error {
	var resp ttlstore.KeyData
	var args StorageArgs
	for _, k := range h.keys {
		args.Key = k
		err := h.n.server.Get(args, &resp)
		if err != nil {
			continue
		}

		// Write results out.
		var header string
		if h.cas {
			header = fmt.Sprintf("VALUE %v %v %v %v", k, resp.Flags, resp.Size, resp.Cas)
		} else {
			header = fmt.Sprintf("VALUE %v %v %v", k, resp.Flags, resp.Size)
		}

		if err := sendMsg(w, header); err != nil {
			return err
		}

		if _, err := w.Write(resp.Data); err != nil {
			return err
		}

		if err := sendMsg(w, ""); err != nil {
			return err
		}
	}

	return sendMsg(w, "END")
}

type deleteHandler struct {
	n       *Node
	key     string
	noReply bool
}

func (h *deleteHandler) parse(flags []string) error {
	l := len(flags)
	if l != 1 && l != 2 {
		return fmt.Errorf("received an unexpected number of flags (%d); expect %v", l, expectedDeleteInput)
	}

	h.key = flags[0]

	if l == 2 {
		if flags[1] != noreply {
			return fmt.Errorf("was expecting %v; got %v", noreply, flags[1])
		}

		h.noReply = true
	}

	return nil
}

func (h *deleteHandler) readBytes() int64 {
	return -1
}

func (h *deleteHandler) run(w io.Writer, data []byte) error {
	var args StorageArgs
	args.Key = h.key
	var resp struct{}
	err := h.n.server.Remove(args, &resp)
	if h.noReply {
		return nil
	}

	// TODO better error checking
	if err != nil {
		return sendMsg(w, "NOT_FOUND")
	}

	return sendMsg(w, "DELETED")
}

type touchHandler struct {
	n       *Node
	args    StorageArgs
	noReply bool
}

func (h *touchHandler) parse(flags []string) error {
	l := len(flags)
	if l != 2 && l != 3 {
		return fmt.Errorf("received an unexpected number of flags (%d); expect %v", l, expectedTouchInput)
	}

	h.args.Key = flags[0]

	exp, err := strconv.ParseInt(flags[1], 10, 32)
	if err != nil {
		return fmt.Errorf("could not parse expiration field: %v", err)
		return err
	}
	h.args.Exp = convertMemcacheExpToUnix(int32(exp))

	if l == 3 {
		if flags[2] != noreply {
			return fmt.Errorf("was expecting %v; got %v", noreply, flags[2])
		}

		h.noReply = true
	}

	return nil
}

func (h *touchHandler) readBytes() int64 {
	return -1
}

func (h *touchHandler) run(w io.Writer, data []byte) error {
	var resp struct{}
	err := h.n.server.Touch(h.args, &resp)
	if h.noReply {
		return nil
	}

	// TODO better error checking
	if err != nil {
		return sendMsg(w, "NOT_FOUND")
	}

	return sendMsg(w, "TOUCHED")
	return nil
}
