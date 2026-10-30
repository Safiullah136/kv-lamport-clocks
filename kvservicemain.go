// A simple key-value store that supports four API calls over RPC:
// val <- get(key,clientid,clock): execute k-v get from clientid with lamport clock value
// - put(key,val,clientid,clock) : execute k-v put from clientid with lamport clock value
// - clockupdate(clientid,clock) : report latest lamport clock value at clientid
// - disconnect(clientid)        : report disconnection/failure of clientid
//
// Usage: go run kvservicemain.go [ip:port] [num-clients]
//
// - [ip:port] : the ip and TCP port on which the service will listen
//               for connections
//
// - [num-clients] : Number of unique clients to expect (unique clientid's)

package main

import (
	"fmt"
	"log"
	"net"
	"net/rpc"
	"os"
	"strconv"
	"sync"
)

// args in get(args)
type GetArgs struct {
	Key      string // key to look up
	Clientid uint8  // client id issuing this get
	Clock    uint64 // value of lamport clock at the issuing client
}

// args in put(args)
type PutArgs struct {
	Key      string // key to associate value with
	Val      string // value
	Clientid uint8  // client id issueing this put
	Clock    uint64 // value of lamport clock at the issuing client
}

// args in clockupdate(args)
type ClockUpdateArgs struct {
	Clientid uint8  // client id issueing this put
	Clock    uint64 // value of lamport clock at the issuing client
}

// args in disconnect(args)
type DisconnectArgs struct {
	Clientid uint8 // client id issueing this put
}

// Reply from service for all the API calls above.
type ValReply struct {
	Val string // value; depends on the call
}

// Value in the key-val store.
type MapVal struct {
	value string // the underlying value representation
}

type KeyValService struct {
	service     int
	active      map[uint8]bool
	clockValues map[uint8]uint64
	kvmap       map[string]*MapVal
	numClients  uint8

	mu   sync.Mutex
	cond *sync.Cond
}

func (kvs *KeyValService) waitForTurn(clientID uint8, myClock uint64) {
	for {
		stable := true
		for id := uint8(0); id < kvs.numClients; id++ {
			if id == clientID || !kvs.active[id] {
				continue
			}
			other := kvs.clockValues[id]
			if other < myClock || (other == myClock && id < clientID) {
				stable = false
				break
			}
		}
		if stable {
			return
		}
		kvs.cond.Wait()
	}
}

// Caller must hold kvs.mu.
func (kvs *KeyValService) observe(id uint8, clock uint64) {
	if clock > kvs.clockValues[id] {
		kvs.clockValues[id] = clock
		kvs.cond.Broadcast()
	}
}

func (kvs *KeyValService) Get(args *GetArgs, reply *ValReply) error {
	kvs.mu.Lock()
	defer kvs.mu.Unlock()

	kvs.observe(args.Clientid, args.Clock)

	fmt.Printf("\n[Get] Waiting For my Turn: ", args.Clientid, args.Clock)

	kvs.waitForTurn(args.Clientid, args.Clock)

	if val, ok := kvs.kvmap[args.Key]; ok {
		reply.Val = val.value
	} else {
		reply.Val = ""
	}
	return nil
}

func (kvs *KeyValService) Put(args *PutArgs, reply *ValReply) error {
	kvs.mu.Lock()
	defer kvs.mu.Unlock()

	kvs.observe(args.Clientid, args.Clock)

	fmt.Printf("\n[Put] Waiting For my Turn: ", args.Clientid, args.Clock)

	kvs.waitForTurn(args.Clientid, args.Clock)

	if kvs.kvmap[args.Key] == nil {
		kvs.kvmap[args.Key] = &MapVal{}
	}
	kvs.kvmap[args.Key].value = args.Val
	return nil
}

func (kvs *KeyValService) ClockUpdate(args *ClockUpdateArgs, reply *ValReply) error {
	kvs.mu.Lock()
	defer kvs.mu.Unlock()

	kvs.observe(args.Clientid, args.Clock)
	return nil
}

func (kvs *KeyValService) Disconnect(args *DisconnectArgs, reply *ValReply) error {
	kvs.mu.Lock()
	defer kvs.mu.Unlock()

	kvs.active[args.Clientid] = false

	kvs.cond.Broadcast()

	return nil
}

// Main server loop.
func main() {
	// Parse args.
	usage := fmt.Sprintf("Usage: %s [ip:port] [num-clients]\n", os.Args[0])
	if len(os.Args) != 3 {
		fmt.Printf(usage)
		os.Exit(1)
	}

	ip_port := os.Args[1]
	arg, err := strconv.ParseUint(os.Args[2], 10, 8)
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
	if arg == 0 {
		fmt.Printf(usage)
		fmt.Printf("\tnum-clients arg must be non-zero\n")
		os.Exit(1)
	}

	// Setup key-value store and register service.
	kvservice := new(KeyValService)
	kvservice.clockValues = make(map[uint8]uint64)
	kvservice.active = make(map[uint8]bool)
	kvservice.kvmap = make(map[string]*MapVal)
	kvservice.numClients = uint8(arg)

	for i := uint8(0); i < kvservice.numClients; i++ {
		kvservice.active[i] = true
	}

	kvservice.cond = sync.NewCond(&kvservice.mu)

	rpc.Register(kvservice)
	l, e := net.Listen("tcp", ip_port)
	if e != nil {
		log.Fatal("listen error:", e)
	}

	for {
		conn, _ := l.Accept()
		go rpc.ServeConn(conn)
	}
}
