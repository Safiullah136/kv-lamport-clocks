// Simple client to connect to the key-value service and exercise the
// key-value RPC API.
//
// Usage: go run kvclientmain.go [clientid] [clock-update-rate] [ip1:port1] ... [ipN:portN]
//
// - [clientid] : the clientid uint8 to use for this client
//
// - [clock-update-rate] : the rate at which the local lamport clock
//                         is reported to the server through a
//                         clockupdate() call. Number of milliseconds
//                         between clockupdate calls.
//
// - [ip1:port1] : the ip and TCP port of the 1st replica listening for clients
// - ...
// - [ipN:portN] : the ip and TCP port of the Nth replica listening for clients

package main

import (
	"errors"
	"fmt"
	"net"
	"net/rpc"
	"os"
	"strconv"
	"sync"
	"time"
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

type KVClient struct {
	clientId uint8
	clock    uint64

	replicas []*rpc.Client
	active   []bool
	done     chan struct{}

	putInFlight bool

	mu   sync.Mutex // protects clock, active, putInFlight (never held across an RPC)
	opMu sync.Mutex // serializes logical operations: Get, Put, Disconnect
}

func (c *KVClient) tick() uint64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.clock++
	return c.clock
}

func (c *KVClient) isActive(i int) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.active[i]
}

func (c *KVClient) setInactive(i int) {
	c.mu.Lock()
	c.active[i] = false
	c.mu.Unlock()
}

func (c *KVClient) ObserveTimestamp(ts uint64) {
	c.mu.Lock()
	if ts > c.clock {
		c.clock = ts
	}
	c.mu.Unlock()
}

func (c *KVClient) Get(key string) (string, uint64, error) {
	c.opMu.Lock()
	defer c.opMu.Unlock()

	for i, replica := range c.replicas {
		if !c.isActive(i) {
			continue
		}

		clock := c.tick() // increment before every RPC, including retries
		args := GetArgs{Key: key, Clientid: c.clientId, Clock: clock}
		var reply ValReply

		if err := replica.Call("KeyValService.Get", &args, &reply); err != nil {
			c.setInactive(i)
			continue
		}
		return reply.Val, clock, nil
	}

	c.mu.Lock()
	clock := c.clock
	c.mu.Unlock()
	return "", clock, errors.New("all replicas have failed")
}

func (c *KVClient) Put(key, value string) (uint64, error) {
	c.opMu.Lock()
	defer c.opMu.Unlock()

	c.mu.Lock()
	c.clock++
	clock := c.clock
	c.putInFlight = true // suppress ClockUpdate from here on
	var targets []int
	for i, a := range c.active {
		if a {
			targets = append(targets, i)
		}
	}
	c.mu.Unlock()

	defer func() {
		c.mu.Lock()
		c.putInFlight = false
		c.mu.Unlock()
	}()

	args := PutArgs{Key: key, Val: value, Clientid: c.clientId, Clock: clock}
	replies := make([]ValReply, len(targets))
	calls := make([]*rpc.Call, len(targets))

	// Go() writes the request to the connection before returning, so every
	// Put is on the wire before any later ClockUpdate can be sent.
	for j, i := range targets {
		calls[j] = c.replicas[i].Go("KeyValService.Put", &args, &replies[j],
			make(chan *rpc.Call, 1))
	}

	successful := 0
	for j, i := range targets {
		<-calls[j].Done
		if calls[j].Error != nil {
			c.setInactive(i)
		} else {
			successful++
		}
	}

	if successful == 0 {
		return clock, errors.New("all replicas have failed")
	}
	return clock, nil
}

func (c *KVClient) ClockUpdate() error {
	c.mu.Lock()
	if c.putInFlight {
		c.mu.Unlock()
		return nil
	}
	c.clock++
	clock := c.clock
	var targets []int
	for i, a := range c.active {
		if a {
			targets = append(targets, i)
		}
	}
	c.mu.Unlock()

	args := ClockUpdateArgs{Clientid: c.clientId, Clock: clock}
	for _, i := range targets {
		var reply ValReply
		if err := c.replicas[i].Call("KeyValService.ClockUpdate", &args, &reply); err != nil {
			c.setInactive(i)
		}
	}
	return nil
}

func (c *KVClient) callClockUpdate(rate time.Duration) {
	ticker := time.NewTicker(rate)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			c.ClockUpdate()
		case <-c.done:
			return
		}
	}
}

func (c *KVClient) Disconnect() {
	c.opMu.Lock()
	defer c.opMu.Unlock()

	close(c.done)

	args := DisconnectArgs{Clientid: c.clientId}
	for i, replica := range c.replicas {
		if c.isActive(i) {
			var reply ValReply
			replica.Call("KeyValService.Disconnect", &args, &reply)
		}
		replica.Close()
	}
}

func main() {
	// parse args
	usage := fmt.Sprintf("Usage: %s [clientid] [clock-update-rate] [ip1:port1] ... [ipN:portN]\n", os.Args[0])
	if len(os.Args) < 4 {
		fmt.Printf(usage)
		os.Exit(1)
	}

	clientId, err := strconv.ParseUint(os.Args[1], 10, 8)
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}

	clockRate, err := strconv.ParseUint(os.Args[2], 10, 8)
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}

	// Parse replica addresses
	var replicas []string

	for _, addr := range os.Args[3:] {
		host, port, err := net.SplitHostPort(addr)
		if err != nil {
			fmt.Printf("Invalid replica address %q: %v\n", addr, err)
			os.Exit(1)
		}

		// Validate port
		portNum, err := strconv.Atoi(port)
		if err != nil || portNum < 1 || portNum > 65535 {
			fmt.Printf("Invalid port in address %q\n", addr)
			os.Exit(1)
		}

		replicas = append(replicas, net.JoinHostPort(host, port))
	}

	client := &KVClient{
		clientId: uint8(clientId),
		replicas: make([]*rpc.Client, len(replicas)),
		active:   make([]bool, len(replicas)),
		done:     make(chan struct{}),
	}

	for i, addr := range replicas {
		conn, err := rpc.Dial("tcp", addr)
		if err != nil {
			fmt.Println("Replica unavailable:", addr, err)
			continue
		}

		client.replicas[i] = conn
		client.active[i] = true
	}

	go client.callClockUpdate(time.Duration(clockRate) * time.Millisecond)

	fmt.Printf("\tBefore Put-Get\n")

	clockPut, err := client.Put("client"+strconv.Itoa(int(clientId)), "Cake")
	val, clockGet, err := client.Get("client" + strconv.Itoa(int(clientId+1)))
	fmt.Println("Get Value: " + val)
	fmt.Println("Clock Values: ", clockPut, clockGet)

	// Don't forget to call Disconnect before quitting.
	client.Disconnect()

	activeReplicas := 0
	for _, replica := range client.active {
		if replica {
			activeReplicas += 1
		}
	}

	fmt.Println("Active Replicas: ", activeReplicas)
	fmt.Println("\nMission accomplished.")
}

// If error is non-nil, print it out and halt.
func checkError(err error) {
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error ", err.Error())
		os.Exit(1)
	}
}
