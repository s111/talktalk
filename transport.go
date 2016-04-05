package talktalk

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"sync"
	"time"

	"github.com/gorilla/websocket"
)

const (
	readWait  = 2000 * time.Millisecond
	writeWait = 2000 * time.Millisecond
)

// NodeID node identifier.
type NodeID int

// ErrInvalidID indicates a invalid NodeID.
type ErrInvalidID NodeID

func (id ErrInvalidID) Error() string {
	return fmt.Sprintf("transport: invalid NodeID %d", id)
}

// ErrMissingAddr indicates a missing address for this node.
type ErrMissingAddr NodeID

func (id ErrMissingAddr) Error() string {
	return fmt.Sprintf("transport: missing addr for NodeID %d", id)
}

// ClientID client identifier.
type ClientID string

// MessageOut wraps data with its type.
type MessageOut struct {
	Type int         `json:"type"`
	Data interface{} `json:"data"`

	clientID ClientID
}

// MessageIn lets us partially decode a received message.
// Data is left as is.
// Clients should set ClientID to their id.
type MessageIn struct {
	Type int
	Data json.RawMessage

	ClientID ClientID
}

// Transport errors.
var (
	ErrTransportNotStarted = errors.New("transport: Start() not called")
	ErrTransportStopped    = errors.New("transport: Stop() called")
)

// Transport is a generic interface that abstracts
// away the underlying network transfer mechanism.
type Transport interface {
	// ID should return this nodes id.
	ID() NodeID

	// Start should start listening for replica/client messages.
	// Must be run before any other method can be.
	Start()

	// Stop should stop accepting new connections.
	// Close open connections.
	// Make all goroutines return.
	// Close listeners.
	// Should block until done.
	Stop()

	// NotifyReplica should try to deliver a message to replica with id.
	NotifyReplica(NodeID, *MessageOut)
	// NotifyAllReplicas should try to deliver a message to all replicas.
	NotifyAllReplicas(*MessageOut)
	// NextReplicaMessage should return messages from replicas in FIFO order.
	// Blocks till a message is received.
	NextReplicaMessage() (*MessageIn, error)

	// NotifyClient should try to deliver a message to client with id.
	NotifyClient(ClientID, *MessageOut)
	// NotifyAllClients should try to deliver a message to all clients.
	NotifyAllClients(*MessageOut)
	// NextClientMessage should return messages from clients in FIFO order.
	// Blocks till a message is received.
	NextClientMessage() (*MessageIn, error)
}

// NewTransport return an implementation of the Transport interface.
// Using UDP for replica communication,
// and WebSocket for client communication.
// Returns an error if arguments are invalid.
func NewTransport(id NodeID, nodes []string) (Transport, error) {
	return newTransport(id, nodes)
}

// newTransport returns the actual transport struct.
// It might be useful to call this directly in tests
// as you can query the struct directly.
func newTransport(id NodeID, nodes []string) (*transport, error) {
	// Make sure id is valid.
	if id < 1 {
		return nil, ErrInvalidID(id)
	}

	addrs := make(map[NodeID]string)

	for i, addr := range nodes {
		// NodeIDs start at 1
		addrs[NodeID(i+1)] = addr
	}

	// Make sure we got this node's address.
	if _, ok := addrs[id]; !ok {
		return nil, ErrMissingAddr(id)
	}

	udpAddrs := make(map[NodeID]*net.UDPAddr)

	// Resolve all UDP addresses.
	for id, addr := range addrs {
		udpAddr, err := net.ResolveUDPAddr("udp", addr)

		if err != nil {
			return nil, err
		}

		udpAddrs[id] = udpAddr
	}

	return &transport{
		id:               id,
		addr:             addrs[id],
		udpAddrs:         udpAddrs,
		replicaIncoming:  make(chan MessageIn, 4096),
		registerClient:   make(chan *client),
		unregisterClient: make(chan *client),
		clients:          make(map[ClientID]*client),
		clientOutgoing:   make(chan *MessageOut),
		clientBroadcast:  make(chan *MessageOut),
		clientIncoming:   make(chan MessageIn, 4096),
		stop:             make(chan int),
		started:          make(chan int),
	}, nil
}

type transport struct {
	id NodeID

	addr     string
	udpAddrs map[NodeID]*net.UDPAddr

	replicaConn    net.PacketConn
	replicaDecoder *json.Decoder

	replicaIncoming chan MessageIn

	clientConn io.Closer

	registerClient   chan *client
	unregisterClient chan *client
	clients          map[ClientID]*client

	clientOutgoing  chan *MessageOut
	clientBroadcast chan *MessageOut
	clientIncoming  chan MessageIn

	// Closing this channel signals that it's time to stop.
	stop chan int

	routines sync.WaitGroup

	// Closing this channel signals that the transport has started.
	started chan int
}

func (t *transport) ID() NodeID {
	return t.id
}

func (t *transport) Start() {
	// Listen for replica messages.
	conn, err := net.ListenUDP("udp", t.udpAddrs[t.id])

	if err != nil || conn == nil {
		panic(err)
	}

	t.replicaConn = conn
	t.replicaDecoder = json.NewDecoder(conn)

	// Read incoming replica messages.
	go t.replicaRead()

	go t.startClientHandler()
	go t.startClientTransport()

	<-t.started
}

func (t *transport) replicaRead() {
	t.routines.Add(1)
	defer t.routines.Done()

	var message MessageIn

	for {
		t.replicaConn.SetReadDeadline(time.Now().Add(readWait))
		err := t.replicaDecoder.Decode(&message)

		select {
		case <-t.stop:
			return
		default:
		}

		if err != nil {
			// Ignore failed messages, they should be re-transmitted.
			continue
		}

		select {
		case t.replicaIncoming <- message:
		case <-t.stop:
			return
		}

		// Don't want to leak data from an old message.
		message.Data = nil
	}
}

func (t *transport) startClientTransport() {
	t.routines.Add(1)
	defer t.routines.Done()

	mux := http.NewServeMux()
	mux.HandleFunc("/ws", t.serveWebSocket)

	conn, err := net.Listen("tcp", t.addr)

	if err != nil || conn == nil {
		panic(err)
	}

	t.clientConn = conn
	close(t.started)

	// Blocks
	http.Serve(conn, mux)

	log.Println("transport: shutdown http server.")
}

func (t *transport) startClientHandler() {
	t.routines.Add(1)
	defer t.routines.Done()

	for {
		select {
		case client := <-t.registerClient:
			if _, ok := t.clients[client.id]; !ok {
				t.clients[client.id] = client
				client.ok <- true
			} else {
				log.Println("transport: already have a connection for client:", client.id)
				client.ok <- false
			}

		case client := <-t.unregisterClient:
			delete(t.clients, client.id)

		case message := <-t.clientOutgoing:
			if client, ok := t.clients[message.clientID]; ok {
				select {
				case client.outgoing <- message:
				default:
				}
			} else {
				log.Printf("transport: tried to send to disconnected client: %+v", message)
			}

		case message := <-t.clientBroadcast:
			for clientID := range t.clients {
				if client, ok := t.clients[clientID]; ok {
					select {
					case client.outgoing <- message:
					default:
					}
				} else {
					log.Printf("transport: tried to send to disconnected client: %+v", message)
				}
			}

		case <-t.stop:
			for {
				if len(t.clients) == 0 {
					return
				}

				client := <-t.unregisterClient
				delete(t.clients, client.id)
			}
		}
	}
}

var upgrader = websocket.Upgrader{
	CheckOrigin: func(r *http.Request) bool {
		return true
	},
}

func (t *transport) serveWebSocket(w http.ResponseWriter, r *http.Request) {
	t.routines.Add(1)
	defer t.routines.Done()

	ws, err := upgrader.Upgrade(w, r, nil)

	if err != nil {
		log.Println("transport: upgrade:", err)

		return
	}

	defer ws.Close()

	// Read one message before registering the client.
	// This is to learn the client's id.
	// After registering, deliver the message we read.

	var message MessageIn

	// Only wait 'readWait' for the first message.
	ws.SetReadDeadline(time.Now().Add(readWait))
	err = ws.ReadJSON(&message)

	if err != nil {
		return
	}

	if len(message.ClientID) == 0 {
		log.Println("transport: message from client missing ClientID:", message)

		return
	}

	client := &client{
		id:       message.ClientID,
		conn:     ws,
		ok:       make(chan bool),
		outgoing: make(chan *MessageOut, 4096),
		incoming: t.clientIncoming,
		stop:     t.stop,
		routines: &t.routines,
	}

	// Register the client so that we can respond to it later.
	select {
	case t.registerClient <- client:
	case <-t.stop:
		return
	}

	// Client was rejected.
	if !<-client.ok {
		return
	}

	// Deliver the message we just read.
	go func() {
		t.routines.Add(1)
		defer t.routines.Done()

		select {
		case t.clientIncoming <- message:
		case <-t.stop:
		}
	}()

	// Keep connection open for ever.
	ws.SetReadDeadline(time.Time{})

	go client.write()
	client.read()

	t.unregisterClient <- client
}

func (t *transport) NotifyAllReplicas(message *MessageOut) {
	for id := range t.udpAddrs {
		t.NotifyReplica(id, message)
	}
}

func (t *transport) NotifyAllClients(message *MessageOut) {
	t.clientBroadcast <- message
}

func (t *transport) NotifyReplica(nodeID NodeID, message *MessageOut) {
	select {
	case <-t.started:
	default:
		panic(ErrTransportNotStarted)
	}

	encoded, err := json.Marshal(*message)

	if err != nil {
		log.Println("transport: could not marshal message:", err)

		return
	}

	t.replicaConn.SetWriteDeadline(time.Now().Add(writeWait))
	t.replicaConn.WriteTo(encoded, t.udpAddrs[nodeID])
}

func (t *transport) NextReplicaMessage() (*MessageIn, error) {
	select {
	case <-t.started:
	default:
		panic(ErrTransportNotStarted)
	}

	select {
	case <-t.stop:
		return nil, ErrTransportStopped
	default:
	}

	select {
	case message, ok := <-t.replicaIncoming:
		if ok {
			return &message, nil
		}
	}

	return nil, ErrTransportStopped
}

func (t *transport) NotifyClient(clientID ClientID, message *MessageOut) {
	select {
	case <-t.started:
	default:
		panic(ErrTransportNotStarted)
	}

	message.clientID = clientID

	select {
	case t.clientOutgoing <- message:
	case <-t.stop:
	}
}

func (t *transport) NextClientMessage() (*MessageIn, error) {
	select {
	case <-t.started:
	default:
		panic(ErrTransportNotStarted)
	}

	select {
	case <-t.stop:
		return nil, ErrTransportStopped
	default:
	}

	select {
	case message, ok := <-t.clientIncoming:
		if ok {
			return &message, nil
		}
	}

	return nil, ErrTransportStopped
}

func (t *transport) Stop() {
	select {
	case <-t.started:
	default:
		panic(ErrTransportNotStarted)
	}

	log.Println(ErrTransportStopped)

	close(t.stop)
	close(t.replicaIncoming)
	close(t.clientIncoming)

	log.Println("transport: closing connections")

	t.clientConn.Close()
	t.replicaConn.Close()

	log.Println("transport: waiting for routines to return")

	t.routines.Wait()
}
