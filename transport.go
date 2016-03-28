package talktalk

import (
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net"
	"net/http"

	"github.com/gorilla/websocket"
)

// NodeID node identifier.
type NodeID int

func (id NodeID) String() string {
	return fmt.Sprintf("%d", id)
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

// Transport is a generic interface that abstracts
// away the underlying network transfer mechanism.
type Transport interface {
	// ID should return this nodes id.
	ID() NodeID

	// Start should start listening for replica/client messages.
	Start() error

	// NotifyReplica should try to deliver a message to replica with id.
	NotifyReplica(NodeID, *MessageOut)
	// NextReplicaMessage should return messages from replicas in FIFO order.
	// Blocks till a message is received.
	NextReplicaMessage() *MessageIn

	// NotifyClient should try to deliver a message to client with id.
	NotifyClient(ClientID, *MessageOut)
	// NextClientMessage should return messages from clients in FIFO order.
	// Blocks till a message is received.
	NextClientMessage() *MessageIn
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
		return nil, errors.New("transport: NodeID must be >= 1, was: " + id.String())
	}

	addrs := make(map[NodeID]string)

	for i, addr := range nodes {
		// NodeIDs start at 1
		addrs[NodeID(i+1)] = addr
	}

	// Make sure we got this node's address.
	if _, ok := addrs[id]; !ok {
		return nil, errors.New("transport: Address missing for this node: " + id.String())
	}

	return &transport{
		id:               id,
		addr:             addrs[id],
		addrs:            addrs,
		udpAddrs:         make(map[NodeID]*net.UDPAddr),
		replicaIncoming:  make(chan MessageIn, 4096),
		registerClient:   make(chan *client),
		unregisterClient: make(chan *client),
		clients:          make(map[ClientID]*client),
		clientOutgoing:   make(chan *MessageOut),
		clientIncoming:   make(chan MessageIn, 4096),
	}, nil
}

type transport struct {
	id NodeID

	addr  string
	addrs map[NodeID]string

	udpAddrs map[NodeID]*net.UDPAddr

	replicaConn    net.PacketConn
	replicaDecoder *json.Decoder

	replicaIncoming chan MessageIn

	registerClient   chan *client
	unregisterClient chan *client
	clients          map[ClientID]*client

	clientOutgoing chan *MessageOut
	clientIncoming chan MessageIn
}

func (t *transport) ID() NodeID {
	return t.id
}

func (t *transport) Start() error {
	// If incoming channels are not buffered, it's best not to start.
	if cap(t.replicaIncoming) == 0 || cap(t.clientIncoming) == 0 {
		return errors.New("incoming channels should be buffered")
	}

	err := t.startReplicaTransport()

	if err != nil {
		return err
	}
	go t.startClientHandler()
	go t.startClientTransport()

	return nil
}

func (t *transport) startReplicaTransport() error {
	// Resolve all UDP addresses.
	for id, addr := range t.addrs {
		udpAddr, err := net.ResolveUDPAddr("udp", addr)

		if err != nil {
			return err
		}

		t.udpAddrs[id] = udpAddr
	}

	// Listen for replica messages.
	conn, err := net.ListenUDP("udp", t.udpAddrs[t.id])

	if err != nil {
		return err
	}

	t.replicaConn = conn
	t.replicaDecoder = json.NewDecoder(conn)

	// Read incoming replica messages.
	go t.replicaRead()

	return nil
}

func (t *transport) replicaRead() {
	var message MessageIn

	for {
		err := t.replicaDecoder.Decode(&message)

		if err != nil {
			// Ignore failed messages, they should be re-transmitted.
			continue
		}

		t.replicaIncoming <- message

		// Don't want to leak data from an old message.
		message.Data = nil
	}
}

func (t *transport) startClientTransport() {
	server := http.NewServeMux()
	server.HandleFunc("/ws", t.serveWebSocket)

	// Blocks
	err := http.ListenAndServe(t.addr, server)

	log.Println("Shutting down http server")
	// err is always non-nil
	log.Println(err)
}

func (t *transport) startClientHandler() {
	for {
		select {
		case client := <-t.registerClient:
			if _, ok := t.clients[client.id]; !ok {
				t.clients[client.id] = client
			} else {
				log.Println("Already have a connection for client:", client.id)
			}
		case client := <-t.unregisterClient:
			delete(t.clients, client.id)
		case message := <-t.clientOutgoing:
			if client, ok := t.clients[message.clientID]; ok {
				client.outgoing <- message
			} else {
				log.Println("Tried to send:", message)
				log.Println("Client no longer connected.")
			}
		}
	}
}

var upgrader = websocket.Upgrader{}

func (t *transport) serveWebSocket(w http.ResponseWriter, r *http.Request) {
	ws, err := upgrader.Upgrade(w, r, nil)

	if err != nil {
		log.Println("upgrade:", err)

		return
	}

	defer ws.Close()

	// Read one message before registering the client.
	// This is to learn the client's id.
	// After registering, deliver the message we read.

	var message MessageIn

	err = ws.ReadJSON(&message)

	if err != nil {
		log.Println(err)

		return
	}

	if len(message.ClientID) == 0 {
		log.Println("Message from client missing ClientID:", message)

		return
	}

	client := &client{
		id:       message.ClientID,
		conn:     ws,
		outgoing: make(chan *MessageOut),
		incoming: t.clientIncoming,
	}

	// Register the client so that we can respond to it later.
	t.registerClient <- client

	// Deliver the message we just read.
	go func() { t.clientIncoming <- message }()

	go client.write()
	client.read()

	t.unregisterClient <- client
}

func (t *transport) NotifyReplica(nodeID NodeID, message *MessageOut) {
	encoded, err := json.Marshal(*message)

	// This should not happen.
	if err != nil {
		// Ignore message if it somehow does, but log it.
		log.Println("Could not marshal message:", err)

		return
	}

	// We don't care if this succeeds or not, so we ignore the error returned.
	t.replicaConn.WriteTo(encoded, t.udpAddrs[nodeID])
}

func (t *transport) NextReplicaMessage() *MessageIn {
	message := <-t.replicaIncoming

	return &message
}

func (t *transport) NotifyClient(clientID ClientID, message *MessageOut) {
	message.clientID = clientID

	t.clientOutgoing <- message
}

func (t *transport) NextClientMessage() *MessageIn {
	message := <-t.clientIncoming

	return &message
}
