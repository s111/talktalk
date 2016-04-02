package talktalk

import (
	"encoding/json"
	"reflect"
	"testing"
	"time"

	"github.com/gorilla/websocket"
)

func TestNewTransportInvalidID(t *testing.T) {
	var idTests = []struct {
		in  NodeID
		out error
	}{
		{0, ErrInvalidID(0)},
		{-1, ErrInvalidID(-1)},
	}

	nodes := []string{":8081", ":8082", ":8083"}

	for _, tt := range idTests {
		_, err := NewTransport(tt.in, nodes)

		if err != tt.out {
			t.Errorf("NewTransport(%d, nodes) => _, %q, want %q", tt.in, err, tt.out)
		}
	}
}

func TestNewTransportMissingAddr(t *testing.T) {
	id := NodeID(4)
	nodes := []string{":8081", ":8082", ":8083"}

	_, err := NewTransport(id, nodes)

	if err != ErrMissingAddr(id) {
		t.Errorf("NewTransport(%d, nodes) => _, %q, want %q", id, err, ErrMissingAddr(id))
	}
}

func TestNewTransportInvalidAddr(t *testing.T) {
	id := NodeID(1)
	nodes := []string{"#INVALID#", ":8082", ":8083"}

	_, err := NewTransport(id, nodes)

	if err == nil {
		t.Errorf("NewTransport(%d, %v) => _, %q, want error", id, nodes, err)
	}
}

func TestNodeID(t *testing.T) {
	id := NodeID(1)
	nodes := []string{":8081", ":8082", ":8083"}

	transport, _ := NewTransport(id, nodes)
	got := transport.ID()

	if got != 1 {
		t.Errorf("NewTransport(%d, nodes).ID() produced %+v expected %+v", id, got, id)
	}
}

func TestTransportStartStop(t *testing.T) {
	defer func() {
		if r := recover(); r != nil {
			t.Fatalf("panicked: %v", r)
		}
	}()

	id := NodeID(1)
	nodes := []string{":8081", ":8082", ":8083"}

	transport, _ := newTransport(id, nodes)
	transport.Start()

	done := make(chan bool)

	go func() {
		transport.Stop()
		done <- true
	}()

	select {
	case <-done:
	case <-time.After(time.Millisecond * 10):
		t.Error("Stop() didn't finish in time")
	}
}

func TestTransportStopBeforeStart(t *testing.T) {
	defer func() {
		if r := recover(); r == nil {
			t.Fatalf("calling Stop() before Start() should cause a panick")
		}
	}()

	id := NodeID(1)
	nodes := []string{":8081", ":8082", ":8083"}

	transport, _ := newTransport(id, nodes)
	transport.Stop()
}

func TestNotifyReplicaBeforeStart(t *testing.T) {
	defer func() {
		if r := recover(); r == nil {
			t.Fatalf("calling NotifyReplica() before Start() should cause a panick")
		}
	}()

	id := NodeID(1)
	nodes := []string{":8081", ":8082", ":8083"}

	transport, _ := newTransport(id, nodes)
	transport.NotifyReplica(id, &MessageOut{})
}

func TestNotifyClientBeforeStart(t *testing.T) {
	defer func() {
		if r := recover(); r == nil {
			t.Fatalf("calling NotifyClient() before Start() should cause a panick")
		}
	}()

	id := NodeID(1)
	nodes := []string{":8081", ":8082", ":8083"}

	client := ClientID("test client")

	transport, _ := newTransport(id, nodes)
	transport.NotifyClient(client, &MessageOut{})
}

func TestNextReplicaMessageBeforeStart(t *testing.T) {
	defer func() {
		if r := recover(); r == nil {
			t.Fatalf("calling NextReplicaMessage() before Start() should cause a panick")
		}
	}()

	id := NodeID(1)
	nodes := []string{":8081", ":8082", ":8083"}

	transport, _ := newTransport(id, nodes)
	transport.NextReplicaMessage()
}

func TestNextClientMessageBeforeStart(t *testing.T) {
	defer func() {
		if r := recover(); r == nil {
			t.Fatalf("calling NextClientMessage() before Start() should cause a panick")
		}
	}()

	id := NodeID(1)
	nodes := []string{":8081", ":8082", ":8083"}

	transport, _ := newTransport(id, nodes)
	transport.NextClientMessage()
}

func TestReplicaMessageExchange(t *testing.T) {
	nodeA := NodeID(1)
	nodeB := NodeID(2)
	nodes := []string{":8081", ":8082", ":8083"}

	transportA, _ := NewTransport(nodeA, nodes)
	transportB, _ := NewTransport(nodeB, nodes)

	transportA.Start()
	transportB.Start()

	message := &MessageOut{
		Type: 1,
		Data: "some data",
	}

	encoded, _ := json.Marshal(message.Data)

	expectedMessage := &MessageIn{
		Type: message.Type,
		Data: encoded,
	}

	transportA.NotifyReplica(nodeB, message)

	receivedMessage, _ := transportB.NextReplicaMessage()

	if !reflect.DeepEqual(expectedMessage, receivedMessage) {
		t.Errorf("NextReplicaMessage() => %q, _, want %q", receivedMessage, expectedMessage)
	}

	transportA.Stop()
	transportB.Stop()
}

func TestNotifyAllReplicas(t *testing.T) {
	nodeA := NodeID(1)
	nodeB := NodeID(2)
	nodeC := NodeID(3)
	nodes := []string{":8081", ":8082", ":8083"}

	transportA, _ := NewTransport(nodeA, nodes)
	transportB, _ := NewTransport(nodeB, nodes)
	transportC, _ := NewTransport(nodeC, nodes)

	transportA.Start()
	transportB.Start()
	transportC.Start()

	message := &MessageOut{
		Type: 1,
		Data: "some data",
	}

	encoded, _ := json.Marshal(message.Data)

	expectedMessage := &MessageIn{
		Type: message.Type,
		Data: encoded,
	}

	transportA.NotifyAllReplicas(message)

	receivedMessageA, _ := transportA.NextReplicaMessage()
	receivedMessageB, _ := transportB.NextReplicaMessage()
	receivedMessageC, _ := transportC.NextReplicaMessage()

	if !reflect.DeepEqual(expectedMessage, receivedMessageA) {
		t.Errorf("NextReplicaMessage() => %q, _, want %q", receivedMessageA, expectedMessage)
	}

	if !reflect.DeepEqual(expectedMessage, receivedMessageB) {
		t.Errorf("NextReplicaMessage() => %q, _, want %q", receivedMessageA, expectedMessage)
	}

	if !reflect.DeepEqual(expectedMessage, receivedMessageC) {
		t.Errorf("NextReplicaMessage() => %q, _, want %q", receivedMessageA, expectedMessage)
	}

	transportA.Stop()
	transportB.Stop()
	transportC.Stop()
}

func TestAddressAlreadyInUse(t *testing.T) {
	nodeA := NodeID(1)
	nodeB := NodeID(1)
	nodes := []string{":8081", ":8082", ":8083"}

	transportA, _ := NewTransport(nodeA, nodes)
	transportB, _ := NewTransport(nodeB, nodes)

	defer func() {
		if r := recover(); r == nil {
			t.Fatalf("calling Start() on two nodes with the same adress should cause a panick")
		}

		transportA.Stop()
	}()

	transportA.Start()
	transportB.Start()
}

func TestTransportStopBeforeReceive(t *testing.T) {
	nodeA := NodeID(1)
	nodeB := NodeID(2)
	nodes := []string{":8081", ":8082", ":8083"}

	transportA, _ := NewTransport(nodeA, nodes)
	transportB, _ := NewTransport(nodeB, nodes)

	transportA.Start()
	transportB.Start()

	message := &MessageOut{
		Type: 1,
		Data: "some data",
	}

	transportA.NotifyReplica(nodeB, message)

	transportB.Stop()

	_, err := transportB.NextReplicaMessage()

	if err != ErrTransportStopped {
		t.Errorf("NextReplicaMessage() => _, %q, want %q", err, ErrTransportStopped)
	}

	transportA.Stop()
}

func TestClientMessageExchange(t *testing.T) {
	nodeA := NodeID(1)
	nodes := []string{":8081", ":8082", ":8083"}

	client := ClientID("test client")

	transportA, _ := newTransport(nodeA, nodes)

	transportA.Start()

	clientConn, _, err := websocket.DefaultDialer.Dial(toWSAddr(nodes[0]), nil)

	if err != nil {
		t.Fatal(err)
	}

	if len(transportA.clients) != 0 {
		t.Error("a client should not be registered until it has sent it's ClientID")
	}

	message := &struct {
		Type     int
		ClientID ClientID
		Data     interface{}
	}{
		Type:     1,
		ClientID: client,
		Data:     "some data",
	}

	clientConn.SetWriteDeadline(time.Now().Add(writeWait))
	err = clientConn.WriteJSON(message)

	// Send twice
	clientConn.SetWriteDeadline(time.Now().Add(writeWait))
	err = clientConn.WriteJSON(message)

	if err != nil {
		t.Fatal(err)
	}

	firstMessage, err := transportA.NextClientMessage()

	if err != nil {
		t.Fatal(err)
	}

	// Give Transport some time to register client
	<-time.After(time.Millisecond * 10)

	if len(transportA.clients) != 1 {
		t.Error("client was not registered")
	}

	secondMessage, err := transportA.NextClientMessage()

	if err != nil {
		t.Fatal(err)
	}

	if !reflect.DeepEqual(firstMessage, secondMessage) {
		t.Error("second client message received didn't match first one")
	}

	transportA.NotifyClient(client, &MessageOut{
		Type: 1,
		Data: "some data",
	})

	var receivedMessage MessageIn

	clientConn.SetReadDeadline(time.Now().Add(readWait))
	err = clientConn.ReadJSON(&receivedMessage)

	if err != nil {
		t.Fatal(err)
	}

	var receivedData string

	json.Unmarshal(receivedMessage.Data, &receivedData)

	if receivedData != message.Data {
		t.Error("received data didn't match data sent")
	}

	clientConn.Close()

	// Give Transport some time to unregister client
	<-time.After(time.Millisecond * 10)

	if len(transportA.clients) != 0 {
		t.Error("client was not unregistered")
	}

	transportA.Stop()
}

func toWSAddr(addr string) string {
	return "ws://" + addr + "/ws"
}

func BenchmarkSendReceive1500(b *testing.B) {
	nodeA := NodeID(1)
	nodeB := NodeID(2)
	nodes := []string{":8081", ":8082"}

	transportA, _ := NewTransport(nodeA, nodes)
	transportB, _ := NewTransport(nodeB, nodes)

	transportA.Start()
	transportB.Start()

	message := &MessageOut{
		Type: 1,
		Data: "some data",
	}

	b.ResetTimer()

	for n := 0; n < b.N; n++ {
		for i := 0; i < 1500; i++ {
			transportA.NotifyReplica(nodeB, message)
			transportB.NextReplicaMessage()
		}
	}

	b.StopTimer()

	transportA.Stop()
	transportB.Stop()
}
