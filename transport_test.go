package talktalk

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/gorilla/websocket"
)

func TestReplicaTransport(t *testing.T) {
	// Third node added but not started,
	// this is to make sure there is no failure,
	// if the node can't be reached.
	nodes := []string{":8081", ":8082", ":8083"}

	nodeA := createTransport(t, 1, nodes)
	nodeB := createTransport(t, 2, nodes)

	startTransport(t, nodeA)
	startTransport(t, nodeB)

	messageA := &MessageOut{
		Type: 1,
		Data: "message A",
	}

	messageB := &MessageOut{
		Type: 1,
		Data: "message B",
	}

	sendReplicaMessage(t, nodeA, nodeB, messageA)
	sendReplicaMessage(t, nodeB, nodeA, messageB)
}

func TestClientTransport(t *testing.T) {
	// Second node added but not started,
	// this is to make sure there is no failure,
	// if the node can't be reached.
	// Client needs full address, so localhost is added to the first node.
	// Transport should handle it fine.
	nodes := []string{"localhost:8084", ":8085"}

	nodeA := createTransport(t, 1, nodes)

	startTransport(t, nodeA)

	// Give http server time to start.
	<-time.After(time.Millisecond * 10)

	client, _, err := websocket.DefaultDialer.Dial("ws://"+nodes[0]+"/ws", nil)

	if err != nil {
		t.Fatal(err, "\nHttp server probably didn't get time to start.")
	}

	clientID := "test client"
	payload := "test message"
	message := "{\"type\": 1, \"data\": \"" + payload + "\", \"clientID\": \"" + clientID + "\"}"

	err = client.WriteMessage(websocket.TextMessage, []byte(message))

	if err != nil {
		t.Fatal(err)
	}

	received := nodeA.NextClientMessage()

	var receivedData string

	err = json.Unmarshal(received.Data, &receivedData)

	if err != nil {
		t.Fatal(err)
	}

	// Check that id match.
	if ClientID(clientID) != received.ClientID {
		t.Errorf("Expected '%s' as ClientID received '%s'", clientID, received.ClientID)
	}

	// Check that data match.
	if payload != receivedData {
		t.Errorf("Sent '%s' to nodeA received '%s'", payload, receivedData)
	}
}

func createTransport(t *testing.T, id NodeID, nodes []string) Transport {
	transport, err := NewTransport(id, nodes)

	if err != nil {
		t.Fatal("NewTransport failed:", err)
	}

	return transport
}

func startTransport(t *testing.T, transport Transport) {
	err := transport.Start()

	if err != nil {
		t.Fatal("Could not start transport:", err)
	}
}

func sendReplicaMessage(t *testing.T, from Transport, to Transport, message *MessageOut) {
	// Send message.
	from.NotifyReplica(to.ID(), message)
	// Receive message.
	messageReceived := to.NextReplicaMessage()

	var receivedData string

	err := json.Unmarshal(messageReceived.Data, &receivedData)

	if err != nil {
		t.Fatal("Could not decode data:", err)
	}

	// Check that they match.
	if message.Data != receivedData {
		t.Errorf("Sent '%s' to nodeB received '%s'", message.Data, receivedData)
	}
}
