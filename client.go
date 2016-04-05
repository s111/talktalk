package talktalk

import (
	"sync"
	"time"

	"github.com/gorilla/websocket"
)

type client struct {
	id   ClientID
	conn *websocket.Conn

	ok chan bool

	outgoing chan *MessageOut
	incoming chan<- MessageIn

	stop chan int

	routines *sync.WaitGroup
}

func (c *client) write() {
	c.routines.Add(1)
	defer c.routines.Done()

	for {
		select {
		case message := <-c.outgoing:
			c.conn.WriteJSON(message)

		case <-c.stop:
			// Fail all further reads.
			// BUG: SetReadDeadline and ReadJSON are called concurrently.
			// FIX: Use ping pong instead.
			c.conn.SetReadDeadline(time.Now())

			return
		}
	}
}

func (c *client) read() {
	var message MessageIn

	for {
		err := c.conn.ReadJSON(&message)

		if err != nil {
			return
		}

		select {
		case c.incoming <- message:
		case <-c.stop:
			return
		}

		// Don't want to leak data from an old message.
		message.Data = nil
	}
}
