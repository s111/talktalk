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
			c.conn.SetWriteDeadline(time.Now().Add(writeWait))
			c.conn.WriteJSON(message)

		case <-c.stop:
			c.conn.SetWriteDeadline(time.Now().Add(writeWait))
			c.conn.WriteMessage(websocket.CloseMessage, []byte{})

			return
		}
	}
}

func (c *client) read() {
	var message MessageIn

	for {
		c.conn.SetReadDeadline(time.Now().Add(readWait))
		err := c.conn.ReadJSON(&message)

		select {
		case <-c.stop:
			return
		default:
		}

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
