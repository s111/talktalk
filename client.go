package talktalk

import (
	"log"

	"github.com/gorilla/websocket"
)

type client struct {
	id   ClientID
	conn *websocket.Conn

	outgoing chan *MessageOut
	incoming chan<- MessageIn
}

func (c *client) write() {
	for {
		message := <-c.outgoing

		err := c.conn.WriteJSON(message)

		if err != nil {
			log.Println(err)
		}
	}
}

func (c *client) read() {
	var message MessageIn

	for {
		err := c.conn.ReadJSON(&message)

		if err != nil {
			log.Println(err)

			return
		}

		c.incoming <- message

		// Don't want to leak data from an old message.
		message.Data = nil
	}
}
