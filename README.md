# talktalk

talktalk is a package for sending messages between nodes and clients.

- Messages between nodes are sent using UDP.
- Messages between a node and a client are sent using websocket.
- All communication is unreliable, meaning, if messages somehow fail to be sent or received, those messages are simply dropped.
  - It's the node's/client's job to retransmit messages that are not answered.

This package was created to be used as the network layer between Paxos replicas and its clients.
