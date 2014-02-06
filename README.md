# network-transport-pipes #

A "named pipes" instantiation of the Network Transport API.

## Design Notes ##

- A new `Transport` creates a new named pipe
  - it also creates a background thread that polls this pipe for IO
- A new `Endpoint` creates a new channel
  - a receive on an `Endpoint` is simply a receive on this channel
  - no sends are allowed on an `Endpoint`.
- A new `Connection` has to be created to send on an endpoint
  - Open the named pipe pointed to by the remote endpoint address.
  - send a control message to the remote process with the source
    endpoint address.
  - the remote process maintains a `ConnectionMap` (`EndpointPair` x
    `ConnectionId`) to lookup the connection Id.

```
         Transport A			       Transport B
	 +-----------------+		   +------------------+
	 |                 |		   |           	      |
	 |    Endpoint 1   |		   |    Endpoint 1    |
	 |                 |		   |           	      |
	 |                 |		   |    Endpoint 2    |
	 |                 |		   |                  |
	 +-----------------+		   +------------------+
```

One thing to note here is that `connect` is mostly a local
operation. This is because opening a connection simply involves
opening a named pipe residing in a shared namespace. But this means
that we have no way to differentiate between connections originating
from the same local endpoint. In fact, the current implementation
reuses an existing connection, if one exists, in such a case. As a
result of this design choice, this example from
`network-transport-tests` fails:

```
    -- Open two connections to the server
    Right conn1 <- connect endpoint server ReliableOrdered defaultConnectHints
    ConnectionOpened serv1 _ _ <- receive endpoint

    Right conn2 <- connect endpoint server ReliableOrdered defaultConnectHints
    ConnectionOpened serv2 _ _ <- receive endpoint

    -- One thread to send "pingA" on the first connection
    forkTry $ replicateM_ numPings $ send conn1 ["pingA"]

    -- One thread to send "pingB" on the second connection
    forkTry $ replicateM_ numPings $ send conn2 ["pingB"]
```

We want to reuse any existing underlying connections, but
semantically, endpoints should be allowed to create multiple logical
connections amongst themselves. The TCP transport resolves this by
making `connect` a non-local operation which involves a hand-shake
with the server. This is potentially expensive, and unnecessary, for a
connectionless protocol.

## Todos ##

- Implement a non-blocking interface to the named pipe IO operations
- Better error handling
- How do we remove the created files on abort? By associating a
  clean-up action with the interrupt signal?
