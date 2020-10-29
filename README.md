# Practical Exercise 1 in Distributed Algorithms

By Daniel Kerbel(CSE user `danielkerbel`)


## Framework

We'll build a framework for modelling unreliable channels. The transport will be gRPC, but we will not rely on the built
in reliability mechanisms of gRPC/HTTP/TCP, or the way IP packets are routed - we'll only look at the payload of the packets,
which are messages serialized via protobuf. Therefore, these messages will form the lowest layer of our simulated network stack.

gRPC is based on a request-response flow, as well as streaming. A `Subscribe` message initiates a server to client stream,
which is effectively used to broadcast messages to all subscribed peers.


This framework is coded in Rust, using the [Tonic](https://github.com/hyperium/tonic/) library for gRPC. 


## Problem

See the PDF

## Solution

We wish to attain safety & liveness within the setting specified in the PDF.
We'll deal with the adversary as follows:

- Every message(request/response) will be identified by the client who initiated it(`client_id`)
  
- Every client will maintain a counter that is incremented upon each successful request, and the request(and response) will
  contain said counter at time of request.(`sequence_num`)

The overall flow will be clients sending requests, and then waiting for an appropriate broadcast before proceeding - if no
response is received in an appropriate time, they will re-transmit their request(due to the synchrony assumption there is
a known timeout number)
 
### Message re-ordering
 
When clients create requests and await for responses serially, message re-ordering is not a problem, however, clients can also make several requests in parallel
and wait for their responses to come in any order - this is where `sequence_num` comes into play, it ensures the server
will process them in the order they came, ensuring safety.

Note - we don't care about message ordering among different clients as this is a different problem that has more to do
with synchronization/locking strategies rather than safety.

### Duplicate messages

The `sequence_num` is also used to determine duplicate requests/responses. The server will not manipulate his state
upon encountering a duplicate write request, however he will respond with the original response - this is because
some duplicate requests are actually legitimate, and occurred due to a response message being dropped.

Clients can use the index of broadcast write messages to determine if a chat message is duplicated(the index of a new
message should be client_log_view.length + 1)

A trickier issue is that broadcasts might not arrive to all clients, causing some of them to have an incomplete
view. This is solved in one of two ways:

1. When the client eventually receives another broadcast, he can inspect the index to determine the length of the real log;
  if he finds out that the index is bigger by his own log's view length by more than 1, it means he has missed some messages
  and can request the log from the server
  
2. In case no broadcast was received in a certain amount of time, the client can actively


* Technical note: Because gRPC is naturally request-response oriented, I found it very difficult to duplicate response messages
  towards the client, so the adversary will only duplicate request messages to the server.  

### Message drop


Due to synchrony assumption, clients can assume that the server will receive their request and broadcast their message in a certain amount of time.
If not, they'll simply retransmit the message(with the original counter)

This handles both request drops or response drops involving the initiating client

A more complicated issue is that not all clients may receive a broadcast chat message, resulting in an inconsistent
chat view. To deal with this, clients will ask for the entire log if they detect an inconsistency(they recognize
that a message was skipped)






