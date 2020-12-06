# Exercise 2 - Synchronous consensus, n > 2f

I decided to implement the Raft protocol, despite the fact we've not learnt it in class, it turns out to be simpler to
implement:

- It has a [clear specification](https://raft.github.io/raft.pdf) including pseudo-code, in contrast to Multi-Paxos
  which has many wildly varying implementations
  
- It is simpler to implement and reason about: the data flows from the leader to the followers, and uses a request-response
  model with only 1 phase: Once a leader is elected, he merely sends append requests, and upon receiving enough responses
  can consider the data a majority
  
## Design/Implementation

The `Node` struct contains all state used by a server in any state
There are several state objects that borrow `Node`: `FollowerState`, `CandidateState`, `LeaderState`
each has an async loop function that runs the logic of a Raft node for said state. Likewise,
there's an async loop function for `Node` itself, which continually switches between one of the previous loops.

State changes are done by the `Node::change_state` function - the various state loops constantly check they're in
the matching state, otherwise we break out of the (Follower/Candidate/Leader) state loop, and the main Node loop
will begin a state loop for the new state.

The main loop of a `Node` is spawned into a separate task, therefore we use message passing to notify the node of incoming
messages (AppendEntry/RequestVote RPCs) when our networking layer receives them.

Communication from/to a node is primarily done via the following:
  
- `NodeCommunicator` is used to respond to requests(originating from clients or other Raft nodes) by invoking handler
   methods on the `Node`
   
- `Transport` is used to send requests from a Raft `Node` (e.g, a candidate sending a vote request, or a leader sending an append entries request)
  to other Raft nodes.
   
A `Transport` is a trait which might can be implemented in various ways, depending how you want to send messages. On
the other hand, `NodeCommunicator` is un-changed(it is a concrete struct) but is usually composed into some web service
that drives the entire process. 

For example, a Raft `Node` would have both a gRPC server and client - the server handler 
invokes methods on `NodeCommunicator`, and the `Node` includes a `Transport` implementation which uses a gRPC client 
to send messages to other servers)

A client(aka, an application using the Raft consensus protocol) would use a gRPC client as well, in order to communicate
with the gRPC server for the leader node(or at least, who the client thinks is the leader)


As of now, all nodes live on a single thread(see `single_process_runner.rs`), and communication between nodes is done via
message passing (see `ThreadTransport`), moreover, clients have direct access to the `NodeCommunicator`, this is only
used for simplicity(there is little point )

## Simulation of adversary

Each `Node` takes a generic `Transport` object which is responsible for communication with other peers.
I've implemented an `AdversaryTransport`, which implements `Transport` by wrapping a `Transport` and delaying/dropping
messages with a given probability. These can be changed at run-time, and we can even pause/un-pause servers by
changing between a probability of 1 and 0.

## Note regarding client omissions

For simplicity, I did not simulate client omissions(client interaction isn't part of the `Transport` trait), 
but by the implementation it is obvious that clients do not help/"cheat" in achieving consensus, as only the leader
node responds to their write requests, and followers/candidates ignore their contents and simply forward them to 
the leader. If the leader isn't known, the client can simply guess a leader, and eventually he'll reach the correct
leader. (In fact, this is what `single_process_runner.rs` does)

Clients can deal with request/response omissions by re-sending their request until getting a positive answer. There
is a chance that their proposed value was committed but they're not aware - I handle this by sending read requests
to read the latest portion of the log, and ensuring the value isn't there before submitting the value again.


## References

- Overall program design is heavily based on [async-raft](https://github.com/async-raft/async-raft/)
- [Implementing Raft](https://eli.thegreenplace.net/2020/implementing-raft-part-1-elections/)
- The raft [specification](https://raft.github.io/raft.pdf)