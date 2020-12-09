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
  
- `ClientTransport` is used to send requests from a client(an application that uses the Raft library) to the `NodeCommunicator`
   
A `Transport`/`ClientTransport` is a trait which might can be implemented in various ways, depending how you want to send messages. On
the other hand, `NodeCommunicator` is un-changed(it is a concrete struct) but is usually composed into some web service
that drives the entire process. 

For example, a Raft `Node` would have both a gRPC server and client - the server handler 
invokes methods on `NodeCommunicator`, and the `Node` includes a `Transport` implementation which uses a gRPC client 
to send messages to other servers). A client would also have a gRPC client in order to communicate with the leader node(which
might change over time)


As of now, all nodes live on a single thread(see `single_process_runner.rs`), and communication between nodes is done via
message passing (see `ThreadTransport`), moreover, clients have direct access to the `NodeCommunicator`, this is only
used for simplicity(there is little point )

## Simulation of adversary

Each `Node` takes a generic `Transport` object which is responsible for communication with other peers.
I've implemented an `AdversaryTransport`, which implements `Transport` by wrapping a `Transport` and delaying/dropping
messages with a given probability. These can be changed at run-time, and we can even pause/un-pause servers by
changing between a probability of 1 and 0.

## How do clients deal with errors

Clients can deal with request/response omissions and other errors by re-sending their request until getting a positive answer. There
is a chance that their proposed value was committed but they're not aware - I handle this by sending read requests
to read the latest portion of the log, and ensuring the value isn't there before submitting the value again.


# Demo

## Simple Crash

See `single_process_runner.rs` test `simple_crash`

## Random omission of clients and server

Run the executable (`single_process_runner.rs`) - there is a background task which tests
consistency (every entry which is committed, was committed to all logs, and there are no holes between committed entries)

Each client is trying to send his own values(labelled with the client name, beginning from 0): a client waits for a
response from the leader to confirm his value was committed before proceeding to the next one, and in case of an error
(e.g, timeout, or leader redirect) he will try another node(the leader, in case of a redirect, otherwise he will randomly
guess)

## Minimal configuration

Consider the following scenario, we have 3 nodes and various clients, at first, node 0 is a leader, nodes 1,2 are followers.

Suppose a client wants to commit a value 'X', the following is the minimal amount of messages in the system:

```
Client -> ClientWriteRequest('X') -> Node 0
Node 0 -> AppendEntries(entries: ['X'], prev_log_index_term: None, term: 0, leader_id: 0, leader_commit: None) -> Node 1 or 2 (w.l.o.g 0)
Node 1 -> AppendEntriesResponse(success: true, term: 0) -> Node 0
```

At this point, node 0 sees that a majority accepted his AE request - himself, and node 1, and so he considers the entry
committed, and responds to the client appropriately. The adversary cannot influence the commit decision, consider the following cases:

1. If he omits the response to the client(a client omission), the client will re-try. By my implementation of the client,
  the client will ensure the value he's proposing wasn't already committed, so, if he queries node 0 successfully, node 0
  will tell him his value was already committed at index 0.
  
  Further omissions of messages between the client and node 0 will cause him to switch to a different node, but as long as
  leader 0 is a leader, the other nodes will simply redirect him back to node 0. So safety isn't broken.
  
2. Suppose node 0 crashes now(and node 2 didn't see the `AppendEntries` request yet)
   After a short period of inactivity, one of the nodes will start an election:
   
   1. if node 1 starts the election and sends a `VoteRequest(term: 1, candidate: 1, last_log_index_term: (0, 0))` to node 2,
      then his vote will be accepted by node 2, since his log is more up-to-date (his log is longer).
      As a result, node 2 will become the leader, he'll send a heartbeat to node 1, and replicate to him his log(the value ['X']),
      via a similar flow: 
      ```
      Node 1: AppendEntries(entries: ['X'], prev_log_index_term: None, term: 1, leader_id: 1, leader_commit: None)` -> Node 2
      Node 2 -> AppendEntriesResponse(success: true, term: 1) -> Node 1
      ```
      
      By the same logic as before, Node 1 now considers `['X']` committed.
  
   2. If node 2 starts the election and sends a `VoteRequest(term: 1, candidate: 2, last_log_index_term: None)` to node 1,
      his vote will be rejected since node 1's log is more updated. When node 1 sends him a vote, node 2 rejects it because
      every candidate only votes for himself in a given term. Since both of nodes 1 and 2 can't get any votes, they will eventually restart the election.
      
      Supposedly, we might have a deadlock issue, but this is solved by randomness - each node generates a random timeout
      until re-election, so eventually Node 1 will start an election of higher term and send a vote to node 2 who is in an older term,
      and thus node 2 will be forced to accept his vote. The rest of the proof is as the previous case
    
So, this is a short explanation why value 'X' was committed. To show that this is minimal committed configuration, consider 
the scenario where the message `AppendEntriesResponse` is omitted, then the following message flow can cause a different
value to be committed:

```
Node 1 -> RequestVote(term: 1, candidate: 1, last_log_index_term: (0, 0)) -> Node 2
Node 2 -> RequestVoteResponse(term: 1, granted: true) -> Node 1
Node 1 -> AppendEntries(entries: [], prev_log_index_term: None, term: 1, leader_id: 1, leader_commit: None) -> Node 2
Node 2 -> AppendEntriesResponse(success: True, term: 1) -> Node 1
Client -> ClientWriteRequest('Y') -> Node 1
Node 1 -> AppendEntries(entries: ['Y'], prev_log_index_term: None, term: 1, leader_id: 1, leader_commit: None) -> Node 2
Node 2 -> AppendEntriesResponse(success: True, term: 1) -> Node 1
Node 1 -> ClientWriteResponse(WriteOk { commit_index: 0 }) -> Client
```
  
In this scenario, node 0 became partitioned after receiving 'X' in his log, and no other node became aware of 'X'
Eventually, an election starts, w.l.o.g node 1 wins the election, and by similar flow to what was explained before, he
receives a different client request 'Y' and commits a different value.

Another possible scenario is one in which node 1 received the `AppendEntries` request, has sent out an `AppendRentriesResponse`,
but node 0 crashed before receiving that response. Either way, both node 1 and node 2's views are identical to what I explained
in `2`, therefore the value 'X' will be committed, because even if a client makes a different request `Y` - he must wait
for someone to be elected leader, and the leader will have necessarily seen `X` first.

To conclude, we've seen that if the `AppendEntriesResponse` message is omitted, there are two possible deciding configurations
afterwards, which implies that the flow I described at the beginning is the minimal committed configuration.

## References

- Overall program design is inspired by [async-raft](https://github.com/async-raft/async-raft/)
- [Implementing Raft](https://eli.thegreenplace.net/2020/implementing-raft-part-1-elections/)
- The raft [specification](https://raft.github.io/raft.pdf)