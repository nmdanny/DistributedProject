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

The main loop of a `Node` is spawned in a separate task/thread, yet we need to notify the node of incoming
messages (AppendEntry/RequestVote RPCs, which should be handled in any state) when our networking layer receives them.
 
This is done via channels - the `NodeCommunicator` struct is essentially our access hatch to the `Node`
