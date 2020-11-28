# Exercise 2 - Synchronous consensus, n > 2f

I decided to implement the Raft protocol, despite the fact we've not learnt it in class, it turns out to be simpler to
implement:

- It has a [clear specification](https://raft.github.io/raft.pdf) including pseudo-code, in contrast to Multi-Paxos
  which has many wildly varying implementations
  
- It is simpler to implement and reason about: the data flows from the leader to the followers, and uses a request-response
  model with only 1 phase: Once a leader is elected, he merely sends append requests, and upon receiving enough responses
  can consider the data a majority