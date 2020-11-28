# Exercise 2 - Synchronous consensus, n > 2f

I initially tried implementing some form of Multi-Paxos, however, unlike single-shot Paxos(also called Single Decree/Synod),
there are many implementation details(such as slots, window size, catchup mechanism, etc..) that can differ between
implementations and it is hard to reason about their correctness, or find pseudocode that takes them into account.

Therefore, I have decided to implement [CASPaxos](https://arxiv.org/pdf/1802.07000.pdf), which is a variation of Paxos
that allows for replicated state machines. 

> An implementation of CASPaxos is a rewritable distributed register.  Clients change its value by submitting side-effect free functions which take 
> the current state as an argument and yield new as a result. Out of the concurrent requests only one can succeed, once a client
> gets a confirmation of the change it’s guaranteed that all future states are its descendants:there exists a chain of changes linking them together.

One can think of a CASPaxos instance as a single re-writeable piece of data indexed by a key, in contrast to Paxos
where eventually all valid nodes decide on a single value. So, a key-value store is managed by many instances
of CASPaxos. However, I don't really need a key-value store, and rather avoid the complexity of many instances.
So instead, the register is the entire state(chat log), and the transition function expects the chat to be in
a certain state, and returns the chat with a certain message appended.

This approach has some issues:

1. The state transitions commands are large, as they need to include the entire log(to match on it) in addition
   to the appended value, and the entire resulting state needs to be transmitted
   
   This can be resolved by using a hash, or some random version number - each append command 

## References

Overview of Paxos implementations: https://vadosware.io/post/paxosmon-gotta-concensus-them-all/

Paxos Made Simple



