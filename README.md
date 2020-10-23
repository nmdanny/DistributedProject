# Practical Exercise 1 in Distributed Algorithms

By Daniel Kerbel(CSE user `danielkerbel`)


## Framework

We'll build a framework for modelling unreliable channels.

I will use the Rust language, and the [Actix](https://github.com/actix/actix) library which provides an actor framework.

From [wikipedia](https://en.wikipedia.org/wiki/Actor_model):

> The actor model in computer science is a mathematical model of concurrent computation that treats actor as the 
> universal primitive of concurrent computation. In response to a message it receives, an actor can: make local decisions,
> create more actors, send more messages, and determine how to respond to the next message received. 
> Actors may modify their own private state, but can only affect each other indirectly through messaging 
> (removing the need for lock-based synchronization). 

There's a wide array of possibilities for implementing actors:

- Threads or async coroutines that live within a single process.
 
- Processes communicating via IPC within a single machine

- Processes communicating via a network protocol(TCP, HTTP, etc) over multiple machines

In this exercise, the entire simulation will run in a single process - I did not want to use a "real" networking stack
(e.g, HTTP or TCP) because implementing real adversaries over a real network(e.g, by implementing a HTTP proxy to perform MITM) seems out of scope
for this course, and since we wish to build safe systems from "first principles", using established protocols that already
provide many security/liveliness guarantees(e.g TLS for authentication, TCP retransmission, etc..) would contrast with this goal.


## Solution

We wish to attain safety & liveness within the setting specified in the PDF.
We'll deal with the adversary as follows:

1. Message drop - whenever a client transmits a message, he will wait for the server to respond/broadcast said message within a certain amount of time
(due to the synchrony assumption, said bound is known), if no response is given, the message will be retransmitted and
this process will repeat. As long as probability(message-drop) < 1, the message will eventually arrive, thus liveness
    is guaranteed.

2. Message reordering - each message will be numbered, starting from 1.
   For each client, the server will maintain a counter and a dictionary mapping message number to message.

    Whenever the server encounters a message whose number is higher than the client's counter, it will be placed into the dictionary,
    and the server will periodically inspect the dictionary until it finds a message which the expected number(such message
    is guaranteed to arrive eventually), then it will process it.
    
3. Message duplication - whenever the server encounters a message whose number is smaller than its client's counter, it will ignore the message.
    



