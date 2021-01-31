# Anonymous Chat - Final Project

See included PDF on overall flow when client `X` sends a message `M` to client `Y`

## Design Recap

The project is based on several components, some were implemented in previous
exercises and are tested independently, and are layered on top of each other
in order to provide the required functionality of omission-tolerant,
anonymous 2 way communication between clients.

1. Consensus
   
   Implemented via Raft, consists of mostly server logic (the Raft protocol) as well as a client
   module allowing to submit values, as well as being notified whenever a value is committed to the state machine.

2. Anonymous public chat

   On the server's side, implemented as a state machine on top of Raft, allowing client crash tolerance and server omission tolerance. 

   This component will be modified in this project in order to allow server
   omission tolerance, via the use of PKI. This means that a client submits
   all shares of all channels via a single message, such that each share is
   encrypted via the corresponding server's public key. Those shares are
   replicated via the Consensus module, thereby ensuring from this point on,
   the value will be recoverable by all valid servers. Each server decrypts the shares
   intended for him, and is unable to decrypt shares of other servers, thereby ensuring
   secrecy.

   Once the secret is recovered, the servers broadcast the value to all clients.


3. Anonymous 2 way chat

   This will be implemented on top of the public chat, by using encrypted messages as values. When
   client `X` sends a message `M` to client `Y`, once it is recovered by the servers, it will be broadcast
   to all clients (after all, due to anonymity, they cannot tell to who it is intended, so broadcast is their
   only option). Every client will try to decipher it via his private key, and only `Y` will succeed.

   In addition to `M`, there will also be `X`'s identifier which allows `Y` to respond to him.
   (If we want `X` to remain anonymous to `Y`, we can use a randomly generated symmetric key instead, and
   have that key re-generated whenever `X` wishes to start a new communication channel with `Y`)

   Note how the servers don't have any further logic beside what was described in 2, their mere role is basically
   forwarding messages between clients, and use secretr-sharing in order to anonymize them

## Test Plan

Every component can be tested separately. So far I've already implemented some basic tests of Raft and anonymous public chat(these tests are essentially the demos of previous exercises)

A few extra tests for the public chat will be needed for the public chat
after implementing client omission tolerance instead of crash tolerance.

For the final component(2 way chat) there will be some basic tests to ensure the overall flow of a client sending a message, and the other client receiving it, with or without omissions.

So far everything was implemented via multiple async tasks over a single
process, using queues as the transport(wrapped with a simulated adversary
which can omit or delay messages). This was convenient for testing, but I
will also implement a real gRPC transport along with a text interface to the client.
