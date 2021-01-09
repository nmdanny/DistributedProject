# Exercise 3 - Anonymous chat with omission failures, n > 2f

Denote `n` number of nodes, `c` number of clients, `d = a*c*log_2(c)` length of log entry(an array), where
`a` is a constant I can choose. `f` is the number of faulty nodes(omissions + listening), all clients might be faulty
and `n > 2f`.

## Overall idea

Essentially, we have 'd' different channels for each server, and when a client wishes to send a secret `S`,
first, he chooses randomly(uniformly) which channel to use, and then, for each server `i`, sends him
the secret share `(i, p(i))` via channel `d`, where `p` is a randomly generated polynomial(over finite prime field) encoding his secret `S`

On all other channels, he sends the secret value 0(or some other special sentinel value) via the same idea. This means, a client generates `d` polynomials,
one of them encoding `S` and the others encoding some null value.

When a server receives a share `(i, p(i))` via channel `d`, he will add it to log entry at `d`, along with an identifier from which client it arrived
(the identifier uses `log_2(c)` space, so in total, each channel uses `clog_2(c)`, and we have `d` of them.)



Note, if we have either client or server omissions - recovery isn't guaranteed. For instance, an invalid client might send his secret to a majority of nodes and a minority
of valid nodes, such that the total number of nodes is still a majority, whereas another, valid client, has only sent it to the valid nodes(invalid ones omitted it). In total,
the invalid node's shares were seen by the majority, yet they might still crash once we begin reconstruction.

So, the fact invalid nodes can stall the network, breaks liveness, as invalid nodes would still be able to get more


So, there's another approach - which can live completely within the consensus protocol while maintaining anonymity:

Clients send all their shares, over all channels, such that each share is asymetrically encrypted with the corresponding server's private ekey.
The sending process will be done via consensus.

This approach simplifies everything related to agreement - by the correctness of Raft/Paxos, every node which sees his share(committed),
knows that at least `f+1` other nodes have seen it, in particular, one valid node must have seen it, so all valid nodes have seen it too, so it must be recoverable
amongst these nodes. 

The main disadvantage is overhead - each node will maintain all shares in the system over its log, while only being able to decrypt 1/c'th of them



Lets assume for a moment that two clients can never transmit a non zero secret via the same channel.

Whenever the server receives a share, he knows which client sent it, but doesn't know how to reconstruct it. 
As a round progresses, a server will receive shares for many different polynomials over the same channel, and add them all up -
he will never know if the share he's adding represents the real client value `S`, or a value of zero.

Eventually, when we reach the reconstruction stage, servers will reconstruct the values at each channel. By properties of polynomial addition,
the polynomial gained by the reconstruction of (1, p(1) + q(1) + s(1), ...), ..., (f+1, p(f+1) + q(f+1), s(f+1), ...), is p+q+s+...
So, the intersection of (p+q+s+...) with the Y-axis, is the sum of the secrets whose shares were sent over that channel. So, for the channel `d` above,
under our assumption, this would be `S` + 0 + 0 + ... 0
So, we managed to reconstruct the secret(safety+validity) while preserving anonymity, as requried.

Note:
Channels are a theoretical concept, we don't literally have `d` different networking channels for every server.
A client will send every server `i` a batch of `d` shares, for the same X coordinate `i`, each one representing a different secret,
with all but one of the secrets being 0 (or, all are 0, in case the client has nothing to say)

Similarly, during reconstruction, servers will send batches of values(aka, their entire log entry for current phase)

## Dealing with collisions

Since clients are not synchronized, it is possible for two clients to transmit non zero values over the same channel. When servers
reconstruct the value at said channel, they will get a value which isn't valid. How can we deal with this?

### Prevention

We can use some sort of randomness beacon to choose which client's turn it is to send a value 
over a certain channel. This is not the approach required in this exercise (and if we used it, we could
forgo channels altogether), and using it would be akin to circular reasoning - if a randomness beacon can be implemented in 
a way that ensures privacy, that implementation would probably be based on what we're trying to implement.

### Detection & Re-transmission

First, we need a way to detect collisions. A simple way would be to encode some kind of checksum within the secret, e.g, some bits of S will be used for checksum over the rest
of the bits

(The checksum wouldn't break anonymity/safety, because it is part of the secret. This wouldn't necessarily be the case if it were stored in plain-text)

Then, after performing reconstruction, we try to decode the checksum and apply it on the rest of the decoded value. If it matches, we can be almost certain there was
no collision. (The probability of both having a collision on a channel, and the resulting data being interpreted as a valid checksum matching the rest of the data in the value, are
extremely low if we use a good checksum)

Once we detected a collision, we can notify 'Collision over channel d', and next round, all clients who are responsible for the collision(who sent real values over `d`) will
try re-sending it over different channels(randomly generated) next round

## Client omissions

There is a potential issue with client omissions - suppose a client sends a share (even of a meaningless value like 0) over some channel to some servers, but crashes before sending
at least f+1 shares. Every server which has received it, has added it to the corresponding channel, thereby effectively corrupting that channel, making its contents unrecoverable.
Retrying wouldn't help, since a faulty client can repeat this behavior every time.

Obtaining consensus on the fact that a given client indeed sent his shares to f+1 servers, isn't enough, since some of those f+1 servers might be faulty too. 
Here's an example:

n = 3, c = 3, f = 1
ClientA -> shares to servers A,B,C
ClientB -> shares to servers A,B
ClientC -> shares to servers B,D


By the consensus mechanism, lets say that nodes B,D agree on getting shares from client B
Now, note that no other agreement is possible in this phase - for example, nodes A,B(,C) cannot
agree on getting shares from from client A, because nodes A(,C) didn't hear a share from client B - after all,
we are trying to have consensus on a sequence of values, in this case, a sequence of clients.

So ultimately, only nodes B,D agreed on shares, and the rest of the nodes are inactive.
Next round, B and D will try to reconstruct their shares. Now, suppose that 'D' is faulty, then no one accepted anything.

So, to have the chance of reconstructring some secret, we should have the maximal set of nodes agree on some value. In this case,
had nodes A,B,C agreed on the input from value client A instead, then even if one of them would crash, the secret would still be recoverable.
So, before entering the reconstruct stage, we could all nodes agree on the number of clients they've seen, and then, share 



I have another solution, which also makes the solution more simple to implement:

- Each client sends all its shares(over all channels) in a single message, where each share is asymetrically encrypted with the corresponding server's
  





In effect, if a client 

## How do we tie this to consensus?

A consensus algorithm(Raft, in my case) can be used for two types of messages
  - sending of (batched) shares between servers
  - notifying that a (batch) of messages was reconstructed

Note that in consensus, the clients are actually the nodes and not the original clients(the dealers), this is
important because in some implementations(like Raft), clients only send messages to the leader, we don't want
the secret sharing client to send all shares to the leader as this breaks anonymity.

Another observation - it is redundant to use consensus for the first type of message, because the SS client
already sends shares to all servers, and if a reconstruct message is committed, then it implies a majority
did pass a majority of those shares to a majority of the servers(`f+1` different shares seen among `f+1` different servers)

However, from an implementation standpoint, there isn't a major difference, either way we'll eventually need to elect a leader in order to commit a secret
which was re-constructed, so we'll use consensus on both message types. The overhead of this is negligible.

Another thing, we will expand our consensus mechanism to support state machines, in my current implementation, servers can only perform consensus logic,
using state machines will allow them to perform arbitrary logic(e.g, reconstruction) in response to these messages.

## Other concerns

- How do we achieve anonymity? Every client will always send a message at the beginning of each round, so we'll use a special value(e.g, 0)
  if a client has nothing to say.

- Message lengths
  We want the length of messages to stay secret, so we cannot use a variable finite field size, nor
  can we use a variable amount of blocks per a secret share of a client within a single round - from both of these the attacker can infer
  which client has a longer secret. (Moreover, I suspect that the latter approach might leak information about the contents of the secret,
  in addition to its length, but this depends on how we implement block chaining, similar to the dangers of ECB in block ciphers)

  So, the simplest solution is to define a maximal message size, tied to the length of our finite field. 
  If we're limited to 64 bit integers, suppose the largest prime is 60 bit, so we'd have 7 bytes, or 7 ASCII characters per message.
  We can also use a bignum library to support messages of larger size, for simplicity I will probably not use this approach.

  Another possibility is to split a long message over many rounds, this approach also brings some extra complexity, moreover
  the increased latency for big messages might be deemed too big, so I will not use this either.

- Can adversaries comprise secrecy/anonymity, e.g, by omissions/crashes or even without?

  Adversaries cannot compromise secrecy by crashes as the number of shares in the system can only decrease. 

  Nor can they compromise anonymity - consider the set of non faulty clients C and assume |C|>2 - if a faulty server has managed to reconstruct
  a secret of C via some channel `d` - this implies he summed at least `f+1` different shares for channel `d`, thus at least one of them
  came from a valid server. The valid server, by its way of operation, has created that share by summing shares of all C clients

  Therefore, the faulty server has reconstructed the secret by summing the shares of at least C clients, and by anonymity of addition,
  cannot attribute the value to any of them. 

  Client omissions can supposedly leak the fact that a client didn't send anything, however, client omissions
  imply that the client is faulty, AKA, part of the colluding set, thus the colluding set obviously doesn't learn
  anything new.