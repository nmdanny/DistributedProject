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

When a server receives a share `(i, p(i))` via channel `d`, he will add it to log entry at `d` (as in, integer addition to the value stored there)


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

The main disadvantage is that we wish to minimize the size of the secret shares (as I'll explain below regarding field size), and the checksum would use some of the space
within the secret.

An alternative, is to use a cryptographic hash function on the secret value, and send it alongside the secret, in plain-text(whether it is 0 or not!)
By correctness of cryptographic hash, a PPT adversary has an extremely low probability of gaining any information about the secret from the hash

A disadvantage of this approach, is that we would need to keep a list of all hashes seen over a given channel, and we'd need to verify all of them with the resulting value
and ensure at least 1 match.

Once we detected a collision, we can notify 'Collision over channel d', and next round, all clients who are responsible for the collision(who sent real values over `d`) will
try re-sending it over different channels(randomly generated of course) next round

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