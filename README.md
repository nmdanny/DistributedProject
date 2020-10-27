# Practical Exercise 1 in Distributed Algorithms

By Daniel Kerbel(CSE user `danielkerbel`)


## Framework

We'll build a framework for modelling unreliable channels. The transport will be TCP, but we will not rely on the built
in reliability mechanisms of TCP, or the way IP packets are routed - we'll only look at the payload of the packets, which are JSON objects representing
messages. Therefore, these messages will form the lowest layer of our simulated network stack.

This framework is coded in Rust, using the Tokio library which allows for asynchronous IO using futures(async-await),
and some helper libraries that deal with message framing(turning byte streams into objects)


## Solution

We wish to attain safety & liveness within the setting specified in the PDF.
We'll deal with the adversary as follows:

First, every message(request/response) will be identified by the client who initiated it
Secondly, every client will maintain a counter that is incremented upon each successful request, and the request(and response) will
contain said counter at time of request.

The overall flow will be clients sending requests, and then waiting for an appropriate broadcast before proceeding

### Message drop


Due to synchrony assumption, clients can assume that the server will receive their request and broadcast their message in a certain amount of time.
If not, they'll simply retransmit the message(with the original counter)

This handles both request drops or response drops involving the initiating client

A more complicated issue is that not all clients may receive a broadcast chat message, resulting in an inconsistent
chat view. To deal with this, clients will ask for the entire log if they detect an inconsistency(they recognize
that a message was skipped)

### Message re-ordering

On the server's side, the server will maintain a counter with each client, and process requests in the order of their numbers.
Requests whose number is higher than the counter will enter a queue and only be processed when it is their turn. This ensures
each client's messages will appear in the log in the order they came. 


### Message duplication

Since each message is identified by the initiating client and numbered, and the server maintains a counter of processed
messages for each client, the server will be able to determine duplicate messages(their number is smaller than the counter)

Note that some duplicates might be legitimate in the sense that they were initiated by a client due to a dropped/re-ordered
response. Therefore, the server will respond to them as if they are legitimate, but without changing his state.

To do this, the server will remember the last response for each client, and re-transmit said response upon receiving
a duplicate request - note that only the last response is sufficient, because duplicate messages that are older were 
definitely made by the adversary, as a client does not proceed before receiving a response to his last message.





