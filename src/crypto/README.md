# Some notes about crypto within the project

As outlined in the architecture diagram, conceptually we use a lot of assymetric encryption: When the client sends its shares,
he encrypts them with each of the server's public keys. And the secret(a message for a client) is encrypted with that client's
public key as well.

From an implementation standpoint, real crypto systems tend to use a hybrid approach, where a public key cipher is used for 
agreement on a shared key, and then that key is used with a symmetric cipher. The main advantage to this is speed, RSA tends to be
very slow. 

However, this hybrid approach has a downside. Each pair of nodes must agree on a secret key in order to communicate. If we have 100 clients who
wish to communicate with each other, each client has to agree on 99 keys. Moreover, since the encrypted value is broadcast after the recovery stage,
each client will have to try all 99 keys(corresponding to each other client) and try to decrypt the value; We cannot provide an unencrypted hint
about who is the sender(in order to narrow down the key), because that would break anonymity. Overall this also turns to be inefficient and inelegant.

It turns out there's some sort of cryptographic construction in the `NaCl` library, "sealed boxes", which allows this. It is a combination of a key
agreement scheme, a symmetric stream cipher and a MAC, allowing us to reach confidentiality and
integrity with only one public-private key pair, no agreement, similar to RSA, but with better performance, albeit not as good as
symmetric ciphers.

For the issue of encrypting shares from a client to all servers, we will use the hybrid approach, this is because we have relatively few
servers, and the share messages are relatively big(number of channels is proportional to the number of clients), so performance matters more.

## Authentication

Since this project is all about anonymous client messaging, we will ignore client authenticity.
From a usability standpoint, the encrypted messages will include the sender's ID (so the recipient can
respond to him), though of course an adversary can forge them(but then he will not see the
recipient's responses), but we don't care, since there is no inherent trust/identity to a particular client ID in the first place.

As for the servers, however, we do want some form of authentication. While we assume that up to half of the servers, minus 1, are
controlled by the adversary(and the adversary has their private keys), the rest are not. This will be handled by using TLS within
the gRPC transport. 

