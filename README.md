# Anonymous Chat - Final Project

In this project we implement an anonymous chat messaging service which allows two clients to chat
privately with each other, without leaking information about the contents of their messages or
their identities to other clients or any servers. In addition, the chat service is tolerant to omission faults so long as at least half of the servers are not faulty. (The number of faulty clients affects the size of the anonymity set)


## Installing and Running

This project was tested on Windows & Linux, compiled via Rust 1.50 and running the client requires a graphical environment.

1. If you don't have Rust, install the latest stable version. The recommended way of installing Rust is via [Rustup](https://rustup.rs/)

2. Compile the project by running `cargo build --release` 
   Note, there are major performance differences between the debug and release builds, and this effect is multiplied
   when you run multiple instances of a server. Running in debug build requires higher timeouts, otherwise there'd be no progress
   at all (servers wouldn't be able to process shares fast enough before having to proceed to the next round)

3. Generating keys & certificates:
   - Run `cargo run --release --bin cert_gen --num_servers 5 --num_clients 50` to generate
     public & private key-pairs for 5 servers and 50 clients   
     
   - For TLS, run 
   
     ```shell
     cd certs
     ./gen.sh
     ```

     (this requires a linux environment & an up-to-date openssh installation)

     to generate a self signed CA and use it to create keys & certificates for the servers


     Alternatively, you can use the `--insecure` flag in the runner & run scripts(see below) to disable TLS.

4. To run either a client or a server, see documentation under
   `cargo run --release --bin runner -- --help`

   (All arguments to the chat client/server appear after `runner -- `, the arguments beore that belong to the `cargo` program)

   There are also scripts for Windows & Linux which start several servers and clients :

   - On Windows,  [Window Terminal](https://www.microsoft.com/en-us/p/windows-terminal/9n0dx20hk701) is required, simply compile the project and then run `./start_servers.ps1` from the project root

   - On Linux, [tmux](https://github.com/tmux/tmux/wiki/Installing) is required, simply compile the project and then run `./start_servers.sh` from the project root.


## Documents

- See [this diagram](DistProjDiagram.pdf) which summarizes the design of
  the program, or the below section

- See [this file](Project.pdf) for a more detailed introduction and
  theoretical analysis

From previous exercises:
   - For recap of Raft implementation, see [this file](src/consensus/README.md)

   - For recap of crash tolerant anonymous, public chat service, see [this file](src/anonymity/README.md) - not very accurate or relevant now.

## Design & Implementation recap

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
   forwarding messages between clients, and use secret-sharing in order to anonymize them
