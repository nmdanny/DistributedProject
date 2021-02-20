use clap::Clap;
use dist_lib::crypto::{KeyType, Peer, path_for_key};
use sodiumoxide::crypto::box_::gen_keypair;

#[derive(Clap, Clone)]
struct CLI {
    
    #[clap(short = 's', long = "num_servers", about = "Number of servers to generate certs for")]
    pub num_servers: usize,

    #[clap(short = 'c', long = "num_clients", about = "Number of clients to generate certs for")]
    pub num_clients: usize,
}


fn main() -> Result<(), anyhow::Error> {
    let options = CLI::parse();

    for i in 0 .. options.num_servers {
        let (pkey, skey) = gen_keypair();
        std::fs::write(path_for_key(Peer::Server, KeyType::Public, i), pkey.as_ref())?;
        std::fs::write(path_for_key(Peer::Server, KeyType::Private, i), skey.as_ref())?;
    }

    for i in 0 .. options.num_clients {
        let (pkey, skey) = gen_keypair();
        std::fs::write(path_for_key(Peer::Client, KeyType::Public, i), pkey.as_ref())?;
        std::fs::write(path_for_key(Peer::Client, KeyType::Private, i), skey.as_ref())?;

    }



    Ok(())
}