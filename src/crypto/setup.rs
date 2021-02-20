use anyhow::Context;
use sodiumoxide::crypto::box_::{PrecomputedKey, PublicKey, SecretKey, precompute};

use crate::{anonymity::logic::Config, consensus::types::Id};
use std::{fmt::Debug, path::{Path, PathBuf}};


#[derive(Debug, Clone)]
pub struct PKISettings {
    pub clients_keys: Vec<(PublicKey, PrecomputedKey)>,
    pub servers_keys: Vec<(PublicKey, PrecomputedKey)>,
    pub my_key: SecretKey,
    pub my_pkey: PublicKey
}

pub fn read_public_key<A: AsRef<Path>>(path: A) -> PublicKey {
    let buf = std::fs::read(&path).context(format!("while reading public key from path {:?}", path.as_ref())).unwrap();
    PublicKey::from_slice(&buf).expect("Public key has wrong length")
}

pub fn read_private_key<A: AsRef<Path>>(path: A) -> SecretKey {
    let buf = std::fs::read(&path).context(format!("while reading private key from path {:?}", path.as_ref())).unwrap();
    SecretKey::from_slice(&buf).expect("Secret key has wrong length")
}

pub struct PKIBuilder {
    clients_keys: Vec<PublicKey>,
    servers_keys: Vec<PublicKey>,
    my_skey: Option<SecretKey>,
    my_pkey: Option<PublicKey>
}



#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Peer {
    Client, Server
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum KeyType {
    Public, Private
}

/// Returns path where a key(public/private) for a peer(client/server) with given ID should be at
pub fn path_for_key(who: Peer, key_type: KeyType, id: Id) -> PathBuf {
    let certs_folder = Path::new(env!("CARGO_MANIFEST_DIR")).join("certs");
    let client_folder = certs_folder.join("client");
    let server_folder = certs_folder.join("server");
    std::fs::create_dir_all(&client_folder).unwrap();
    std::fs::create_dir_all(&server_folder).unwrap();
    match (who, key_type) {
        (Peer::Client, KeyType::Public) =>   client_folder.join(format!("{}.public", id)),
        (Peer::Client, KeyType::Private) =>  client_folder.join(format!("{}.private", id)),
        (Peer::Server, KeyType::Public) =>   server_folder.join(format!("{}.public", id)),
        (Peer::Server, KeyType::Private) =>  server_folder.join(format!("{}.private", id)),
    }
}

impl PKIBuilder {
    pub fn new(num_servers: usize, num_clients: usize) -> Self {
        let servers_keys = (0 .. num_servers).map(|server_id| {
            read_public_key(path_for_key(Peer::Server, KeyType::Public, server_id))
        }).collect();

        let clients_keys = (0 .. num_clients).map(|client_id| {
            read_public_key(path_for_key(Peer::Client, KeyType::Public, client_id))
        }).collect();

        PKIBuilder {
            servers_keys, clients_keys, my_skey: None, my_pkey: None
        }
    }

    pub fn for_server(mut self, server_id: Id) -> Self {
        self.my_skey = Some(read_private_key(path_for_key(Peer::Server, KeyType::Private, server_id)));
        self.my_pkey = Some(read_public_key(path_for_key(Peer::Server, KeyType::Public, server_id)));
        assert_eq!(&self.my_skey.as_ref().unwrap().public_key(), self.my_pkey.as_ref().unwrap(), 
                   "Mismatch between my published public key and the one derived from the private key");
        self
    }

    pub fn for_client(mut self, client_id: Id) -> Self {
        self.my_skey = Some(read_private_key(path_for_key(Peer::Client, KeyType::Private, client_id)));
        self.my_pkey = Some(read_public_key(path_for_key(Peer::Client, KeyType::Public, client_id)));
        assert_eq!(&self.my_skey.as_ref().unwrap().public_key(), self.my_pkey.as_ref().unwrap(), 
                   "Mismatch between my published public key and the one derived from the private key");
        self
    }

    pub fn build(&mut self) -> PKISettings {
        let my_skey = self.my_skey.clone().expect("Must setup a private key");
        let my_pkey = self.my_pkey.clone().expect("Must setup a private key");
        let clients_keys = self.clients_keys.iter().map(|pkey| (pkey.clone(), precompute(pkey, &my_skey))).collect();
        let servers_keys = self.servers_keys.iter().map(|pkey| (pkey.clone(), precompute(pkey, &my_skey))).collect();
        PKISettings {
            servers_keys, clients_keys, my_key: my_skey, my_pkey
        }
    }
}