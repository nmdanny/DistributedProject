/// A small wrapper over common cryptographic operations

use rand::Rng;
use serde::{Serialize, Deserialize};

use sodiumoxide::crypto::box_::{NONCEBYTES, Nonce, PrecomputedKey, PublicKey, SecretKey};
use sodiumoxide::crypto::box_;
use sodiumoxide::crypto::sealedbox;



use crate::consensus::types::Id;

pub use super::setup::*;

#[derive(Serialize, Deserialize, PartialEq, Eq, Clone)]
pub struct HybEncrypted {
    ciphertext: Vec<u8>,
    nonce: Vec<u8>
}

#[derive(Serialize, Deserialize, PartialEq, Eq, Clone)]
pub struct AsymEncrypted {
    ciphertext: Vec<u8>
}


/// Hybrid encryption: essentilly performs symmetric encryption using a key obtained via PKI
pub fn hyb_encrypt<A: AsRef<[u8]>>(precomputed: &PrecomputedKey, plaintext: A) -> HybEncrypted {
    let nonce = box_::gen_nonce();
    let ciphertext = box_::seal_precomputed(plaintext.as_ref(), &nonce, precomputed);
    HybEncrypted {
        nonce: nonce.as_ref().to_vec(), ciphertext
    }

}

/// Hybrid encryption: essentilly performs symmetric encryption using a key obtained via PKI
pub fn hyb_decrypt(precomputed: &PrecomputedKey, encrypted: &HybEncrypted) -> Option<Vec<u8>> {
    let nonce = Nonce::from_slice(&encrypted.nonce).expect("Given nonce length is invalid");
    box_::open_precomputed(&encrypted.ciphertext, &nonce, precomputed).ok()
}

/// Performs assymetric encryption, maintaining anonymity of the sender
pub fn asym_encrypt<A: AsRef<[u8]>>(pkey: &PublicKey, plaintext: A) -> AsymEncrypted {
    let ciphertext = sealedbox::seal(plaintext.as_ref(), pkey);
    AsymEncrypted {
        ciphertext
    }
}

/// Performs assymetric decryption, without knowing the sender
pub fn asym_decrypt(my_skey: &SecretKey, my_pkey: &PublicKey, encrypted: &AsymEncrypted) -> Option<Vec<u8>> {
    sealedbox::open(&encrypted.ciphertext, my_pkey, my_skey).ok()
}

#[cfg(test)]
mod tests {
    use super::*;

    use sodiumoxide::crypto::box_::{gen_keypair, precompute};
    #[test]
    fn hyb_encrypt_decrypt() {
        let (a_pkey, a_skey) = gen_keypair();
        let (b_pkey, b_skey) = gen_keypair();

        let a_precomp = precompute(&b_pkey, &a_skey);
        let b_precomp = precompute(&a_pkey, &b_skey);

        assert_eq!(a_precomp, b_precomp);

        let enc = hyb_encrypt(&a_precomp, b"shalom");
        let dec = hyb_decrypt(&b_precomp, &enc);

        assert_eq!(dec, Some(b"shalom".to_vec()));
    }

    #[test]
    fn asym_encrypt_decrypt() {
        let (a_pkey, a_skey) = gen_keypair();

        let enc = asym_encrypt(&a_pkey, b"shalom");
        let dec = asym_decrypt(&a_skey, &a_pkey, &enc);

        assert_eq!(dec, Some(b"shalom".to_vec()))
    }
}