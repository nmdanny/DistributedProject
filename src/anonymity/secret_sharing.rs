use anyhow::Context;
use std::ops::{Add, Sub, Mul, Div};
use std::collections::BTreeSet;
use rand::seq::SliceRandom;
use serde::{Serialize, Deserialize};
use serde::ser::{Serializer, SerializeStruct};
use serde::de::{self, DeserializeOwned, Deserializer, Visitor, SeqAccess, MapAccess};
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hasher, Hash};
use std::convert::TryFrom;
use derivative;
use curve25519_dalek::scalar::Scalar;


// A finite field with 256 bits
pub type FP = Scalar;


/// A secret share
#[derive(Derivative, Clone, PartialEq, Eq)]
#[derivative(Debug)]
pub struct Share {
    pub x: u64,

    #[derivative(Debug = "ignore")]
    pub p_x: FP
}

#[derive(Derivative, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[derivative(Debug)]
pub struct ShareBytes {
    x: u64,

    #[derivative(Debug = "ignore")]
    p_x: [u8; 32],
}

impl Share {
    /// Creates a share for 'x' using the 
    pub fn new(x: impl Into<u64>) -> Share {
        let x: u64 = x.into();
        Share {
            x: x,
            p_x: 0u64.into()
        }
    }

    pub fn to_bytes(&self) -> ShareBytes {
        let p_x = self.p_x.to_bytes();
        ShareBytes {
            x: self.x, p_x
        }
    }
}


impl ShareBytes {
    pub fn to_share(&self) -> Share {
        let p_x = FP::from_bytes_mod_order(self.p_x.clone());
        Share {
            x: self.x,
            p_x
        }
    }
}

/// Represents a polynomial 'f(x) = c0 + c1*x + c2*x^2 + ....' 
struct Polynomial {
    // Coefficients c0, c1, c2, ...
    pol_coef: Vec<FP>
}

impl Polynomial {
    /// Creates a polynomial whose value is `secret` at the Y intersection,
    /// and has `deg` 
    fn encode_secret(secret: FP, deg: u64) -> Polynomial {
        // note that ThreadRng is cryptographically secure(an important distinction)
        let rng = &mut rand::thread_rng();
        let mut pol_coef = Vec::new();
        pol_coef.push(secret);
        for _ in 1 ..= deg
        {
            pol_coef.push(FP::random(rng));
        }
        Polynomial { pol_coef }
    }

    /// Evaluates the polynomial at x, returning the result
    fn evaluate(&self, x: impl Into<FP>) -> FP {
        let x = x.into();
        return (0 .. ).into_iter().zip(self.pol_coef.iter()).fold(0u64.into(), |acc, (i, ci)| {
            let mut exp = FP::one();
            for _ in 1 ..= i {
                exp *= x;
            }
            acc + (*ci) * exp
        })
    }

}

/// Splits the secret into `n` shares, needing `k` shares to re-construct
pub fn create_share(secret: FP, k: u64, n: u64) -> Vec<Share> {
    assert!(k <= n, "number of shares can't be bigger than n");
    assert!(k >= 1, "number of shares must be at least 1");

    // A polynomial of degree 'k-1' requires at least 'k' points to re-construct
    let pol = Polynomial::encode_secret(secret, k-1);
    (1 ..= n).into_iter().map(|x| {
        let p_x = pol.evaluate(x);
        Share {
            x: x.into(),
            p_x
        }
    }).collect()
}

/// Reconstructs a secret from a given list of secret shares, assuming secret was split to 'k' different shares
pub fn reconstruct_secret(shares: &[Share], k: u64) -> FP {
    assert!(shares.len() >= k as usize, "need at least k different points");
    assert_eq!(shares.iter().map(|s| s.x).collect::<BTreeSet<_>>().len(), shares.len(),
               "all shares must have unique 'x' values");

    // perform langrange interpolation for p(0), where p is a polynomial of degree 'k-1'
    let mut result = FP::zero();

    for (j, Share { x: xj, p_x: p_xj}) in (0..).into_iter().zip(shares.iter().take(k as usize)) {
        let mut el_j = FP::one();
        for m in (0 .. k).into_iter().filter(|&m| m != j) {
            let xm = FP::from(shares[m as usize].x);
            let xj = FP::from(*xj);
            el_j *= xm * (xm - xj).invert();
        }
        result += (*p_xj) * el_j;
    }
    return result;
}

#[derive(Serialize, Deserialize)]
struct ValueAndHash<V> {
    value: V,
    hash: u64
}

pub fn encode_secret<S: Serialize + Hash>(data: S) -> Result<FP, anyhow::Error> {
    let mut value_and_hash = ValueAndHash {
        value: data,
        hash: 0
    };
    let mut hasher = DefaultHasher::new();
    value_and_hash.value.hash(&mut hasher);
    value_and_hash.hash = hasher.finish();
    let bytes = bincode::serialize(&value_and_hash)?;
    if bytes.len() > 32 {
        return Err(anyhow::anyhow!("Need more than 31 bytes to safely store value, does not fit into secret"));
    }

    let mut bytes_arr = [0u8; 32];
    bytes_arr[ .. bytes.len()].copy_from_slice(&bytes);

    let secret = FP::from_canonical_bytes(bytes_arr).context(
        "Couldn't create secret from value & hash, encoded value is probably too big"
    )?;

    return Ok(secret)

}

pub fn encode_zero_secret() -> FP {
    0u64.into()
}

/// Given a secret(or what we suspect is a secret), tries to deserialize its contents
/// and return the resulting value. If no value is contained(e.g, no client sent a value), None is returned
///
/// Note, if the secret is the result of a collision, then it's likely that de-serialization will fail(depends
/// on the format) - if not, then it's extremely likely that hash check will fail. If not, you should buy a lottery ticket
pub fn decode_secret<S: DeserializeOwned + Hash>(secret: FP) -> Result<Option<S>, anyhow::Error> {

    if secret == 0u64.into() {
        return Ok(None)
    }

    let bytes = secret.to_bytes();

    let value_and_hash = bincode::deserialize::<ValueAndHash<S>>(&bytes)?;
    let mut hasher = DefaultHasher::new();
    value_and_hash.value.hash(&mut hasher);
    let computed_hash = hasher.finish();
    if computed_hash != value_and_hash.hash {
        return Err(anyhow::anyhow!("Decoded value has wrong hash, it was probably the result of a collision"));
    }
    Ok(Some(value_and_hash.value))
}

pub fn add_shares(share1: &Share, share2: &Share) -> Share {
    assert_eq!(share1.x, share2.x, "Cannot add shares with different X coordinates");
    return Share {
        x: share1.x,
        p_x: share1.p_x + share2.p_x
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_polynomial_evaluate() {
        // 5 + x^2 + 2*x^4
        let p = Polynomial { pol_coef: [5u64, 0u64, 1u64, 0u64, 2u64 ].iter().copied().map(Into::into).collect()};
        assert_eq!(p.evaluate(0u64), 5u64.into());
        assert_eq!(p.evaluate(1u64), 8u64.into()); // 5 + 1 + 2*1
        assert_eq!(p.evaluate(2u64), 41u64.into()); // 5 + 2 + 2*16
    }

    // #[test]
    fn test_secret_share(k: u64, n: u64, secret: FP) {
        let mut shares = create_share(secret, k, n);

        shares.shuffle(&mut rand::thread_rng());

        let recons = reconstruct_secret(&shares[.. k as usize], k);
        assert_eq!(secret, recons, "reconstructed value should be equal");
    }

    #[test]
    fn test_secret_shares() {
        for (k, n) in &[(2, 3), (5, 10), (20, 22)] {
            for secret in &[
                0u64, 1, 100, 1337, 420, 0xCAFEBABE
            ] {
                test_secret_share(*k, *n, FP::from(*secret));
            }
        }
    }

    #[test]
    fn can_encode_and_decode_secret() {
        let secret = encode_secret("shalom").unwrap();
        let decoded = decode_secret::<String>(secret).unwrap();
        assert_eq!(decoded, Some("shalom".into()), "decode mismatch");
    }


    #[test]
    fn decode_flow_no_collision() {
        let secret_a = encode_secret("shalom").unwrap();
        let secret_b = encode_secret("bye").unwrap();

        let zero_secret = encode_zero_secret();

        // say A wants to send over channel A (and 0 over rest)
        let client_a_chan_a = create_share(secret_a, 2, 2);
        let client_a_chan_b = create_share(zero_secret, 2, 2);

        // and B wants to send over channel B (and 0 over rest)

        let client_b_chan_a = create_share(zero_secret, 2, 2);
        let client_b_chan_b = create_share(secret_b, 2, 2);

        // note that both server A and B would have an idetnical view of the following share vector by now
        let server_chan_a_shares = vec![
            add_shares(&client_a_chan_a[0], &client_b_chan_a[0]),
            add_shares(&client_a_chan_a[1], &client_b_chan_a[1]),
        ];

        let server_chan_a_decoded_val = decode_secret::<String>(reconstruct_secret(server_chan_a_shares.as_slice(), 2)).unwrap();
        assert_eq!(server_chan_a_decoded_val, Some("shalom".into()), "decoded value mismatch");

        
        let server_chan_b_shares = vec![
            add_shares(&client_a_chan_b[0], &client_b_chan_b[0]),
            add_shares(&client_a_chan_b[1], &client_b_chan_b[1]),
        ];

        let sever_chan_b_decoded_val = decode_secret::<String>(reconstruct_secret(server_chan_b_shares.as_slice(), 2)).unwrap();
        assert_eq!(sever_chan_b_decoded_val, Some("bye".into()), "decoded value mismatch");
    }

    #[test]
    fn decode_flow_with_collision() {
        let secret_a = encode_secret("shalom").unwrap();
        let secret_b = encode_secret("bye").unwrap();

        let zero_secret = encode_zero_secret();

        // say A wants to send over channel A (and 0 over rest)
        let client_a_chan_a = create_share(secret_a, 2, 2);
        let client_a_chan_b = create_share(zero_secret, 2, 2);

        // and B wants to send over channel A as well

        let client_b_chan_a = create_share(secret_b, 2, 2);
        let client_b_chan_b = create_share(zero_secret, 2, 2);


        // note that both server A and B would have an idetnical view of the following share vector by now
        let server_chan_a = vec![
            add_shares(&client_a_chan_a[0], &client_b_chan_a[0]),
            add_shares(&client_a_chan_a[1], &client_b_chan_a[1]),
        ];


        let _expected_err = decode_secret::<String>(reconstruct_secret(server_chan_a.as_slice(), 2)).unwrap_err();

        
        let server_chan_b_shares = vec![
            add_shares(&client_a_chan_b[0], &client_b_chan_b[0]),
            add_shares(&client_a_chan_b[1], &client_b_chan_b[1]),
        ];

        let server_chan_b_decoded_val = decode_secret::<String>(reconstruct_secret(server_chan_b_shares.as_slice(), 2)).unwrap();
        assert_eq!(server_chan_b_decoded_val, None, "channel B should have no value");
    }

    #[test]
    fn share_bytes_conversion_works() {
        let share = Share {
            x: 1337u64,
            p_x: 35125135u64.into()
        };
        let bytes = share.to_bytes();
        let share2 = bytes.to_share();
        assert_eq!(share, share2); 
        assert_eq!(share.x, 1337u64);
    }
}