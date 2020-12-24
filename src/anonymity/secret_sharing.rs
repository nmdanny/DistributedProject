use rand::distributions::{Distribution, Uniform, Standard};
use std::ops::{Add, Sub, Mul, Div};
use std::num::Wrapping;
use std::collections::BTreeSet;
use num_traits::*;
use gridiron::fp_256::Fp256;
use rand::seq::SliceRandom;
use serde::{Serialize, Deserialize};
use serde::ser::{Serializer, SerializeStruct};
use serde::de::{self, DeserializeOwned, Deserializer, Visitor, SeqAccess, MapAccess};
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hasher, Hash};
use std::convert::TryFrom;

// A finite field with 256 bits
pub type FP = Fp256;


/// A secret share
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Share {
    pub x: FP,
    pub p_x: FP
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ShareBytes {
    x: [u8; gridiron::fp_256::PRIMEBYTES],
    p_x: [u8; gridiron::fp_256::PRIMEBYTES],
}

impl Share {
    /// Creates a share for 'x' using the 
    pub fn new(x: u32) -> Share {
        Share {
            x: x.into(),
            p_x: 0u32.into()
        }
    }

    pub fn to_bytes(&self) -> ShareBytes {
        let x = self.x.to_bytes_array();
        let p_x = self.p_x.to_bytes_array();
        ShareBytes {
            x, p_x
        }
    }
}


impl ShareBytes {
    pub fn to_share(&self) -> Share {
        Share {
            x: self.x.into(),
            p_x: self.p_x.into()
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
    fn encode_secret(secret: FP, deg: u32) -> Polynomial {
        // note that ThreadRng is cryptographically secure(an important distinction)
        let rng = rand::thread_rng();
        let pol_coef = std::iter::once(secret).chain(
            Standard.sample_iter(rng).take(deg as usize).map(|b: [u8; 32]| b.into())).collect::<Vec<_>>();
        Polynomial { pol_coef }
    }

    /// Evaluates the polynomial at x, returning the result
    fn evaluate(&self, x: impl Into<FP>) -> FP {
        let x = x.into();
        return (0 .. ).into_iter().zip(self.pol_coef.iter()).fold(0u32.into(), |acc, (i, ci)| {
            acc + (*ci) * x.pow(i)
        })
    }

}

/// Splits the secret into `n` shares, needing `k` shares to re-construct
pub fn create_share(secret: FP, k: u32, n: u32) -> Vec<Share> {
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
pub fn reconstruct_secret(shares: &[Share], k: u32) -> FP {
    assert!(shares.len() >= k as usize, "need at least k different points");
    assert_eq!(shares.iter().map(|s| s.x).collect::<BTreeSet<_>>().len(), shares.len(),
               "all shares must have unique 'x' values");

    // perform langrange interpolation for p(0), where p is a polynomial of degree 'k-1'
    let mut result = FP::zero();

    for (j, Share { x: xj, p_x: p_xj}) in (0..).into_iter().zip(shares.iter().take(k as usize)) {
        let mut el_j = FP::one();
        for m in (0 .. k).into_iter().filter(|&m| m != j) {
            let xm = shares[m as usize].x;
            el_j *= xm/(xm - *xj);
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
    if bytes.len() > 31 {
        return Err(anyhow::anyhow!("Need more than 31 bytes to safely store value, does not fit into secret"));
    }

    let mut secret_bytes = [0u8; 32];
    &secret_bytes[ .. bytes.len()].copy_from_slice(&bytes);

    let fp = FP::from(secret_bytes);
    return Ok(fp)

}

pub fn encode_zero_secret() -> FP {
    0u32.into()
}

/// Given a secret(or what we suspect is a secret), tries to deserialize its contents
/// and return the resulting value. If no value is contained(e.g, no client sent a value), None is returned
///
/// Note, if the secret is the result of a collision, then it's likely that de-serialization will fail(depends
/// on the format) - if not, then it's extremely likely that hash check will fail. If not, you should buy a lottery ticket
pub fn decode_secret<S: DeserializeOwned + Hash>(secret: FP) -> Result<Option<S>, anyhow::Error> {

    if secret == 0u32.into() {
        return Ok(None)
    }

    let bytes = secret.to_bytes_array();

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
        let p = Polynomial { pol_coef: [5u32, 0u32, 1u32, 0u32, 2u32 ].iter().copied().map(Into::into).collect()};
        assert_eq!(p.evaluate(0u32), 5u32.into());
        assert_eq!(p.evaluate(1u32), 8u32.into()); // 5 + 1 + 2*1
        assert_eq!(p.evaluate(2u32), 41u32.into()); // 5 + 2 + 2*16
    }

    // #[test]
    fn test_secret_share(k: u32, n: u32, secret: FP) {
        let mut shares = create_share(secret, k, n);

        shares.shuffle(&mut rand::thread_rng());

        let recons = reconstruct_secret(&shares[.. k as usize], k);
        assert_eq!(secret, recons, "reconstructed value should be equal");
    }

    #[test]
    fn test_secret_shares() {
        for (k, n) in &[(2, 3), (5, 10), (20, 22)] {
            for secret in &[
                0u32, 1, 100, 1337, 420, 0xCAFEBABE
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
            x: 1337u32.into(),
            p_x: 35125135u32.into()
        };
        let bytes = share.to_bytes();
        let share2 = bytes.to_share();
        assert_eq!(share, share2); 
    }
}