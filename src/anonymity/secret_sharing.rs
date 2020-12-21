use rand::distributions::{Distribution, Uniform, Standard};
use std::ops::{Add, Sub, Mul, Div};
use std::num::Wrapping;
use std::collections::BTreeSet;
use num_traits::*;
use gridiron::fp_256::Fp256;
use rand::seq::SliceRandom;


// A finite field with 256 bits
type FP = Fp256;

/// A secret share
pub struct Share {
    x: FP,
    p_x: FP
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
fn create_share(secret: FP, k: u32, n: u32) -> Vec<Share> {
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
fn reconstruct_secret(shares: &[Share], k: u32) -> FP {
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

}