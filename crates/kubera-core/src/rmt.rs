//! Random Matrix Theory utilities for expert correlation structure.
//!
//! This module provides small, dependency-free routines tailored to low-dimensional
//! correlation/covariance matrices (number of experts is typically <= 8-12).
//!
//! Implementations:
//! - Symmetric Jacobi eigen-decomposition (eigenvalues only)
//! - Marčenko–Pastur bounds (bulk edge)
//! - Effective rank / participation ratio
//! - Simple weight shrinkage toward uniform when correlation structure is poor/noisy

use std::f64;

#[derive(Debug, Clone)]
pub struct RmtSummary {
    pub n: usize,
    pub t: usize,
    pub q: f64,
    pub lambda_max_mp: f64,
    pub eigenvalues: Vec<f64>,
    pub effective_rank: f64,
}

/// Compute Marčenko–Pastur bulk maximum eigenvalue for a correlation matrix.
///
/// For standardized series with variance 1, eigenvalues lie in:
/// [ (1 - sqrt(1/q))^2, (1 + sqrt(1/q))^2 ] where q = T/N and q >= 1.
///
/// If q < 1 or insufficient data, returns None.
pub fn marcenko_pastur_lambda_max(q: f64) -> Option<f64> {
    if !(q.is_finite()) || q < 1.0 {
        return None;
    }
    let s = (1.0 / q).sqrt();
    Some((1.0 + s) * (1.0 + s))
}

/// Jacobi eigenvalue algorithm for symmetric matrices (eigenvalues only).
///
/// `a` is an NxN symmetric matrix stored row-major in a flat Vec.
/// Returns eigenvalues (unsorted).
pub fn jacobi_eigenvalues(mut a: Vec<f64>, n: usize, max_sweeps: usize, eps: f64) -> Vec<f64> {
    if n == 0 {
        return vec![];
    }
    let idx = |i: usize, j: usize| -> usize { i * n + j };

    for _ in 0..max_sweeps {
        // Find largest off-diagonal element
        let mut p = 0usize;
        let mut q = 1usize;
        let mut max = 0.0f64;

        for i in 0..n {
            for j in (i + 1)..n {
                let v = a[idx(i, j)].abs();
                if v > max {
                    max = v;
                    p = i;
                    q = j;
                }
            }
        }

        if max < eps {
            break;
        }

        let app = a[idx(p, p)];
        let aqq = a[idx(q, q)];
        let apq = a[idx(p, q)];

        let phi = 0.5 * (aqq - app) / apq;
        let t = 1.0 / (phi.abs() + (1.0 + phi * phi).sqrt());
        let t = if phi < 0.0 { -t } else { t };
        let c = 1.0 / (1.0 + t * t).sqrt();
        let s = t * c;

        // Rotate
        for k in 0..n {
            if k != p && k != q {
                let aik = a[idx(k, p)];
                let akq = a[idx(k, q)];
                a[idx(k, p)] = c * aik - s * akq;
                a[idx(p, k)] = a[idx(k, p)];
                a[idx(k, q)] = s * aik + c * akq;
                a[idx(q, k)] = a[idx(k, q)];
            }
        }

        let app_new = c * c * app - 2.0 * s * c * apq + s * s * aqq;
        let aqq_new = s * s * app + 2.0 * s * c * apq + c * c * aqq;

        a[idx(p, p)] = app_new;
        a[idx(q, q)] = aqq_new;
        a[idx(p, q)] = 0.0;
        a[idx(q, p)] = 0.0;
    }

    (0..n).map(|i| a[i * n + i]).collect()
}

/// Compute effective rank (participation ratio) given eigenvalues.
pub fn effective_rank(eigs: &[f64]) -> f64 {
    if eigs.is_empty() {
        return 0.0;
    }
    let sum: f64 = eigs.iter().cloned().sum();
    if sum <= 0.0 {
        return 0.0;
    }
    let sumsq: f64 = eigs.iter().map(|v| v * v).sum();
    if sumsq <= 0.0 {
        return 0.0;
    }
    (sum * sum) / sumsq
}

/// Build correlation matrix from aligned return series.
///
/// Input: returns[k][t] where k=expert index. Series should be same length >= 2.
/// Returns NxN correlation matrix row-major, with diagonal=1.
pub fn correlation_matrix(returns: &[Vec<f64>]) -> Option<(Vec<f64>, usize, usize)> {
    let n = returns.len();
    if n < 2 {
        return None;
    }
    let t = returns[0].len();
    if t < 3 {
        return None;
    }
    if !returns.iter().all(|r| r.len() == t) {
        return None;
    }

    let mut mean = vec![0.0f64; n];
    for i in 0..n {
        mean[i] = returns[i].iter().sum::<f64>() / (t as f64);
    }
    let mut var = vec![0.0f64; n];
    for i in 0..n {
        let mut v = 0.0;
        for x in &returns[i] {
            let d = x - mean[i];
            v += d * d;
        }
        var[i] = v / ((t - 1) as f64);
    }
    if var.iter().any(|v| !v.is_finite() || *v <= 0.0) {
        return None;
    }

    let idx = |i: usize, j: usize| -> usize { i * n + j };
    let mut c = vec![0.0f64; n * n];
    for i in 0..n {
        c[idx(i, i)] = 1.0;
        for j in (i + 1)..n {
            let mut cov = 0.0;
            for tt in 0..t {
                cov += (returns[i][tt] - mean[i]) * (returns[j][tt] - mean[j]);
            }
            cov /= (t - 1) as f64;
            let corr = cov / (var[i].sqrt() * var[j].sqrt());
            let corr = corr.clamp(-1.0, 1.0);
            c[idx(i, j)] = corr;
            c[idx(j, i)] = corr;
        }
    }
    Some((c, n, t))
}

/// Summarize correlation structure using RMT and MP bounds.
pub fn summarize_rmt(returns: &[Vec<f64>]) -> Option<RmtSummary> {
    let (c, n, t) = correlation_matrix(returns)?;
    let q = (t as f64) / (n as f64);
    let lambda_max_mp = marcenko_pastur_lambda_max(q)?;
    let eigs = jacobi_eigenvalues(c, n, 64, 1e-10);
    let er = effective_rank(&eigs);
    Some(RmtSummary {
        n,
        t,
        q,
        lambda_max_mp,
        eigenvalues: eigs,
        effective_rank: er,
    })
}

/// Shrink weights toward uniform when effective rank is low (high correlation).
///
/// `min_frac` is minimum acceptable effective-rank fraction vs N (e.g., 0.65).
/// `max_alpha` caps shrinkage strength.
/// Returns (new_weights, alpha_used).
pub fn shrink_weights_toward_uniform(
    weights: &[f64],
    effective_rank: f64,
    min_frac: f64,
    max_alpha: f64,
) -> (Vec<f64>, f64) {
    let n = weights.len();
    if n == 0 {
        return (vec![], 0.0);
    }
    let target = (n as f64) * min_frac.clamp(0.0, 1.0);
    if effective_rank >= target {
        return (weights.to_vec(), 0.0);
    }

    let gap = (target - effective_rank) / target.max(1e-9);
    let alpha = gap.clamp(0.0, 1.0).min(max_alpha.clamp(0.0, 1.0));
    let uni = 1.0 / (n as f64);

    let mut out: Vec<f64> = weights.iter().map(|w| (1.0 - alpha) * *w + alpha * uni).collect();
    // re-normalize
    let s: f64 = out.iter().sum();
    if s > 0.0 {
        for w in &mut out {
            *w /= s;
        }
    }
    (out, alpha)
}
