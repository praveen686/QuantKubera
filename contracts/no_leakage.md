# No-Leakage Contract (Phase 0)

This repository must satisfy the following invariants for any training/evaluation pipeline.

1. **Split-first**: dataset is split into train/valid/test prior to any scaling, normalization, or feature aggregation.
2. **Train-only scalers**: any transform with learned parameters (mean/std, PCA, vocab, etc.) is fit on train only.
3. **Causal features**: features at decision time `t` may depend only on data with timestamps `<= t`.
4. **No oracle teachers**: the execution core and environment must not read future prices/LOB states to generate labels or Q-values.
5. **Deterministic replay**: given identical WAL + seed, the engine must reproduce identical fills and accounting results.
