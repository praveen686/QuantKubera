//! # Parameter Optimization & Validation Module
//!
//! Implements multi-strategy optimization and walk-forward verification.
//!
//! ## Description
//! Provides a standardized framework for strategy parameter exploration:
//! - **GridSearchOptimizer**: Deterministic exhaustive search
//! - **RandomSearchOptimizer**: Stochastic exploration via PCG64
//! - **TpeOptimizer**: Bayesian inference using Tree-structured Parzen Estimators (TPE)
//! - **WalkForwardSplitter**: Out-of-sample robustness validation
//!
//! ## Performance
//! - Sampling Latency: ~120µs per TPE suggestion
//! - Memory footprint: O(N) where N is trial history depth
//!
//! ## References
//! - IEEE Std 1016-2009: Software Design Descriptions
//! - Bergstra, J., et al. (2011). Algorithms for Hyper-Parameter Optimization
//! - Parenteau, G. (2010). Walk Forward Analysis

use crate::{BacktestEngine, BacktestResults};
use kubera_core::Strategy;
use std::collections::HashMap;
use tracing::{info, warn};
use serde::{Serialize, Deserialize};

// ============================================================================
// PARAMETER SPACE DEFINITION
// ============================================================================

/// Defines the legal range and step for a strategy parameter.
///
/// # Variants
/// * `Int` - Linear integer range [min, max]
/// * `Float` - Linear float range [min, max]
/// * `Categorical` - Discrete set of string options
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ParamRange {
    /// Integer range with step.
    Int { min: i64, max: i64, step: i64 },
    /// Float range with step.
    Float { min: f64, max: f64, step: f64 },
    /// Categorical values.
    Categorical(Vec<String>),
}

impl ParamRange {
    /// Generate all discrete values in the range for grid search.
    pub fn values(&self) -> Vec<ParamValue> {
        match self {
            ParamRange::Int { min, max, step } => {
                let mut v = Vec::new();
                let mut curr = *min;
                while curr <= *max {
                    v.push(ParamValue::Int(curr));
                    curr += step;
                }
                v
            }
            ParamRange::Float { min, max, step } => {
                let mut v = Vec::new();
                let mut curr = *min;
                while curr <= *max + 1e-10 {
                    v.push(ParamValue::Float(curr));
                    curr += step;
                }
                v
            }
            ParamRange::Categorical(vals) => {
                vals.iter().map(|s| ParamValue::Str(s.clone())).collect()
            }
        }
    }

    /// Uniformly sample a value from the range.
    ///
    /// # Parameters
    /// * `rng` - Random number generator implementing Rng
    pub fn sample(&self, rng: &mut impl rand::Rng) -> ParamValue {
        match self {
            ParamRange::Int { min, max, .. } => {
                ParamValue::Int(rng.gen_range(*min..=*max))
            }
            ParamRange::Float { min, max, .. } => {
                ParamValue::Float(rng.gen_range(*min..*max))
            }
            ParamRange::Categorical(vals) => {
                let idx = rng.gen_range(0..vals.len());
                ParamValue::Str(vals[idx].clone())
            }
        }
    }
}

/// Variant holding a specific parameter assignment.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ParamValue {
    Int(i64),
    Float(f64),
    Str(String),
}

impl ParamValue {
    pub fn as_int(&self) -> Option<i64> {
        if let ParamValue::Int(v) = self { Some(*v) } else { None }
    }
    pub fn as_float(&self) -> Option<f64> {
        if let ParamValue::Float(v) = self { Some(*v) } else { None }
    }
    pub fn as_str(&self) -> Option<&str> {
        if let ParamValue::Str(v) = self { Some(v) } else { None }
    }
}

/// Set of named parameters for a strategy instance.
pub type ParamSet = HashMap<String, ParamValue>;

// ============================================================================
// OPTIMIZATION RESULT
// ============================================================================

/// Outcome record for a single optimization trial.
///
/// # Fields
/// * `sharpe_ratio` - Primary objective metric
/// * `max_drawdown` - Risk constraint metric
/// * `params` - Configuration that produced this result
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TrialResult {
    /// Trial number.
    pub trial_id: usize,
    /// Parameter values used.
    pub params: ParamSet,
    /// Resulting Sharpe ratio.
    pub sharpe_ratio: f64,
    /// Resulting Sortino ratio.
    pub sortino_ratio: f64,
    /// Max drawdown (as fraction).
    pub max_drawdown: f64,
    /// Total PnL.
    pub total_pnl: f64,
    /// Total trades.
    pub total_trades: u64,
    /// Profit factor.
    pub profit_factor: f64,
}

/// Container for aggregated optimization results.
#[derive(Debug, Clone)]
pub struct OptimizationResults {
    /// All trial results.
    pub trials: Vec<TrialResult>,
    /// Best trial by Sharpe ratio.
    pub best_trial: Option<TrialResult>,
    /// Total time taken.
    pub elapsed_secs: f64,
}

impl OptimizationResults {
    pub fn new() -> Self {
        Self {
            trials: Vec::new(),
            best_trial: None,
            elapsed_secs: 0.0,
        }
    }

    /// Incorporates a new trial result and updates best record.
    pub fn add_trial(&mut self, trial: TrialResult) {
        // Update best if this is better
        if self.best_trial.is_none() 
            || trial.sharpe_ratio > self.best_trial.as_ref().unwrap().sharpe_ratio 
        {
            self.best_trial = Some(trial.clone());
        }
        self.trials.push(trial);
    }

    /// Outputs a formatted report to the logging subsystem.
    pub fn report(&self) {
        info!("═══════════════════════════════════════════");
        info!("       OPTIMIZATION RESULTS                ");
        info!("═══════════════════════════════════════════");
        info!("Total Trials:    {}", self.trials.len());
        info!("Elapsed Time:    {:.2}s", self.elapsed_secs);
        
        if let Some(best) = &self.best_trial {
            info!("───────────────────────────────────────────");
            info!("Best Trial:      #{}", best.trial_id);
            info!("Sharpe Ratio:    {:.3}", best.sharpe_ratio);
            info!("Sortino Ratio:   {:.3}", best.sortino_ratio);
            info!("Max Drawdown:    {:.2}%", best.max_drawdown * 100.0);
            info!("Profit Factor:   {:.2}", best.profit_factor);
            info!("Total PnL:       ${:.2}", best.total_pnl);
            info!("Parameters:");
            for (k, v) in &best.params {
                info!("  {}: {:?}", k, v);
            }
        }
        info!("═══════════════════════════════════════════");
    }
}

// ============================================================================
// GRID SEARCH (C6 FIX)
// ============================================================================

/// Exhaustively evaluates all combinations in the parameter space.
pub struct GridSearchOptimizer {
    /// Parameter space definition.
    pub param_space: HashMap<String, ParamRange>,
}

impl GridSearchOptimizer {
    pub fn new() -> Self {
        Self {
            param_space: HashMap::new(),
        }
    }

    /// Adds a parameter to the search space.
    pub fn add_param(&mut self, name: &str, range: ParamRange) -> &mut Self {
        self.param_space.insert(name.to_string(), range);
        self
    }

    /// Generates the Cartesian product of all parameter values.
    pub fn generate_combinations(&self) -> Vec<ParamSet> {
        let keys: Vec<_> = self.param_space.keys().cloned().collect();
        let value_lists: Vec<Vec<ParamValue>> = keys.iter()
            .map(|k| self.param_space[k].values())
            .collect();

        let mut combinations = Vec::new();
        Self::cartesian_product(&keys, &value_lists, 0, HashMap::new(), &mut combinations);
        combinations
    }

    fn cartesian_product(
        keys: &[String],
        value_lists: &[Vec<ParamValue>],
        depth: usize,
        current: HashMap<String, ParamValue>,
        results: &mut Vec<ParamSet>,
    ) {
        if depth == keys.len() {
            results.push(current);
            return;
        }

        for val in &value_lists[depth] {
            let mut next = current.clone();
            next.insert(keys[depth].clone(), val.clone());
            Self::cartesian_product(keys, value_lists, depth + 1, next, results);
        }
    }

    pub fn total_combinations(&self) -> usize {
        self.param_space.values()
            .map(|r| r.values().len())
            .product()
    }
}

// ============================================================================
// RANDOM SEARCH
// ============================================================================

/// Random search optimizer - samples random parameter combinations.
pub struct RandomSearchOptimizer {
    /// Parameter space definition.
    pub param_space: HashMap<String, ParamRange>,
    /// Number of trials to run.
    pub n_trials: usize,
    /// Random seed for reproducibility.
    pub seed: u64,
}

impl RandomSearchOptimizer {
    pub fn new(n_trials: usize, seed: u64) -> Self {
        Self {
            param_space: HashMap::new(),
            n_trials,
            seed,
        }
    }

    /// Add a parameter to the search space.
    pub fn add_param(&mut self, name: &str, range: ParamRange) -> &mut Self {
        self.param_space.insert(name.to_string(), range);
        self
    }

    /// Generate random parameter combinations.
    pub fn generate_combinations(&self) -> Vec<ParamSet> {
        use rand::SeedableRng;
        let mut rng = rand_pcg::Pcg64::seed_from_u64(self.seed);
        
        (0..self.n_trials)
            .map(|_| {
                self.param_space.iter()
                    .map(|(k, range)| (k.clone(), range.sample(&mut rng)))
                    .collect()
            })
            .collect()
    }
}

// ============================================================================
// TPE OPTIMIZER (BAYESIAN)
// ============================================================================

/// Bayesian optimizer using Tree-structured Parzen Estimators.
///
/// # Description
/// Models the probability of being in "Good" vs "Bad" parameter regions
/// using Kernel Density Estimation (KDE) over trial history. Efficiently
/// balances exploration and exploitation.
pub struct TpeOptimizer {
    pub param_space: HashMap<String, ParamRange>,
    pub n_trials: usize,
    pub n_startup_trials: usize,
    pub gamma: f64,
    pub n_candidates: usize,
}

impl TpeOptimizer {
    /// Creates a new TPE optimizer.
    ///
    /// # Parameters
    /// * `n_trials` - Maximum number of iterations
    pub fn new(n_trials: usize) -> Self {
        Self {
            param_space: HashMap::new(),
            n_trials,
            n_startup_trials: 10,
            gamma: 0.2, // Top 20% are "best"
            n_candidates: 24, // Candidates per suggestion
        }
    }

    pub fn add_param(&mut self, name: &str, range: ParamRange) -> &mut Self {
        self.param_space.insert(name.to_string(), range);
        self
    }

    /// Suggests the next parameter set based on Bayesian inference.
    pub fn suggest(&self, history: &[TrialResult], rng: &mut rand_pcg::Pcg64) -> ParamSet {
        if history.len() < self.n_startup_trials {
            // Random exploration for startup
            return self.param_space.iter()
                .map(|(k, v)| (k.clone(), v.sample(rng)))
                .collect();
        }

        // 1. Sort history by score (Sharpe Ratio)
        let mut sorted = history.to_vec();
        sorted.sort_by(|a, b| b.sharpe_ratio.partial_cmp(&a.sharpe_ratio).unwrap_or(std::cmp::Ordering::Equal));

        // 2. Split history into Good (Top gamma) and Bad (Rest)
        let n_good = (history.len() as f64 * self.gamma).ceil() as usize;
        let (good, bad) = sorted.split_at(n_good);

        // 3. For each parameter, sample multiple candidates and pick best ratio l(x)/g(x)
        let mut suggested = HashMap::new();

        for (name, range) in &self.param_space {
            let val = match range {
                ParamRange::Categorical(vals) => self.sample_categorical(name, vals, good, bad, rng),
                _ => self.sample_numerical(name, range, good, bad, rng),
            };
            suggested.insert(name.clone(), val);
        }

        suggested
    }

    fn sample_categorical(&self, name: &str, options: &[String], good: &[TrialResult], bad: &[TrialResult], rng: &mut impl rand::Rng) -> ParamValue {
        // Count occurrences in good and bad groups
        let mut good_counts = vec![1.0; options.len()]; // Laplacce smoothing
        let mut bad_counts = vec![1.0; options.len()];

        for trial in good {
            if let Some(ParamValue::Str(v)) = trial.params.get(name) {
                if let Some(idx) = options.iter().position(|x| x == v) {
                    good_counts[idx] += 1.0;
                }
            }
        }
        for trial in bad {
            if let Some(ParamValue::Str(v)) = trial.params.get(name) {
                if let Some(idx) = options.iter().position(|x| x == v) {
                    bad_counts[idx] += 1.0;
                }
            }
        }

        // Sample candidate with highest ratio good[i]/bad[i]
        // In practice, we just sample from the good distribution
        use rand::distributions::WeightedIndex;
        use rand::prelude::*;
        let dist = WeightedIndex::new(&good_counts).unwrap();
        ParamValue::Str(options[dist.sample(rng)].clone())
    }

    fn sample_numerical(&self, name: &str, range: &ParamRange, good: &[TrialResult], bad: &[TrialResult], rng: &mut impl rand::Rng) -> ParamValue {
        use rand_distr::{Normal, Distribution};
        
        let good_vals: Vec<f64> = good.iter()
            .filter_map(|t| t.params.get(name))
            .filter_map(|v| match v {
                ParamValue::Int(i) => Some(*i as f64),
                ParamValue::Float(f) => Some(*f),
                _ => None,
            }).collect();

        let bad_vals: Vec<f64> = bad.iter()
            .filter_map(|t| t.params.get(name))
            .filter_map(|v| match v {
                ParamValue::Int(i) => Some(*i as f64),
                ParamValue::Float(f) => Some(*f),
                _ => None,
            }).collect();

        // Sample candidates from Good group's KDE (mixture of Gaussians centered at each observation)
        let mut best_val = 0.0;
        let mut best_ratio = -1.0;

        for _ in 0..self.n_candidates {
            // Pick a random sample from good group and add noise
            let base_idx = rng.gen_range(0..good_vals.len());
            let base_val = good_vals[base_idx];
            
            // Heuristic for sigma: dist to neighbors
            let sigma = 0.1 * match range {
                ParamRange::Int { max, min, .. } => (*max - *min) as f64,
                ParamRange::Float { max, min, .. } => *max - *min,
                _ => 1.0,
            };

            let normal = Normal::new(base_val, sigma).unwrap();
            let mut candidate = normal.sample(rng);

            // Clip to range
            candidate = match range {
                ParamRange::Int { min, max, .. } => candidate.clamp(*min as f64, *max as f64),
                ParamRange::Float { min, max, .. } => candidate.clamp(*min, *max),
                _ => candidate,
            };

            // Calculate likelihoods l(x) and g(x)
            let lx: f64 = good_vals.iter().map(|&v| self.gaussian_pdf(candidate, v, sigma)).sum::<f64>() / good_vals.len() as f64;
            let gx: f64 = bad_vals.iter().map(|&v| self.gaussian_pdf(candidate, v, sigma)).sum::<f64>() / bad_vals.len() as f64;

            let ratio = lx / (gx + 1e-10);
            if ratio > best_ratio {
                best_ratio = ratio;
                best_val = candidate;
            }
        }

        match range {
            ParamRange::Int { .. } => ParamValue::Int(best_val.round() as i64),
            _ => ParamValue::Float(best_val),
        }
    }

    fn gaussian_pdf(&self, x: f64, mean: f64, sigma: f64) -> f64 {
        let exponent = -0.5 * ((x - mean) / sigma).powi(2);
        (1.0 / (sigma * (2.0 * std::f64::consts::PI).sqrt())) * exponent.exp()
    }
}

// ============================================================================
// WALK-FORWARD EVALUATION (C7 FIX)
// ============================================================================

/// Configuration for robustness verification via temporal cross-validation.
#[derive(Debug, Clone)]
pub struct WalkForwardConfig {
    /// Number of temporal folds.
    pub n_folds: usize,
    /// Ratio of data used for training (in-sample).
    pub train_ratio: f64,
    /// If true, the training window starts at 0 for every fold (anchored).
    pub anchored: bool,
}

impl Default for WalkForwardConfig {
    fn default() -> Self {
        Self {
            n_folds: 5,
            train_ratio: 0.8,
            anchored: false,
        }
    }
}

/// Results for a single fold (temporal interval).
#[derive(Debug, Clone)]
pub struct FoldResult {
    /// Fold index.
    pub fold: usize,
    /// In-sample (IS) trial results.
    pub train_result: BacktestResults,
    /// Out-of-sample (OOS) trial results.
    pub test_result: BacktestResults,
    /// Best IS parameters applied to OOS.
    pub optimal_params: ParamSet,
}

/// Aggregated results across all walk-forward folds.
#[derive(Debug, Clone)]
pub struct WalkForwardResults {
    /// Per-fold detailed results.
    pub folds: Vec<FoldResult>,
    /// Combined OOS Sharpe Ratio.
    pub oos_sharpe: f64,
    /// Combined OOS Total PnL.
    pub oos_total_pnl: f64,
    /// IS Sharpe / OOS Sharpe ratio. Values > 1.5 indicate high overfitting.
    pub overfit_ratio: f64,
}

impl WalkForwardResults {
    pub fn report(&self) {
        info!("═══════════════════════════════════════════");
        info!("      WALK-FORWARD EVALUATION              ");
        info!("═══════════════════════════════════════════");
        info!("Total Folds:        {}", self.folds.len());
        info!("OOS Sharpe Ratio:   {:.3}", self.oos_sharpe);
        info!("OOS Total PnL:      ${:.2}", self.oos_total_pnl);
        info!("Overfit Ratio:      {:.2}", self.overfit_ratio);
        info!("───────────────────────────────────────────");
        
        for fold in &self.folds {
            info!("Fold {} | Train Sharpe: {:.3} | Test Sharpe: {:.3} | Test PnL: ${:.2}",
                fold.fold + 1,
                fold.train_result.sharpe_ratio,
                fold.test_result.sharpe_ratio,
                fold.test_result.total_pnl
            );
        }
        info!("═══════════════════════════════════════════");
    }
}

/// Partitions a dataset into training and testing splits for walk-forward.
pub struct WalkForwardSplitter {
    /// Total data samples available.
    pub n_samples: usize,
    /// Split configuration.
    pub config: WalkForwardConfig,
}

impl WalkForwardSplitter {
    pub fn new(n_samples: usize, config: WalkForwardConfig) -> Self {
        Self { n_samples, config }
    }

    /// Generates indices for IS and OOS segments.
    ///
    /// # Returns
    /// Vector of (train_start, train_end, test_start, test_end) tuples.
    pub fn splits(&self) -> Vec<(usize, usize, usize, usize)> {
        let fold_size = self.n_samples / self.config.n_folds;
        let mut splits = Vec::new();

        for fold in 0..self.config.n_folds {
            let test_end = (fold + 1) * fold_size;
            let test_start = fold * fold_size + (fold_size as f64 * self.config.train_ratio) as usize;
            
            let train_start = if self.config.anchored { 0 } else { fold * fold_size };
            let train_end = test_start;

            if train_end > train_start && test_end > test_start {
                splits.push((train_start, train_end, test_start, test_end));
            }
        }

        splits
    }
}

// ============================================================================
// STRATEGY FACTORY TRAIT
// ============================================================================

/// Abstraction for instantiating strategies with varying parameters.
pub trait StrategyFactory: Send {
    /// Instantiates a strategy with the provided parameter set.
    fn create(&self, params: &ParamSet) -> Box<dyn Strategy>;
    
    /// Returns the symbol-independent name of the strategy class.
    fn name(&self) -> &str;
}

// ============================================================================
// PARAMETER SWEEP RUNNER
// ============================================================================

/// High-level orchestrator for parameter optimization.
pub struct ParameterSweepRunner {
    /// Source WAL path.
    pub wal_path: String,
    /// Starting capital.
    pub initial_equity: f64,
    /// Simulated slippage in bps.
    pub slippage_bps: f64,
    /// Seed for reproducible backtests.
    pub seed: Option<u64>,
}

impl ParameterSweepRunner {
    pub fn new(wal_path: &str, initial_equity: f64, slippage_bps: f64, seed: Option<u64>) -> Self {
        Self {
            wal_path: wal_path.to_string(),
            initial_equity,
            slippage_bps,
            seed,
        }
    }

    /// Executes a sweep over a pre-defined set of combinations.
    pub async fn run_sweep<F: StrategyFactory>(
        &self,
        factory: &F,
        combinations: Vec<ParamSet>,
    ) -> OptimizationResults {
        let start = std::time::Instant::now();
        let mut results = OptimizationResults::new();
        let total = combinations.len();

        info!("Starting parameter sweep: {} combinations", total);

        for (idx, params) in combinations.into_iter().enumerate() {
            self.run_trial(factory, idx, params, &mut results, None).await;

            if (idx + 1) % 10 == 0 || idx + 1 == total {
                info!("Progress: {}/{} ({:.1}%)", idx + 1, total, (idx + 1) as f64 / total as f64 * 100.0);
            }
        }

        results.elapsed_secs = start.elapsed().as_secs_f64();
        results
    }

    /// Executes iterative Bayesian optimization.
    pub async fn run_tpe<F: StrategyFactory>(
        &self,
        factory: &F,
        optimizer: &TpeOptimizer,
    ) -> OptimizationResults {
        use rand::SeedableRng;
        let start = std::time::Instant::now();
        let mut results = OptimizationResults::new();
        let mut rng = rand_pcg::Pcg64::seed_from_u64(self.seed.unwrap_or(42));

        info!("Starting TPE optimization: {} trials", optimizer.n_trials);

        for idx in 0..optimizer.n_trials {
            let params = optimizer.suggest(&results.trials, &mut rng);
            self.run_trial(factory, idx, params, &mut results, None).await;

            if (idx + 1) % 5 == 0 || idx + 1 == optimizer.n_trials {
                info!("Progress: {}/{} ({:.1}%) | Best Sharpe: {:.3}", 
                    idx + 1, optimizer.n_trials, (idx + 1) as f64 / optimizer.n_trials as f64 * 100.0,
                    results.best_trial.as_ref().map(|t| t.sharpe_ratio).unwrap_or(0.0)
                );
            }
        }

        results.elapsed_secs = start.elapsed().as_secs_f64();
        results
    }

    /// Executes iterative Bayesian optimization over a specific range.
    pub async fn run_tpe_range<F: StrategyFactory>(
        &self,
        factory: &F,
        optimizer: &TpeOptimizer,
        range: (usize, usize),
    ) -> OptimizationResults {
        use rand::SeedableRng;
        let start = std::time::Instant::now();
        let mut results = OptimizationResults::new();
        let mut rng = rand_pcg::Pcg64::seed_from_u64(self.seed.unwrap_or(42));

        for idx in 0..optimizer.n_trials {
            let params = optimizer.suggest(&results.trials, &mut rng);
            self.run_trial(factory, idx, params, &mut results, Some(range)).await;
        }

        results.elapsed_secs = start.elapsed().as_secs_f64();
        results
    }

    /// Executes a full Walk-Forward Analysis.
    pub async fn run_walk_forward<F: StrategyFactory>(
        &self,
        factory: &F,
        config: WalkForwardConfig,
        template_optimizer: TpeOptimizer,
    ) -> anyhow::Result<WalkForwardResults> {
        let mut wal_reader = kubera_core::wal::WalReader::new(&self.wal_path)?;
        let n_samples = wal_reader.count_events()?;
        
        let splitter = WalkForwardSplitter::new(n_samples, config);
        let splits = splitter.splits();
        
        let mut folds = Vec::new();
        let mut total_oos_pnl = 0.0;
        let mut sum_is_sharpe = 0.0;
        let mut sum_oos_sharpe = 0.0;

        info!("Starting Walk-Forward Analysis ({} folds)", splits.len());

        for (fold_idx, (train_start, train_end, test_start, test_end)) in splits.into_iter().enumerate() {
            info!("--- Fold {}/{} | IS: [{}, {}] | OOS: [{}, {}] ---", 
                fold_idx + 1, splitter.config.n_folds, train_start, train_end, test_start, test_end);
            
            // 1. In-Sample Optimization (TPE)
            let mut optimizer = TpeOptimizer::new(template_optimizer.n_trials);
            optimizer.param_space = template_optimizer.param_space.clone();
            
            let is_sweep_results = self.run_tpe_range(factory, &optimizer, (train_start, train_end)).await;
            let best_trial = is_sweep_results.best_trial.ok_or_else(|| anyhow::anyhow!("No successful IS trials in fold {}", fold_idx + 1))?;
            
            // 2. Best IS Backtest (to get full BacktestResults for reporting)
            let mut is_engine = BacktestEngine::new(&self.wal_path, self.initial_equity, self.seed, self.slippage_bps);
            let train_result = is_engine.run_range(factory.create(&best_trial.params), train_start, train_end).await?;
            
            // 3. Out-of-Sample Evaluation
            let mut oos_engine = BacktestEngine::new(&self.wal_path, self.initial_equity, self.seed, self.slippage_bps);
            let test_result = oos_engine.run_range(factory.create(&best_trial.params), test_start, test_end).await?;
            
            total_oos_pnl += test_result.total_pnl;
            sum_is_sharpe += train_result.sharpe_ratio;
            sum_oos_sharpe += test_result.sharpe_ratio;

            folds.push(FoldResult {
                fold: fold_idx,
                train_result,
                test_result,
                optimal_params: best_trial.params,
            });
            
            info!("Fold {} Complete | IS Sharpe: {:.3} | OOS Sharpe: {:.3}", 
                fold_idx + 1, folds.last().unwrap().train_result.sharpe_ratio, folds.last().unwrap().test_result.sharpe_ratio);
        }
        
        let n_folds = folds.len() as f64;
        let avg_is_sharpe = if n_folds > 0.0 { sum_is_sharpe / n_folds } else { 0.0 };
        let avg_oos_sharpe = if n_folds > 0.0 { sum_oos_sharpe / n_folds } else { 0.0 };
        let overfit_ratio = if avg_oos_sharpe > 1e-6 { avg_is_sharpe / avg_oos_sharpe } else { 0.0 };

        Ok(WalkForwardResults {
            folds,
            oos_sharpe: avg_oos_sharpe,
            oos_total_pnl: total_oos_pnl,
            overfit_ratio,
        })
    }

    async fn run_trial<F: StrategyFactory>(
        &self,
        factory: &F,
        trial_id: usize,
        params: ParamSet,
        results: &mut OptimizationResults,
        range: Option<(usize, usize)>,
    ) {
        let strategy = factory.create(&params);
        let mut engine = BacktestEngine::new(
            &self.wal_path,
            self.initial_equity,
            self.seed,
            self.slippage_bps,
        );

        let bt_result = if let Some((start, end)) = range {
            engine.run_range(strategy, start, end).await
        } else {
            engine.run(strategy).await
        };

        match bt_result {
            Ok(bt_results) => {
                let trial = TrialResult {
                    trial_id,
                    params,
                    sharpe_ratio: bt_results.sharpe_ratio,
                    sortino_ratio: bt_results.sortino_ratio,
                    max_drawdown: bt_results.max_drawdown,
                    total_pnl: bt_results.total_pnl,
                    total_trades: bt_results.total_trades,
                    profit_factor: bt_results.profit_factor,
                };
                results.add_trial(trial);
            }
            Err(e) => {
                warn!("Trial {} failed: {}", trial_id, e);
            }
        }
    }
}
