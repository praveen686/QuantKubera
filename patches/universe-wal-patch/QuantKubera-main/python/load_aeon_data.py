"""
VectorBT Pro Integration Example for QuantKubera AEON

This script demonstrates how to load Parquet files exported from
the Rust backtesting engine and analyze them with VectorBT Pro.

Prerequisites:
    pip install vectorbtpro pandas pyarrow

Usage:
    1. Run a backtest with Parquet export enabled:
       cargo run -p kubera-runner -- --mode backtest --aeon --export-parquet
    
    2. Run this script to analyze the results:
       python load_aeon_data.py
"""

import pandas as pd

# Optional: Use vectorbtpro if available
try:
    import vectorbtpro as vbt
    HAS_VBT = True
except ImportError:
    HAS_VBT = False
    print("vectorbtpro not installed. Using pandas only.")


def load_equity_curve(path: str = "aeon_equity.parquet") -> pd.DataFrame:
    """Load equity curve from Parquet file."""
    df = pd.read_parquet(path)
    df['timestamp'] = pd.to_datetime(df['timestamp_ms'], unit='ms')
    df = df.set_index('timestamp')
    return df


def load_signals(path: str = "aeon_signals.parquet") -> pd.DataFrame:
    """Load signal data from Parquet file."""
    df = pd.read_parquet(path)
    df['timestamp'] = pd.to_datetime(df['timestamp_ms'], unit='ms')
    df = df.set_index('timestamp')
    return df


def load_trades(path: str = "aeon_trades.parquet") -> pd.DataFrame:
    """Load trade data from Parquet file."""
    df = pd.read_parquet(path)
    df['timestamp'] = pd.to_datetime(df['timestamp_ms'], unit='ms')
    df = df.set_index('timestamp')
    return df


def analyze_with_vectorbt(equity_df: pd.DataFrame, signals_df: pd.DataFrame):
    """Analyze results using VectorBT Pro."""
    if not HAS_VBT:
        print("VectorBT Pro not available for analysis")
        return
    
    # Create portfolio from equity curve
    returns = equity_df['equity'].pct_change().dropna()
    
    # Calculate key metrics
    sharpe = vbt.returns_nb.sharpe_ratio(returns.values, ann_factor=252*24*60)
    sortino = vbt.returns_nb.sortino_ratio(returns.values, ann_factor=252*24*60)
    max_dd = vbt.returns_nb.max_drawdown(returns.values)
    
    print("\n" + "="*50)
    print("VECTORBT PRO ANALYSIS")
    print("="*50)
    print(f"Sharpe Ratio:    {sharpe:.4f}")
    print(f"Sortino Ratio:   {sortino:.4f}")
    print(f"Max Drawdown:    {max_dd*100:.2f}%")
    print(f"Total Return:    {(equity_df['equity'].iloc[-1] / equity_df['equity'].iloc[0] - 1)*100:.2f}%")
    print("="*50)


def main():
    print("Loading AEON backtest data from Parquet files...\n")
    
    # Load data
    try:
        equity = load_equity_curve()
        print(f"✓ Loaded equity curve: {len(equity)} points")
        print(f"  Start: {equity.index[0]}")
        print(f"  End:   {equity.index[-1]}")
        print(f"  Initial Equity: ${equity['equity'].iloc[0]:,.2f}")
        print(f"  Final Equity:   ${equity['equity'].iloc[-1]:,.2f}")
        print(f"  Total PnL:      ${equity['realized_pnl'].iloc[-1]:,.2f}")
    except FileNotFoundError:
        print("✗ aeon_equity.parquet not found")
        equity = None

    try:
        signals = load_signals()
        print(f"\n✓ Loaded signals: {len(signals)} records")
        print(f"  Experts: {signals['expert'].unique().tolist()}")
    except FileNotFoundError:
        print("✗ aeon_signals.parquet not found")
        signals = None

    try:
        trades = load_trades()
        print(f"\n✓ Loaded trades: {len(trades)} fills")
        print(f"  Total Commission: ${trades['commission'].sum():,.2f}")
        print(f"  Total PnL:        ${trades['pnl'].sum():,.2f}")
        
        # Trade statistics
        winning = trades[trades['pnl'] > 0]
        losing = trades[trades['pnl'] < 0]
        if len(trades) > 0:
            win_rate = len(winning) / len(trades) * 100
            print(f"  Win Rate:         {win_rate:.1f}%")
    except FileNotFoundError:
        print("✗ aeon_trades.parquet not found")
        trades = None

    # VectorBT Analysis
    if equity is not None and signals is not None:
        analyze_with_vectorbt(equity, signals)


if __name__ == "__main__":
    main()
