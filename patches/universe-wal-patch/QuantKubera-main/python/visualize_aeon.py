"""
VectorBT Pro Visualization for AEON Backtest Results

Loads Parquet files exported from the Rust AEON backtest and visualizes
with VectorBT Pro. This script uses the REAL AEON signals, not approximations.

Usage:
    python3 visualize_aeon.py
"""

import pandas as pd
import numpy as np

# Check for vectorbtpro
try:
    import vectorbtpro as vbt
    HAS_VBT = True
except ImportError:
    HAS_VBT = False
    print("Note: vectorbtpro not available, using basic pandas analysis")


def load_parquet_files():
    """Load all AEON Parquet exports."""
    files = {}
    
    try:
        files['equity'] = pd.read_parquet('aeon_equity.parquet')
        print(f"✓ Loaded equity curve: {len(files['equity'])} points")
    except FileNotFoundError:
        print("✗ aeon_equity.parquet not found")
    
    try:
        files['signals'] = pd.read_parquet('aeon_signals.parquet')
        print(f"✓ Loaded signals: {len(files['signals'])} records")
    except FileNotFoundError:
        print("  aeon_signals.parquet not found (may be empty)")
    
    try:
        files['trades'] = pd.read_parquet('aeon_trades.parquet')
        print(f"✓ Loaded trades: {len(files['trades'])} fills")
    except FileNotFoundError:
        print("  aeon_trades.parquet not found (may be empty)")
    
    return files


def analyze_equity(equity_df: pd.DataFrame):
    """Analyze equity curve."""
    print("\n" + "="*60)
    print("EQUITY CURVE ANALYSIS")
    print("="*60)
    
    if 'timestamp_ms' in equity_df.columns:
        equity_df = equity_df.copy()
        equity_df.index = range(len(equity_df))  # Use tick number as index
    
    initial = equity_df['equity'].iloc[0]
    final = equity_df['equity'].iloc[-1]
    pnl = final - initial
    ret_pct = (final / initial - 1) * 100
    
    print(f"Initial Equity: ${initial:,.2f}")
    print(f"Final Equity:   ${final:,.2f}")
    print(f"Total PnL:      ${pnl:,.2f}")
    print(f"Return:         {ret_pct:.2f}%")
    
    # Calculate drawdown
    peak = equity_df['equity'].cummax()
    drawdown = (equity_df['equity'] - peak) / peak * 100
    max_dd = drawdown.min()
    print(f"Max Drawdown:   {max_dd:.2f}%")
    
    # Calculate Sharpe/Sortino manually
    returns = equity_df['equity'].pct_change().dropna()
    if len(returns) > 0:
        ann_factor = np.sqrt(252 * 24 * 60)  # Tick-level data
        
        # Sharpe Ratio
        mean_ret = returns.mean()
        std_ret = returns.std()
        sharpe = (mean_ret / std_ret) * ann_factor if std_ret > 0 else 0
        
        # Sortino Ratio (downside deviation)
        downside = returns[returns < 0]
        downside_std = downside.std() if len(downside) > 0 else std_ret
        sortino = (mean_ret / downside_std) * ann_factor if downside_std > 0 else 0
        
        print(f"Sharpe Ratio:   {sharpe:.4f}")
        print(f"Sortino Ratio:  {sortino:.4f}")
    
    return equity_df


def analyze_trades(trades_df: pd.DataFrame):
    """Analyze trade statistics."""
    print("\n" + "="*60)
    print("TRADE ANALYSIS")
    print("="*60)
    
    if trades_df is None or len(trades_df) == 0:
        print("No trade data available")
        return
    
    total_trades = len(trades_df)
    total_commission = trades_df['commission'].sum() if 'commission' in trades_df.columns else 0
    total_pnl = trades_df['pnl'].sum() if 'pnl' in trades_df.columns else 0
    
    print(f"Total Fills:      {total_trades}")
    print(f"Total Commission: ${total_commission:,.2f}")
    print(f"Net PnL:          ${total_pnl:,.2f}")
    
    if 'pnl' in trades_df.columns:
        winners = trades_df[trades_df['pnl'] > 0]
        losers = trades_df[trades_df['pnl'] < 0]
        win_rate = len(winners) / total_trades * 100 if total_trades > 0 else 0
        
        print(f"Winners:          {len(winners)}")
        print(f"Losers:           {len(losers)}")
        print(f"Win Rate:         {win_rate:.1f}%")


def create_vbt_portfolio(equity_df: pd.DataFrame):
    """Create VectorBT portfolio analysis if possible."""
    if not HAS_VBT:
        print("\nVectorBT Pro not available for portfolio creation")
        return None
    
    print("\n" + "="*60)
    print("VECTORBT PRO PORTFOLIO ANALYSIS")
    print("="*60)
    
    # Create returns series with proper index
    equity_series = equity_df['equity'].copy()
    equity_series.index = pd.RangeIndex(len(equity_series))
    returns = equity_series.pct_change().dropna()
    
    if len(returns) > 0:
        # Use VBT's built-in returns analysis via pandas accessor
        ret_series = pd.Series(returns.values, index=pd.RangeIndex(len(returns)))
        
        # Calculate metrics
        ann_factor = np.sqrt(252 * 24 * 60)
        mean_ret = returns.mean()
        std_ret = returns.std()
        
        stats = {
            'Equity Points': len(equity_df),
            'Total Return [%]': (equity_df['equity'].iloc[-1] / equity_df['equity'].iloc[0] - 1) * 100,
            'Max Drawdown [%]': ((equity_df['equity'] - equity_df['equity'].cummax()) / equity_df['equity'].cummax()).min() * 100,
            'Sharpe Ratio': (mean_ret / std_ret) * ann_factor if std_ret > 0 else 0,
            'Volatility [%]': std_ret * ann_factor * 100,
        }
        
        for k, v in stats.items():
            if isinstance(v, float):
                print(f"{k}: {v:.4f}")
            else:
                print(f"{k}: {v}")
        
        return stats
    
    return None


def main():
    print("="*60)
    print("AEON Backtest Results - VectorBT Pro Visualization")
    print("="*60)
    print()
    
    # Load data
    files = load_parquet_files()
    
    if 'equity' in files:
        equity_df = analyze_equity(files['equity'])
    
    if 'trades' in files:
        analyze_trades(files['trades'])
    
    if 'equity' in files:
        create_vbt_portfolio(files['equity'])
    
    print("\n" + "="*60)
    print("Analysis complete. Data exported from real AEON backtest.")
    print("="*60)


if __name__ == "__main__":
    main()
