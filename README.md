# QuantKubera - Institutional-Grade Rust Trading Platform

A high-frequency, multi-venue trading system built in Rust for NSE F&O and crypto markets.

## Features

- **Multi-Venue Support**: Binance and Zerodha connectors with unified `MarketConnector` trait
- **HYDRA Strategy**: Multi-expert ensemble with 5 specialized strategies and meta-allocation
- **Real-Time Greeks**: Black-Scholes pricing with delta hedging execution
- **Circuit Breakers**: Rate limiting, latency monitoring, drawdown protection
- **Event Bus Architecture**: Low-latency (~230ns) async message passing
- **Institutional Observability**: Prometheus metrics, Grafana dashboards, OpenTelemetry tracing

## Project Structure

```
QuantKubera/
├── crates/                    # Rust workspace
│   ├── kubera-core/           # Event bus, strategies, risk management
│   ├── kubera-data/           # Market data types, WAL recording
│   ├── kubera-executor/       # Order execution (simulated + live)
│   ├── kubera-connectors/     # Zerodha/Binance WebSocket connectors
│   ├── kubera-options/        # Black-Scholes, Greeks, NSE specs
│   ├── kubera-backtest/       # Backtesting engine
│   ├── kubera-risk/           # Position limits, order validation
│   ├── kubera-runner/         # TUI dashboard, main entry point
│   └── kubera-mlflow/         # MLFlow experiment tracking
├── configs/                   # Configuration files
│   ├── backtest.toml
│   ├── paper.toml
│   └── live.toml
├── infra/                     # Infrastructure
│   └── observability/         # Grafana, Prometheus configs
├── python/                    # Python utilities (Zerodha auth, data)
├── research/                  # Research and indicators
├── ui/                        # Web dashboard (React)
├── contracts/                 # Solidity contracts
└── docs/                      # Documentation
```

## Quick Start

```bash
# Build all crates
cargo build --release

# Run paper trading mode
cargo run -p kubera-runner --release -- --mode paper

# Run backtest
cargo run -p kubera-backtest --release

# Run benchmarks
cargo bench -p kubera-core
```

## Configuration

All configuration files are in `configs/`:

- `configs/paper.toml` - Paper trading configuration
- `configs/backtest.toml` - Backtesting configuration
- `configs/live.toml` - Live trading configuration (use with caution)

## Environment Variables

Required for Zerodha:
```bash
ZERODHA_API_KEY=your_api_key
ZERODHA_API_SECRET=your_api_secret
ZERODHA_USER_ID=your_user_id
ZERODHA_PASSWORD=your_password
ZERODHA_TOTP_SECRET=your_totp_secret
```

## License

PROPRIETARY - All rights reserved. See [LICENSE](./LICENSE).
