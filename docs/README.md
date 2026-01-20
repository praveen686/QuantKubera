# QuantKubera - Institutional-Grade Rust Trading Platform

A high-frequency, multi-venue trading system built in Rust, adhering to MasterPrompt engineering standards.

## Features

- **Multi-Venue Support**: Binance and Zerodha connectors with unified `MarketConnector` trait
- **SBE Decoding**: Binary-efficient Simple Binary Encoding for Binance streams
- **Institutional Observability**: Prometheus metrics (`:9000/metrics`), OpenTelemetry tracing
- **Circuit Breaker Isolation**: Per-symbol strategy task isolation with panic recovery
- **Event Bus Architecture**: Low-latency (~230ns) async message passing

## Quick Start

```bash
# Build all crates
cargo build --all

# Run paper trading mode
cargo run -p kubera-runner -- --mode paper

# Run benchmarks
cargo bench -p kubera-core
```

## License

PROPRIETARY - All rights reserved. See [LICENSE](./LICENSE).
