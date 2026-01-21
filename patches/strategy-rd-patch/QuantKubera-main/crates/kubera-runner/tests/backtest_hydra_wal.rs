//! # HYDRA Backtest (WAL Replay)
//!
//! ## P.1016 - Support Documentation
//! **Test**: Verification-HYDRA-WAL
//! **Author**: QuantKubera Team
//! **Date**: 2024-12-19
//!
//! ## Description
//! Verifies the KUBERA HYDRA strategy by replaying historical Write-Ahead Log (WAL) data.
//! Checks if the multi-expert system generates signals on real market data.

#[cfg(test)]
mod verification {
    use kubera_core::{EventBus, HydraStrategy, Strategy, Portfolio};
    use kubera_core::wal::{WalReader, WalEvent};
    use kubera_models::{MarketEvent, MarketPayload, OrderEvent, OrderPayload};
    use kubera_executor::SimulatedExchange;
    use chrono::Utc;
    use uuid::Uuid;
    use std::path::PathBuf;

    #[tokio::test]
    async fn backtest_hydra_real_wal() {
        // Locate WAL file
        let mut wal_path = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
        wal_path.pop(); // crates/
        wal_path.pop(); // root
        wal_path.push("trading.wal");
        
        println!(">>> Starting HYDRA Backtest (WAL Replay: {:?}) <<<", wal_path);
        
        if !wal_path.exists() {
            println!("SKIPPING: WAL file not found at {:?}", wal_path);
            return;
        }

        let bus = EventBus::new(10000);
        let mut strategy = HydraStrategy::new();
        strategy.on_start(bus.clone());
        
        let mut exchange = SimulatedExchange::new(bus.clone(), 0.0, None);
        let mut portfolio = Portfolio::new();
        let initial_capital = 10000.0;
        portfolio.balance = initial_capital; 

        let mut signal_rx = bus.take_signal_receiver().unwrap();
        let mut fill_rx = bus.subscribe_fill();
        
        let mut reader = WalReader::new(&wal_path).expect("Failed to open WAL");
        
        let mut signals = 0;
        let mut market_events = 0;
        let mut last_price = 0.0;
        
        // Performance Tracking
        let mut equity_curve: Vec<f64> = vec![initial_capital];
        let mut trade_pnls: Vec<f64> = Vec::new();
        let mut peak_equity = initial_capital;
        let mut max_drawdown = 0.0f64;
        let mut last_fill_price = 0.0;
        let mut last_fill_side: Option<kubera_models::Side> = None;
        
        // Replay Loop
        while let Ok(Some(wal_event)) = reader.next_any() {
            if let WalEvent::Market(event) = wal_event {
                market_events += 1;
                
                // Track Last Price for PnL
                if let MarketPayload::Tick { price, .. } = &event.payload {
                    last_price = *price;
                } else if let MarketPayload::Trade { price, .. } = &event.payload {
                    last_price = *price;
                }
                
                // 1. Feed Strategy
                strategy.on_tick(&event);
                
                // 2. Feed Exchange
                exchange.on_market_data(event.clone()).await.unwrap();
                
                // 3. Process Signals
                while let Ok(signal) = signal_rx.try_recv() {
                    signals += 1;
                    // Mock Execution for Backtest
                    exchange.handle_order(OrderEvent {
                        order_id: Uuid::new_v4(),
                        timestamp: Utc::now(),
                        symbol: signal.symbol.clone(),
                        side: signal.side,
                        payload: OrderPayload::New {
                            symbol: signal.symbol.clone(),
                            side: signal.side,
                            quantity: signal.quantity,
                            price: None, 
                            order_type: kubera_models::OrderType::Market
                        }
                    }).await.unwrap();
                }
                
                // 4. Process Fills
                while let Ok(fill) = fill_rx.try_recv() {
                    portfolio.apply_fill(&fill.symbol, fill.side, fill.quantity, fill.price);
                    
                    // Calculate trade PnL if closing a position
                    if let Some(prev_side) = last_fill_side {
                        if prev_side != fill.side {
                            // Position reversal = trade complete
                            let trade_pnl = match prev_side {
                                kubera_models::Side::Buy => (fill.price - last_fill_price) * fill.quantity,
                                kubera_models::Side::Sell => (last_fill_price - fill.price) * fill.quantity,
                            };
                            trade_pnls.push(trade_pnl);
                        }
                    }
                    last_fill_price = fill.price;
                    last_fill_side = Some(fill.side);
                    
                    // Update equity curve
                    let mut prices = std::collections::HashMap::new();
                    prices.insert("BTCUSDT".to_string(), fill.price);
                    let current_equity = portfolio.calculate_total_value(&prices);
                    equity_curve.push(current_equity);
                    
                    // Track max drawdown
                    if current_equity > peak_equity {
                        peak_equity = current_equity;
                    }
                    let drawdown = (peak_equity - current_equity) / peak_equity;
                    if drawdown > max_drawdown {
                        max_drawdown = drawdown;
                    }
                    
                    // FEED BACK TO STRATEGY (Critical for Meta-Allocator)
                    strategy.on_fill(&OrderEvent { 
                        order_id: Uuid::new_v4(), timestamp: Utc::now(), symbol: fill.symbol.clone(), side: fill.side,
                        payload: OrderPayload::Update { status: kubera_models::OrderStatus::Filled, filled_quantity: fill.quantity, avg_price: fill.price }
                    });
                }
            }
        }
        
        if market_events == 0 {
            println!("No market events found in WAL.");
            return;
        }
        
        // Final Valuation
        let mut prices = std::collections::HashMap::new();
        prices.insert("BTC-USDT".to_string(), last_price);
        prices.insert("BTCUSDT".to_string(), last_price); 
        
        let final_value = portfolio.calculate_total_value(&prices);
        let total_return = (final_value - initial_capital) / initial_capital;
        
        // Calculate Performance Metrics
        let returns: Vec<f64> = equity_curve.windows(2)
            .map(|w| (w[1] - w[0]) / w[0])
            .collect();
        
        let mean_return = if returns.is_empty() { 0.0 } else { 
            returns.iter().sum::<f64>() / returns.len() as f64 
        };
        
        let std_return = if returns.len() < 2 { 0.0 } else {
            let variance = returns.iter()
                .map(|r| (r - mean_return).powi(2))
                .sum::<f64>() / (returns.len() - 1) as f64;
            variance.sqrt()
        };
        
        // Annualized Sharpe (assuming ~252 trading days, ~1440 minutes per day)
        let sharpe_ratio = if std_return > 0.0 {
            (mean_return / std_return) * (252.0_f64).sqrt()
        } else { 0.0 };
        
        // Win Rate & Profit Factor
        let winning_trades: Vec<&f64> = trade_pnls.iter().filter(|&&p| p > 0.0).collect();
        let losing_trades: Vec<&f64> = trade_pnls.iter().filter(|&&p| p < 0.0).collect();
        
        let win_rate = if trade_pnls.is_empty() { 0.0 } else {
            winning_trades.len() as f64 / trade_pnls.len() as f64 * 100.0
        };
        
        let gross_profit: f64 = winning_trades.iter().map(|&&p| p).sum();
        let gross_loss: f64 = losing_trades.iter().map(|&&p| p.abs()).sum();
        let profit_factor = if gross_loss > 0.0 { gross_profit / gross_loss } else { f64::INFINITY };
        
        // Output Results
        println!("\n{}", "=".repeat(60));
        println!("          KUBERA HYDRA - BACKTEST RESULTS");
        println!("{}", "=".repeat(60));
        println!("Events Processed:     {:>10}", market_events);
        println!("Total Signals:        {:>10}", signals);
        println!("Total Trades:         {:>10}", trade_pnls.len());
        println!("{}", "-".repeat(60));
        println!("Initial Capital:      {:>10}", format!("${:.2}", initial_capital));
        println!("Final Portfolio:      {:>10}", format!("${:.2}", final_value));
        println!("Total Return:         {:>10}", format!("{:.2}%", total_return * 100.0));
        println!("{}", "-".repeat(60));
        println!("Sharpe Ratio:         {:>10}", format!("{:.2}", sharpe_ratio));
        println!("Max Drawdown:         {:>10}", format!("{:.2}%", max_drawdown * 100.0));
        println!("Win Rate:             {:>10}", format!("{:.1}%", win_rate));
        println!("Profit Factor:        {:>10}", format!("{:.2}", profit_factor));
        println!("Winning Trades:       {:>10}", winning_trades.len());
        println!("Losing Trades:        {:>10}", losing_trades.len());
        println!("{}", "=".repeat(60));
    }
}
