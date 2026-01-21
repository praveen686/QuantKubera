use kubera_core::{EventBus, Strategy, MomentumStrategy, OrasStrategy};
use kubera_models::{MarketEvent, OrderEvent, SignalEvent, RiskEvent, HostEvent, HostResponse};
use tokio::net::TcpStream;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use std::sync::Arc;
use clap::Parser;

#[derive(Parser, Debug)]
struct Args {
    #[arg(short, long, default_value = "127.0.0.1:9091")]
    addr: String,
    
    #[arg(short, long, default_value = "MomentumStrategy")]
    strategy: String,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt::init();
    let args = Args::parse();
    
    let mut strategy: Box<dyn Strategy> = match args.strategy.as_str() {
        "MomentumStrategy" => Box::new(MomentumStrategy::new(20)),
        "ORAS" => Box::new(OrasStrategy::new()),
        _ => panic!("Unknown strategy"),
    };
    
    tracing::info!("Connecting to runner at {}", args.addr);
    let mut stream = TcpStream::connect(args.addr).await?;
    
    // We need a dummy bus for the local strategy to publish signals to
    // But since we are isolated, we'll intercept publish_signal and send them over the wire
    let bus = EventBus::new(10);
    let mut signal_rx = bus.subscribe_signal();
    
    strategy.on_start(bus);
    
    // Send Ready
    let ready = serde_json::to_vec(&HostResponse::Ready)?;
    stream.write_u32(ready.len() as u32).await?;
    stream.write_all(&ready).await?;

    let (mut rd, mut wr) = stream.into_split();

    // Task to forward signals from local bus back to runner
    tokio::spawn(async move {
        while let Ok(signal) = signal_rx.recv().await {
            let resp = HostResponse::Signal(signal);
            let buf = serde_json::to_vec(&resp).unwrap();
            wr.write_u32(buf.len() as u32).await.unwrap();
            wr.write_all(&buf).await.unwrap();
        }
    });

    let mut buf = vec![0u8; 65536];
    loop {
        let len = rd.read_u32().await? as usize;
        if len > buf.len() { buf.resize(len, 0); }
        rd.read_exact(&mut buf[..len]).await?;
        
        let event: HostEvent = serde_json::from_slice(&buf[..len])?;
        match event {
            HostEvent::Start => {} // Already started
            HostEvent::Tick(e) => strategy.on_tick(&e),
            HostEvent::Bar(e) => strategy.on_bar(&e),
            HostEvent::Order(e) => strategy.on_order_update(&e),
            HostEvent::Risk(e) => strategy.on_risk_event(&e),
            HostEvent::Timer(t) => strategy.on_signal_timer(t),
            HostEvent::Stop => {
                strategy.on_stop();
                break;
            }
        }
    }
    
    Ok(())
}
