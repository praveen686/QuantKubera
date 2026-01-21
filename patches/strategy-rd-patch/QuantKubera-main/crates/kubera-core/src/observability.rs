//! # System Observability & Telemetry
//!
//! Integration with OpenTelemetry and Prometheus for production monitoring.
//!
//! ## Description
//! Provides centralized initialization for the system's observability stack:
//! - **Metrics**: Prometheus HTTP exporter for hardware and business logic metrics.
//! - **Distributed Tracing**: OpenTelemetry integration for service-level profiling.
//! - **Structured Logging**: Integration with `tracing` for hierarchical log streams.
//!
//! ## References
//! - IEEE Std 1016-2009: Software Design Descriptions
//! - OpenTelemetry documentation
//! - Prometheus Monitoring Guide

use metrics_exporter_prometheus::PrometheusBuilder;
use opentelemetry::global;
use opentelemetry_sdk::propagation::TraceContextPropagator;
use opentelemetry_sdk::trace::{Config, Sampler};
use tracing_subscriber::prelude::*;
use std::net::SocketAddr;

/// Initializes the Prometheus metrics exporter on the specified socket.
///
/// # Parameters
/// * `addr` - The network address to bind the HTTP metrics endpoint to.
pub fn init_metrics(addr: SocketAddr) {
    let builder = PrometheusBuilder::new()
        .with_http_listener(addr);
    
    builder.install().expect("failed to install Prometheus recorder");
    tracing::info!("Prometheus metrics exporter started on {}", addr);
}

/// Initializes the OpenTelemetry tracing provider and subscriber registry.
///
/// # Parameters
/// * `service_name` - Identifier for the current executing binary/context for trace aggregation.
pub fn init_tracing(service_name: &str) {
    global::set_text_map_propagator(TraceContextPropagator::new());

    let provider = opentelemetry_sdk::trace::TracerProvider::builder()
        .with_config(Config::default().with_sampler(Sampler::AlwaysOn))
        .build();

    global::set_tracer_provider(provider.clone());

    let tracer = opentelemetry::trace::TracerProvider::tracer(&provider, service_name.to_string());
    let telemetry = tracing_opentelemetry::layer().with_tracer(tracer);

    tracing_subscriber::registry()
        .with(tracing_subscriber::fmt::layer())
        .with(telemetry)
        .init();

    tracing::info!("OpenTelemetry tracing layer initialized for service: {}", service_name);
}
