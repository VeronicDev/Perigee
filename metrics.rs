//! # Perigee Metrics Module
//!
//! Exports Prometheus-compatible metrics for monitoring the Perigee DeFi platform.
//!
//! ## Standard HTTP Metrics
//! - `perigee_http_requests_total` — Total HTTP requests by method, path, and status
//! - `perigee_http_request_duration_seconds` — Request latency histogram
//! - `perigee_http_errors_total` — Total HTTP errors (4xx/5xx) by path and status
//!
//! ## Perigee-Specific Metrics
//! - `perigee_vault_count` — Current number of active vaults
//! - `perigee_agent_health` — Agent health status (1 = healthy, 0 = unhealthy)
//! - `perigee_fee_collected_stroops` — Total fees collected in stroops (Stellar's smallest unit)
//! - `perigee_liquidity_pool_count` — Number of active liquidity pools
//! - `perigee_swap_volume_stroops` — Total swap volume in stroops

use axum::{
    extract::State,
    http::StatusCode,
    response::{IntoResponse, Response},
    routing::get,
    Router,
};
use prometheus::{
    Encoder, Gauge, GaugeVec, Histogram, HistogramOpts, HistogramVec, IntCounter, IntCounterVec,
    Opts, Registry, TextEncoder,
};
use std::sync::Arc;
use std::time::Instant;

/// Shared application metrics accessible across the application.
#[derive(Clone)]
pub struct PerigeeMetrics {
    pub registry: Registry,

    // ── Standard HTTP metrics ──────────────────────────────────────────
    /// Total number of HTTP requests by method, path, and status code.
    pub http_requests_total: IntCounterVec,
    /// Request latency histogram by method and path.
    pub http_request_duration_seconds: HistogramVec,
    /// Total number of HTTP errors (status >= 400) by path and status code.
    pub http_errors_total: IntCounterVec,

    // ── Perigee-specific metrics ───────────────────────────────────────
    /// Current number of active vaults.
    pub vault_count: Gauge,
    /// Agent health status: 1 = healthy, 0 = unhealthy.
    pub agent_health: GaugeVec,
    /// Total fees collected in stroops (1 XLM = 10,000,000 stroops).
    pub fee_collected_stroops: IntCounter,
    /// Number of active liquidity pools.
    pub liquidity_pool_count: Gauge,
    /// Total swap volume in stroops.
    pub swap_volume_stroops: IntCounter,
}

impl PerigeeMetrics {
    /// Create a new `PerigeeMetrics` instance with all metrics registered.
    pub fn new() -> Self {
        let registry = Registry::new();

        // ── Standard HTTP metrics ──────────────────────────────────────
        let http_requests_total = IntCounterVec::new(
            Opts::new(
                "perigee_http_requests_total",
                "Total number of HTTP requests",
            ),
            &["method", "path", "status"],
        )
        .expect("Failed to create http_requests_total metric");

        let http_request_duration_seconds = HistogramVec::new(
            HistogramOpts::new(
                "perigee_http_request_duration_seconds",
                "HTTP request latency in seconds",
            )
            .buckets(vec![
                0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0,
            ]),
            &["method", "path"],
        )
        .expect("Failed to create http_request_duration_seconds metric");

        let http_errors_total = IntCounterVec::new(
            Opts::new(
                "perigee_http_errors_total",
                "Total number of HTTP errors (status >= 400)",
            ),
            &["path", "status"],
        )
        .expect("Failed to create http_errors_total metric");

        // ── Perigee-specific metrics ───────────────────────────────────
        let vault_count = Gauge::with_opts(Opts::new(
            "perigee_vault_count",
            "Current number of active vaults",
        ))
        .expect("Failed to create vault_count metric");

        let agent_health = GaugeVec::new(
            Opts::new("perigee_agent_health", "Agent health status (1=healthy, 0=unhealthy)"),
            &["agent_id"],
        )
        .expect("Failed to create agent_health metric");

        let fee_collected_stroops = IntCounter::with_opts(Opts::new(
            "perigee_fee_collected_stroops",
            "Total fees collected in stroops (1 XLM = 10,000,000 stroops)",
        ))
        .expect("Failed to create fee_collected_stroops metric");

        let liquidity_pool_count = Gauge::with_opts(Opts::new(
            "perigee_liquidity_pool_count",
            "Number of active liquidity pools",
        ))
        .expect("Failed to create liquidity_pool_count metric");

        let swap_volume_stroops = IntCounter::with_opts(Opts::new(
            "perigee_swap_volume_stroops",
            "Total swap volume in stroops",
        ))
        .expect("Failed to create swap_volume_stroops metric");

        // ── Register all metrics ───────────────────────────────────────
        registry
            .register(Box::new(http_requests_total.clone()))
            .expect("Failed to register http_requests_total");
        registry
            .register(Box::new(http_request_duration_seconds.clone()))
            .expect("Failed to register http_request_duration_seconds");
        registry
            .register(Box::new(http_errors_total.clone()))
            .expect("Failed to register http_errors_total");
        registry
            .register(Box::new(vault_count.clone()))
            .expect("Failed to register vault_count");
        registry
            .register(Box::new(agent_health.clone()))
            .expect("Failed to register agent_health");
        registry
            .register(Box::new(fee_collected_stroops.clone()))
            .expect("Failed to register fee_collected_stroops");
        registry
            .register(Box::new(liquidity_pool_count.clone()))
            .expect("Failed to register liquidity_pool_count");
        registry
            .register(Box::new(swap_volume_stroops.clone()))
            .expect("Failed to register swap_volume_stroops");

        Self {
            registry,
            http_requests_total,
            http_request_duration_seconds,
            http_errors_total,
            vault_count,
            agent_health,
            fee_collected_stroops,
            liquidity_pool_count,
            swap_volume_stroops,
        }
    }

    // ── Convenience recording helpers ──────────────────────────────────

    /// Record a completed HTTP request.
    pub fn record_request(&self, method: &str, path: &str, status: u16, duration_secs: f64) {
        let status_str = status.to_string();

        self.http_requests_total
            .with_label_values(&[method, path, &status_str])
            .inc();

        self.http_request_duration_seconds
            .with_label_values(&[method, path])
            .observe(duration_secs);

        if status >= 400 {
            self.http_errors_total
                .with_label_values(&[path, &status_str])
                .inc();
        }
    }

    /// Record a fee collection event.
    pub fn record_fee(&self, stroops: u64) {
        self.fee_collected_stroops.inc_by(stroops);
    }

    /// Record a swap volume event.
    pub fn record_swap_volume(&self, stroops: u64) {
        self.swap_volume_stroops.inc_by(stroops);
    }

    /// Set the vault count gauge.
    pub fn set_vault_count(&self, count: f64) {
        self.vault_count.set(count);
    }

    /// Set agent health status.
    pub fn set_agent_health(&self, agent_id: &str, healthy: bool) {
        self.agent_health
            .with_label_values(&[agent_id])
            .set(if healthy { 1.0 } else { 0.0 });
    }

    /// Set the liquidity pool count gauge.
    pub fn set_liquidity_pool_count(&self, count: f64) {
        self.liquidity_pool_count.set(count);
    }
}

impl Default for PerigeeMetrics {
    fn default() -> Self {
        Self::new()
    }
}

// ── Axum /metrics handler ──────────────────────────────────────────────────────

/// Application state shared across handlers.
#[derive(Clone)]
pub struct AppState {
    pub metrics: Arc<PerigeeMetrics>,
}

/// GET /metrics — Prometheus text exposition format.
pub async fn metrics_handler(State(state): State<AppState>) -> Response {
    let encoder = TextEncoder::new();
    let metric_families = state.metrics.registry.gather();
    let mut buffer = Vec::new();
    encoder.encode(&metric_families, &mut buffer).expect("Failed to encode metrics");

    (
        StatusCode::OK,
        [(
            axum::http::header::CONTENT_TYPE,
            "text/plain; version=0.0.4; charset=utf-8",
        )],
        buffer,
    )
        .into_response()
}

// ── Axum request-recording middleware ───────────────────────────────────────────

/// Axum layer that wraps each request to record latency, count, and errors.
pub struct MetricsLayer {
    pub metrics: Arc<PerigeeMetrics>,
}

impl<S> tower::Layer<S> for MetricsLayer {
    type Service = MetricsService<S>;

    fn layer(&self, inner: S) -> Self::Service {
        MetricsService {
            inner,
            metrics: self.metrics.clone(),
        }
    }
}

#[derive(Clone)]
pub struct MetricsService<S> {
    inner: S,
    metrics: Arc<PerigeeMetrics>,
}

impl<S, B> tower::Service<axum::http::Request<B>> for MetricsService<S>
where
    S: tower::Service<axum::http::Request<B>, Response = axum::http::Response<axum::body::Body>>
        + Clone
        + Send
        + 'static,
    S::Future: Send,
    B: Send + 'static,
{
    type Response = S::Response;
    type Error = S::Error;
    type Future = std::pin::Pin<
        Box<dyn std::future::Future<Output = Result<Self::Response, Self::Error>> + Send>,
    >;

    fn poll_ready(
        &mut self,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx)
    }

    fn call(&mut self, req: axum::http::Request<B>) -> Self::Future {
        let method = req.method().to_string();
        let path = req.uri().path().to_string();
        let metrics = self.metrics.clone();
        let start = Instant::now();

        // Skip recording the /metrics endpoint itself to avoid double-counting.
        let skip = path == "/metrics";

        let fut = self.inner.call(req);

        Box::pin(async move {
            let response = fut.await?;
            if !skip {
                let status = response.status().as_u16();
                let duration = start.elapsed().as_secs_f64();
                metrics.record_request(&method, &path, status, duration);
            }
            Ok(response)
        })
    }
}

// ── Router builder helper ──────────────────────────────────────────────────────

/// Build an Axum router with the `/metrics` endpoint and the metrics middleware layer.
pub fn metrics_routes(state: AppState) -> Router<AppState> {
    Router::new()
        .route("/metrics", get(metrics_handler))
        .with_state(state)
}

// ── Tests ──────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use axum::body::Body;
    use axum::http::{Request, StatusCode};
    use tower::ServiceExt;

    #[test]
    fn metrics_creates_and_registers_all_metrics() {
        let m = PerigeeMetrics::new();

        // Standard HTTP metrics exist.
        m.http_requests_total
            .with_label_values(&["GET", "/vaults", "200"])
            .inc();
        m.http_request_duration_seconds
            .with_label_values(&["GET", "/vaults"])
            .observe(0.05);
        m.http_errors_total
            .with_label_values(&["/vaults", "500"])
            .inc();

        // Perigee-specific metrics exist.
        m.set_vault_count(42.0);
        m.set_agent_health("agent-1", true);
        m.record_fee(1_000_000); // 0.1 XLM
        m.set_liquidity_pool_count(7.0);
        m.record_swap_volume(50_000_000); // 5 XLM

        // Verify the registry can gather all metrics.
        let families = m.registry.gather();
        assert!(
            families.len() >= 8,
            "Expected at least 8 metric families, got {}",
            families.len()
        );

        // Verify counter values.
        assert_eq!(m.http_requests_total.with_label_values(&["GET", "/vaults", "200"]).get(), 1);
        assert_eq!(m.fee_collected_stroops.get(), 1_000_000);
        assert_eq!(m.swap_volume_stroops.get(), 50_000_000);
    }

    #[test]
    fn record_request_increments_error_counter_for_5xx() {
        let m = PerigeeMetrics::new();

        m.record_request("POST", "/vaults", 201, 0.03);
        assert_eq!(m.http_errors_total.with_label_values(&["/vaults", "201"]).get(), 0);

        m.record_request("POST", "/vaults", 500, 1.2);
        assert_eq!(m.http_errors_total.with_label_values(&["/vaults", "500"]).get(), 1);

        m.record_request("GET", "/vaults/1", 404, 0.01);
        assert_eq!(m.http_errors_total.with_label_values(&["/vaults/1", "404"]).get(), 1);
    }

    #[test]
    fn record_request_increments_error_counter_for_4xx() {
        let m = PerigeeMetrics::new();

        m.record_request("POST", "/vaults", 400, 0.01);
        assert_eq!(m.http_errors_total.with_label_values(&["/vaults", "400"]).get(), 1);
    }

    #[test]
    fn agent_health_toggle() {
        let m = PerigeeMetrics::new();

        m.set_agent_health("agent-1", true);
        assert_eq!(m.agent_health.with_label_values(&["agent-1"]).get(), 1.0);

        m.set_agent_health("agent-1", false);
        assert_eq!(m.agent_health.with_label_values(&["agent-1"]).get(), 0.0);
    }

    #[tokio::test]
    async fn metrics_handler_returns_prometheus_text() {
        let metrics = Arc::new(PerigeeMetrics::new());
        metrics.set_vault_count(5.0);

        let state = AppState { metrics };
        let app = metrics_routes(state);

        let response = app
            .oneshot(
                Request::builder()
                    .uri("/metrics")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);

        let content_type = response
            .headers()
            .get("content-type")
            .unwrap()
            .to_str()
            .unwrap();
        assert!(content_type.contains("text/plain"), "Expected text/plain, got {}", content_type);

        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        let body_str = String::from_utf8(body.to_vec()).unwrap();

        // Verify Prometheus text format contains our metrics.
        assert!(
            body_str.contains("perigee_vault_count"),
            "Response body should contain perigee_vault_count"
        );
        assert!(
            body_str.contains("perigee_http_requests_total"),
            "Response body should contain perigee_http_requests_total"
        );
        assert!(
            body_str.contains("perigee_fee_collected_stroops"),
            "Response body should contain perigee_fee_collected_stroops"
        );
    }
}
