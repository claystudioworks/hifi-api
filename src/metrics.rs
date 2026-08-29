//! Prometheus metrics — the visibility layer *before* Tidal bans.
//!
//! Exposes a process-wide [`Registry`] backed by [`OnceLock`] so counters
//! survive for the lifetime of the process (including across integration
//! tests that share a test binary). Two counters are registered eagerly:
//!
//! * `hifi_requests_total{route,status}` — all proxied requests
//! * `hifi_429_total` — upstream Tidal HTTP-429 responses observed by the
//!   anti-ban layer (Task 3 wiring; the counter exists from day one so
//!   scrapes always show it, even at zero).
//!
//! Registration is idempotent: each counter is created exactly once by its
//! [`OnceLock`], and a re-registration of the same metric name is silently
//! ignored, so [`init`] may safely be called from any startup or scrape
//! path, any number of times.

use std::sync::OnceLock;

use prometheus::{Encoder, IntCounter, IntCounterVec, Opts, Registry, TextEncoder};

static REGISTRY: OnceLock<Registry> = OnceLock::new();
static REQUESTS: OnceLock<IntCounterVec> = OnceLock::new();
static HITS_429: OnceLock<IntCounter> = OnceLock::new();

/// The process-wide metrics registry.
pub fn registry() -> &'static Registry {
    REGISTRY.get_or_init(Registry::new)
}

/// All hifi-api requests, labelled by route and response status.
///
/// Populated by the per-request instrumentation layered in a later task;
/// the counter itself is registered eagerly by [`init`] so it always
/// appears in scrapes, even at zero.
pub fn requests() -> &'static IntCounterVec {
    REQUESTS.get_or_init(|| {
        let counter = IntCounterVec::new(
            Opts::new(
                "hifi_requests_total",
                "Total hifi-api requests by route and response status.",
            ),
            &["route", "status"],
        )
        .expect("static metric definition is valid");
        // `ok()`: a duplicate registration (e.g. `init` called twice) is
        // expected and harmless — the OnceLock keeps the first instance.
        registry().register(Box::new(counter.clone())).ok();
        counter
    })
}

/// Upstream Tidal HTTP-429 hits observed by the anti-ban layer.
pub fn hits_429() -> &'static IntCounter {
    HITS_429.get_or_init(|| {
        let counter = IntCounter::with_opts(Opts::new(
            "hifi_429_total",
            "Total upstream Tidal HTTP-429 responses observed.",
        ))
        .expect("static metric definition is valid");
        registry().register(Box::new(counter.clone())).ok();
        counter
    })
}

/// Eagerly create and register all static metrics.
///
/// Safe to call from any thread, any number of times: everything is
/// [`OnceLock`]-backed and duplicate registrations are ignored.
/// Also seeds a zero-value sample for the labelled `hifi_requests_total`
/// so it appears in scrapes even before any real request has been counted
/// (prometheus only exposes a CounterVec family after at least one label
/// set has been created).
pub fn init() {
    let r = requests();
    // Seed a zero sample — inc_by(0) creates the label-set without counting.
    r.with_label_values(&["init", "0"]).inc_by(0);
    let h = hits_429();
    h.inc_by(0);
}

/// Render all metrics in Prometheus text exposition format (version 0.0.4).
pub fn gather() -> String {
    let mut buf = Vec::new();
    TextEncoder::new()
        .encode(&registry().gather(), &mut buf)
        .expect("prometheus text encoder writes to an in-memory buffer");
    String::from_utf8(buf).expect("prometheus text format is valid UTF-8")
}