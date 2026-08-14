//! Process metrics in Prometheus text format.
//!
//! A fixed set of counters registered at first use, incremented from
//! the paths that matter, and rendered on demand. No exporter
//! dependency: the format is a few lines of text, and a scrape
//! endpoint that cannot pull in a supply chain is worth more than
//! histogram quantiles.
//!
//! Request latency is exposed as a sum and a count (an average, not
//! quantiles). Buckets can follow when someone needs them; claiming
//! percentiles the process does not compute would be worse than
//! offering the honest pair.
//!
//! The endpoint lives on the ADMIN surface. Per-tenant volume is
//! operator data, and deployments that split `COPAL_ADMIN_BIND` keep
//! it off the tenant-facing network with everything else.

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{OnceLock, RwLock};

/// Registered counters, keyed by their rendered name (including any
/// labels). A read-mostly map: every counter is created once and
/// incremented forever after.
type Counters = RwLock<BTreeMap<String, &'static AtomicU64>>;

fn counters() -> &'static Counters {
    static COUNTERS: OnceLock<Counters> = OnceLock::new();
    COUNTERS.get_or_init(|| RwLock::new(BTreeMap::new()))
}

/// Add to a counter, creating it on first use. Leaking one atomic per
/// distinct series is deliberate: the series set is bounded by the
/// code, never by request input.
pub fn add(name: &str, by: u64) {
    if let Ok(map) = counters().read() {
        if let Some(cell) = map.get(name) {
            cell.fetch_add(by, Ordering::Relaxed);
            return;
        }
    }
    let Ok(mut map) = counters().write() else {
        return;
    };
    let cell = map
        .entry(name.to_owned())
        .or_insert_with(|| Box::leak(Box::new(AtomicU64::new(0))));
    cell.fetch_add(by, Ordering::Relaxed);
}

/// Increment a counter by one.
pub fn incr(name: &str) {
    add(name, 1);
}

/// Registered gauges, separate from the counters because their verbs
/// differ: a gauge is SET to the latest observation (a sweep's count
/// of the moment), never accumulated.
fn gauges() -> &'static Counters {
    static GAUGES: OnceLock<Counters> = OnceLock::new();
    GAUGES.get_or_init(|| RwLock::new(BTreeMap::new()))
}

/// Set a gauge to the latest observation, creating it on first use.
/// The same bounded-series rule as [`add`]: label sets come from
/// configuration and operator actions, never from request input.
pub fn set_gauge(name: &str, value: u64) {
    if let Ok(map) = gauges().read() {
        if let Some(cell) = map.get(name) {
            cell.store(value, Ordering::Relaxed);
            return;
        }
    }
    let Ok(mut map) = gauges().write() else {
        return;
    };
    let cell = map
        .entry(name.to_owned())
        .or_insert_with(|| Box::leak(Box::new(AtomicU64::new(0))));
    cell.store(value, Ordering::Relaxed);
}

/// Record one served request: its status class and its duration.
pub fn observe_request(status: u16, seconds: f64) {
    let class = match status {
        200..=299 => "2xx",
        300..=399 => "3xx",
        400..=499 => "4xx",
        _ => "5xx",
    };
    incr(&format!("copal_http_responses_total{{class=\"{class}\"}}"));
    // Microseconds keep the accumulator integral; the render divides.
    add(
        "copal_http_request_duration_micros_sum",
        (seconds * 1_000_000.0) as u64,
    );
    incr("copal_http_request_duration_count");
}

/// Render the current values in Prometheus text format.
pub fn render() -> String {
    let Ok(map) = counters().read() else {
        return String::new();
    };
    let mut out = String::new();
    let mut duration_sum = 0u64;
    let mut duration_count = 0u64;
    let mut seen_help: BTreeMap<&str, ()> = BTreeMap::new();

    for (name, cell) in map.iter() {
        let value = cell.load(Ordering::Relaxed);
        if name == "copal_http_request_duration_micros_sum" {
            duration_sum = value;
            continue;
        }
        if name == "copal_http_request_duration_count" {
            duration_count = value;
            continue;
        }
        let base = name.split('{').next().unwrap_or(name);
        if seen_help.insert(base, ()).is_none() {
            out.push_str(&format!("# TYPE {base} counter\n"));
        }
        out.push_str(&format!("{name} {value}\n"));
    }

    if let Ok(map) = gauges().read() {
        let mut seen_gauge: BTreeMap<&str, ()> = BTreeMap::new();
        for (name, cell) in map.iter() {
            let value = cell.load(Ordering::Relaxed);
            let base = name.split('{').next().unwrap_or(name);
            if seen_gauge.insert(base, ()).is_none() {
                out.push_str(&format!("# TYPE {base} gauge\n"));
            }
            out.push_str(&format!("{name} {value}\n"));
        }
    }

    out.push_str("# TYPE copal_http_request_duration_seconds summary\n");
    out.push_str(&format!(
        "copal_http_request_duration_seconds_sum {:.6}\n",
        duration_sum as f64 / 1_000_000.0,
    ));
    out.push_str(&format!(
        "copal_http_request_duration_seconds_count {duration_count}\n",
    ));
    out
}

/// Test support: current value of one series, zero when absent.
pub fn value(name: &str) -> u64 {
    counters()
        .read()
        .ok()
        .and_then(|map| map.get(name).map(|c| c.load(Ordering::Relaxed)))
        .unwrap_or(0)
}

/// Test support: current value of one gauge, zero when absent.
pub fn gauge_value(name: &str) -> u64 {
    gauges()
        .read()
        .ok()
        .and_then(|map| map.get(name).map(|c| c.load(Ordering::Relaxed)))
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn counters_accumulate_and_render() {
        incr("copal_test_widgets_total{kind=\"a\"}");
        add("copal_test_widgets_total{kind=\"a\"}", 4);
        observe_request(204, 0.25);
        observe_request(503, 0.75);

        assert_eq!(value("copal_test_widgets_total{kind=\"a\"}"), 5);
        let text = render();
        assert!(text.contains("# TYPE copal_test_widgets_total counter"));
        assert!(text.contains("copal_test_widgets_total{kind=\"a\"} 5"));
        assert!(text.contains("copal_http_responses_total{class=\"2xx\"} 1"));
        assert!(text.contains("copal_http_responses_total{class=\"5xx\"} 1"));
        assert!(text.contains("copal_http_request_duration_seconds_sum 1.000000"));
        assert!(text.contains("copal_http_request_duration_seconds_count 2"));
        // One TYPE line per metric family, labels excluded.
        assert_eq!(text.matches("# TYPE copal_http_responses_total").count(), 1);
    }

    #[test]
    fn gauges_set_to_the_latest_observation() {
        set_gauge("copal_test_depth{pool=\"a\"}", 7);
        set_gauge("copal_test_depth{pool=\"a\"}", 3);
        assert_eq!(gauge_value("copal_test_depth{pool=\"a\"}"), 3);
        let text = render();
        assert!(text.contains("# TYPE copal_test_depth gauge"));
        assert!(text.contains("copal_test_depth{pool=\"a\"} 3"));
    }
}
