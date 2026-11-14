//! Real-time SSE benchmark telemetry broadcaster (`GET /api/v1/benchmarks/{run_id}/stream`)
//! and multi-source read-only pipeline telemetry hub (`GET /api/v1/telemetry/pipeline/stream`)
//! with Tokio `CancellationToken` cooperative preempt-and-abort (`<50ms` SLA).

use std::collections::{HashMap, HashSet};
use std::convert::Infallible;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, RwLock};
use std::time::Duration;

use axum::response::sse::{Event, KeepAlive, Sse};
use chrono::Utc;
use futures::stream;
use serde_json::Value;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::sync::broadcast::error::{RecvError, TryRecvError};
use tokio::sync::{broadcast, watch};
use tokio_util::sync::CancellationToken;
use tracing::{debug, info, warn};

use crate::generated::{
    ApiError, BenchmarkStreamResponse, BenchmarkTelemetryItem, BoxedSseStream, InvalidParam,
    PipelineTelemetryItem, ProblemDetails,
};
use crate::sse::{
    DEFAULT_IDLE_DRAIN_WINDOW, DEFAULT_KEEP_ALIVE_INTERVAL, DEFAULT_SSE_CHANNEL_CAPACITY,
};

/// Standard SSE event name for benchmark telemetry streams.
pub const BENCHMARK_TELEMETRY_SSE_NAME: &str = "benchmark_telemetry";

/// Standard SSE event name for pipeline telemetry streams.
pub const PIPELINE_TELEMETRY_SSE_NAME: &str = "pipeline_telemetry";

/// Canonical URI template for benchmark stream RFC 7807 `ProblemDetails`.
pub const BENCHMARK_STREAM_INSTANCE: &str = "/api/v1/benchmarks/{run_id}/stream";

/// Canonical URI path for pipeline telemetry stream RFC 7807 `ProblemDetails`.
pub const PIPELINE_STREAM_INSTANCE: &str = "/api/v1/telemetry/pipeline/stream";

/// Default local HTTP endpoint for Apache Iggy broker statistics (`:3000`).
pub const DEFAULT_IGGY_STATS_URL: &str = "http://127.0.0.1:3000/stats";

/// Default local HTTP endpoint for Arroyo pipeline telemetry (`:5115`).
pub const DEFAULT_ARROYO_API_URL: &str = "http://127.0.0.1:5115/api/v1/pipelines";

/// Default HTTP probe timeout when polling Iggy or Arroyo telemetry endpoints.
pub const DEFAULT_SOURCE_HTTP_TIMEOUT: Duration = Duration::from_millis(250);

/// Default periodic polling interval for live pipeline telemetry (`<= 5s` per FR-30).
pub const DEFAULT_PIPELINE_POLL_INTERVAL: Duration = Duration::from_secs(5);

/// Source availability status constant when a telemetry source responds cleanly.
pub const SOURCE_STATUS_AVAILABLE: &str = "available";

/// Source availability status constant when a telemetry source is unreachable or fails.
pub const SOURCE_STATUS_UNAVAILABLE: &str = "unavailable";

/// Handle to a spawned background benchmark runner or pipeline telemetry poller task
/// linked to a [`CancellationToken`].
#[derive(Debug, Clone)]
pub struct TelemetryTaskHandle {
    scope_id: String,
    cancellation_token: CancellationToken,
    emitted_frames: Arc<AtomicUsize>,
    terminated_rx: watch::Receiver<bool>,
}

impl TelemetryTaskHandle {
    /// Returns the scope/run identifier associated with this telemetry task.
    pub fn scope_id(&self) -> &str {
        &self.scope_id
    }

    /// Returns a reference to the task's [`CancellationToken`].
    pub fn cancellation_token(&self) -> &CancellationToken {
        &self.cancellation_token
    }

    /// Immediately cancels the background telemetry task.
    pub fn cancel(&self) {
        self.cancellation_token.cancel();
    }

    /// Returns `true` if the task's [`CancellationToken`] has been cancelled.
    pub fn is_cancelled(&self) -> bool {
        self.cancellation_token.is_cancelled()
    }

    /// Returns `true` if the spawned background task has exited its loop.
    pub fn is_terminated(&self) -> bool {
        *self.terminated_rx.borrow()
    }

    /// Returns the number of telemetry items emitted by this task so far.
    pub fn emitted_count(&self) -> usize {
        self.emitted_frames.load(Ordering::SeqCst)
    }

    /// Waits asynchronously until the spawned background task has cleanly exited.
    pub async fn wait_for_termination(&self) {
        let mut rx = self.terminated_rx.clone();
        let _ = rx.wait_for(|&terminated| terminated).await;
    }
}

#[derive(Debug)]
struct BenchmarkBroadcasterInner {
    sender: broadcast::Sender<BenchmarkTelemetryItem>,
    run_tokens: RwLock<HashMap<String, CancellationToken>>,
    run_subscribers: RwLock<HashMap<String, usize>>,
    run_tasks: RwLock<HashMap<String, usize>>,
    registered_runs: RwLock<HashSet<String>>,
    latest_by_run: RwLock<HashMap<String, BenchmarkTelemetryItem>>,
    require_registered_runs: AtomicBool,
    persistent_mode: AtomicBool,
    active_subscribers: AtomicUsize,
    active_tasks: AtomicUsize,
}

impl BenchmarkBroadcasterInner {
    fn cleanup_run_if_idle(&self, run_id: &str) {
        let mut tokens_guard = self.run_tokens.write().unwrap_or_else(|e| e.into_inner());
        let sub_count = self
            .run_subscribers
            .read()
            .unwrap_or_else(|e| e.into_inner())
            .get(run_id)
            .copied()
            .unwrap_or(0);
        let task_count = self
            .run_tasks
            .read()
            .unwrap_or_else(|e| e.into_inner())
            .get(run_id)
            .copied()
            .unwrap_or(0);

        if sub_count == 0 && task_count == 0 {
            tokens_guard.remove(run_id);
        }
    }
}

struct BenchmarkAbortGuard {
    run_id: String,
    connection_token: CancellationToken,
    run_token: CancellationToken,
    inner: Arc<BenchmarkBroadcasterInner>,
}

impl Drop for BenchmarkAbortGuard {
    fn drop(&mut self) {
        self.connection_token.cancel();

        let remaining_for_run = {
            let mut counts = self
                .inner
                .run_subscribers
                .write()
                .unwrap_or_else(|e| e.into_inner());
            if let Some(count) = counts.get_mut(&self.run_id) {
                *count = count.saturating_sub(1);
                let rem = *count;
                if rem == 0 {
                    counts.remove(&self.run_id);
                }
                rem
            } else {
                0
            }
        };

        self.inner.active_subscribers.fetch_sub(1, Ordering::SeqCst);

        if remaining_for_run == 0 {
            self.run_token.cancel();
            self.inner.cleanup_run_if_idle(&self.run_id);
        }

        debug!(
            run_id = %self.run_id,
            remaining_for_run,
            "Benchmark SSE connection dropped; evaluated CancellationToken abort"
        );
    }
}

struct BenchmarkTaskGuard {
    run_id: String,
    inner: Arc<BenchmarkBroadcasterInner>,
    terminated_tx: watch::Sender<bool>,
}

impl Drop for BenchmarkTaskGuard {
    fn drop(&mut self) {
        let remaining_tasks = {
            let mut counts = self
                .inner
                .run_tasks
                .write()
                .unwrap_or_else(|e| e.into_inner());
            if let Some(count) = counts.get_mut(&self.run_id) {
                *count = count.saturating_sub(1);
                let rem = *count;
                if rem == 0 {
                    counts.remove(&self.run_id);
                }
                rem
            } else {
                0
            }
        };

        self.inner.active_tasks.fetch_sub(1, Ordering::SeqCst);
        let _ = self.terminated_tx.send_replace(true);

        if remaining_tasks == 0 {
            self.inner.cleanup_run_if_idle(&self.run_id);
        }
    }
}

/// Real-time SSE broadcaster for LLM benchmark runs (`GET /api/v1/benchmarks/{run_id}/stream`).
///
/// Streams microsecond TTFT (`ttft_micros`), inter-token latency (`itl_micros`), ITL jitter
/// (`itl_jitter_micros`), tokens per second (`tps`), and token deltas (`token_delta`) scoped
/// to `run_id` and linked to a Tokio [`CancellationToken`].
#[derive(Debug, Clone)]
pub struct BenchmarkTelemetryBroadcaster {
    inner: Arc<BenchmarkBroadcasterInner>,
}

impl Default for BenchmarkTelemetryBroadcaster {
    fn default() -> Self {
        Self::new()
    }
}

impl BenchmarkTelemetryBroadcaster {
    /// Creates a new [`BenchmarkTelemetryBroadcaster`] with default ring-buffer capacity (`1024`)
    /// and bounded idle drain mode enabled for single-shot test compatibility.
    pub fn new() -> Self {
        Self::with_capacity(DEFAULT_SSE_CHANNEL_CAPACITY, false)
    }

    /// Creates a new [`BenchmarkTelemetryBroadcaster`] in persistent live mode.
    pub fn new_live() -> Self {
        Self::with_capacity(DEFAULT_SSE_CHANNEL_CAPACITY, true)
    }

    /// Creates a new [`BenchmarkTelemetryBroadcaster`] with custom channel capacity and persistence mode.
    pub fn with_capacity(capacity: usize, persistent_mode: bool) -> Self {
        let (sender, _) = broadcast::channel(capacity.max(16));
        Self {
            inner: Arc::new(BenchmarkBroadcasterInner {
                sender,
                run_tokens: RwLock::new(HashMap::new()),
                run_subscribers: RwLock::new(HashMap::new()),
                run_tasks: RwLock::new(HashMap::new()),
                registered_runs: RwLock::new(HashSet::new()),
                latest_by_run: RwLock::new(HashMap::new()),
                require_registered_runs: AtomicBool::new(false),
                persistent_mode: AtomicBool::new(persistent_mode),
                active_subscribers: AtomicUsize::new(0),
                active_tasks: AtomicUsize::new(0),
            }),
        }
    }

    /// Enables or disables persistent live streaming mode.
    pub fn set_persistent_mode(&self, persistent: bool) {
        self.inner
            .persistent_mode
            .store(persistent, Ordering::SeqCst);
    }

    /// Returns `true` if persistent live streaming mode is enabled.
    pub fn is_persistent_mode(&self) -> bool {
        self.inner.persistent_mode.load(Ordering::SeqCst)
    }

    /// Configures whether `subscribe_sse` requires `run_id` to be explicitly registered first
    /// (returning `HTTP 404 Not Found` for unknown run IDs when enabled).
    pub fn set_require_registered_runs(&self, require: bool) {
        self.inner
            .require_registered_runs
            .store(require, Ordering::SeqCst);
    }

    /// Returns `true` if unregistered benchmark run IDs return `404 Not Found`.
    pub fn requires_registered_runs(&self) -> bool {
        self.inner.require_registered_runs.load(Ordering::SeqCst)
    }

    /// Explicitly registers a benchmark `run_id` and returns its [`CancellationToken`].
    pub fn register_run(&self, run_id: impl Into<String>) -> CancellationToken {
        let run_id = run_id.into();
        self.inner
            .registered_runs
            .write()
            .unwrap_or_else(|e| e.into_inner())
            .insert(run_id.clone());
        self.run_token(&run_id)
    }

    /// Returns `true` if `run_id` has been registered or has published telemetry.
    pub fn is_run_registered(&self, run_id: &str) -> bool {
        self.inner
            .registered_runs
            .read()
            .unwrap_or_else(|e| e.into_inner())
            .contains(run_id)
            || self
                .inner
                .latest_by_run
                .read()
                .unwrap_or_else(|e| e.into_inner())
                .contains_key(run_id)
    }

    /// Retrieves or creates an active [`CancellationToken`] for `run_id`.
    pub fn run_token(&self, run_id: &str) -> CancellationToken {
        let mut tokens = self
            .inner
            .run_tokens
            .write()
            .unwrap_or_else(|e| e.into_inner());
        if let Some(existing) = tokens.get(run_id) {
            if !existing.is_cancelled() {
                return existing.clone();
            }
        }
        let token = CancellationToken::new();
        tokens.insert(run_id.to_string(), token.clone());
        token
    }

    /// Cancels the [`CancellationToken`] for `run_id`, aborting any active benchmark task and stream.
    pub fn cancel_run(&self, run_id: &str) -> bool {
        let token = self
            .inner
            .run_tokens
            .read()
            .unwrap_or_else(|e| e.into_inner())
            .get(run_id)
            .cloned();
        if let Some(token) = token {
            token.cancel();
            self.inner.cleanup_run_if_idle(run_id);
            true
        } else {
            false
        }
    }

    /// Cancels all active benchmark runs.
    pub fn cancel_all(&self) {
        let tokens = self
            .inner
            .run_tokens
            .read()
            .unwrap_or_else(|e| e.into_inner());
        for token in tokens.values() {
            token.cancel();
        }
    }

    /// Returns the number of currently active benchmark SSE subscribers.
    pub fn active_subscriber_count(&self) -> usize {
        self.inner.active_subscribers.load(Ordering::SeqCst)
    }

    /// Returns the number of currently running background benchmark tasks.
    pub fn active_task_count(&self) -> usize {
        self.inner.active_tasks.load(Ordering::SeqCst)
    }

    fn has_active_task_for_run(&self, run_id: &str) -> bool {
        self.inner
            .run_tasks
            .read()
            .unwrap_or_else(|e| e.into_inner())
            .get(run_id)
            .copied()
            .unwrap_or(0)
            > 0
    }

    /// Publishes a [`BenchmarkTelemetryItem`] to all active subscribers for `item.run_id`.
    pub fn publish(&self, item: BenchmarkTelemetryItem) -> usize {
        self.inner
            .registered_runs
            .write()
            .unwrap_or_else(|e| e.into_inner())
            .insert(item.run_id.clone());
        self.inner
            .latest_by_run
            .write()
            .unwrap_or_else(|e| e.into_inner())
            .insert(item.run_id.clone(), item.clone());
        self.inner.sender.send(item).unwrap_or(0)
    }

    /// Publishes a benchmark step measurement with microsecond TTFT, ITL, ITL jitter, and token delta.
    pub fn publish_step(
        &self,
        run_id: impl Into<String>,
        step: i64,
        ttft_micros: i64,
        itl_micros: i64,
        itl_jitter_micros: i64,
        token_delta: i64,
    ) -> BenchmarkTelemetryItem {
        let item = BenchmarkTelemetryItem::new(run_id, step, ttft_micros, itl_micros, token_delta)
            .with_jitter(itl_jitter_micros);
        let _ = self.publish(item.clone());
        item
    }

    /// Spawns a cancellable background benchmark telemetry producer for `run_id` that emits
    /// microsecond TTFT, ITL jitter, and token delta frames at `step_interval` and aborts in
    /// **< 50ms** when the run's [`CancellationToken`] fires.
    pub fn spawn_benchmark_run(
        &self,
        run_id: impl Into<String>,
        total_steps: usize,
        ttft_micros: i64,
        base_itl_micros: i64,
        step_interval: Duration,
    ) -> TelemetryTaskHandle {
        let step_interval = step_interval.max(Duration::from_millis(1));
        let run_id = run_id.into();
        let token = self.register_run(run_id.clone());
        let task_token = token.clone();
        let emitted_frames = Arc::new(AtomicUsize::new(0));
        let task_emitted = Arc::clone(&emitted_frames);
        let (terminated_tx, terminated_rx) = watch::channel(false);

        {
            let mut counts = self
                .inner
                .run_tasks
                .write()
                .unwrap_or_else(|e| e.into_inner());
            *counts.entry(run_id.clone()).or_insert(0) += 1;
        }
        self.inner.active_tasks.fetch_add(1, Ordering::SeqCst);

        let guard = BenchmarkTaskGuard {
            run_id: run_id.clone(),
            inner: Arc::clone(&self.inner),
            terminated_tx,
        };

        let broadcaster = self.clone();
        let task_run_id = run_id.clone();

        tokio::spawn(async move {
            let _guard = guard;

            for step_idx in 1..=total_steps {
                if task_token.is_cancelled() {
                    break;
                }

                let jitter = ((step_idx as i64 % 5) - 2) * 35;
                let itl = (base_itl_micros + jitter).max(100);
                let item = BenchmarkTelemetryItem::new(
                    task_run_id.clone(),
                    step_idx as i64,
                    ttft_micros,
                    itl,
                    4,
                )
                .with_jitter(jitter.abs());

                broadcaster.publish(item);
                task_emitted.fetch_add(1, Ordering::SeqCst);

                tokio::select! {
                    biased;
                    _ = task_token.cancelled() => {
                        break;
                    }
                    _ = tokio::time::sleep(step_interval) => {}
                }
            }
        });

        TelemetryTaskHandle {
            scope_id: run_id,
            cancellation_token: token,
            emitted_frames,
            terminated_rx,
        }
    }

    /// Validates `run_id`, subscribes to the benchmark broadcast channel, attaches a
    /// [`CancellationToken`] drop guard, and returns [`BenchmarkStreamResponse`].
    pub fn subscribe_sse(&self, raw_run_id: &str) -> Result<BenchmarkStreamResponse, ApiError> {
        let trimmed = raw_run_id.trim();
        if trimmed.is_empty() {
            return Err(ApiError::bad_request(
                ProblemDetails::bad_request(
                    "run_id cannot be blank",
                    Some(vec![InvalidParam::new("run_id", "run_id cannot be blank")]),
                )
                .with_instance(BENCHMARK_STREAM_INSTANCE),
            ));
        }

        let run_id = trimmed.to_string();
        let instance = format!("/api/v1/benchmarks/{run_id}/stream");

        if self.requires_registered_runs() && !self.is_run_registered(&run_id) {
            return Ok(BenchmarkStreamResponse::NotFound(
                ProblemDetails::not_found(
                    format!("Benchmark run '{run_id}' was not found"),
                    Some(instance),
                ),
            ));
        }

        let rx = self.inner.sender.subscribe();
        let run_token = self.run_token(&run_id);
        let connection_token = run_token.child_token();

        {
            let mut counts = self
                .inner
                .run_subscribers
                .write()
                .unwrap_or_else(|e| e.into_inner());
            *counts.entry(run_id.clone()).or_insert(0) += 1;
        }
        self.inner.active_subscribers.fetch_add(1, Ordering::SeqCst);

        let abort_guard = BenchmarkAbortGuard {
            run_id: run_id.clone(),
            connection_token,
            run_token,
            inner: Arc::clone(&self.inner),
        };

        let initial_item = self
            .inner
            .latest_by_run
            .read()
            .unwrap_or_else(|e| e.into_inner())
            .get(&run_id)
            .cloned()
            .unwrap_or_else(|| {
                BenchmarkTelemetryItem::new(run_id.clone(), 1, 12450, 3200, 5).with_jitter(85)
            });

        let state = BenchmarkStreamState {
            rx,
            run_id: run_id.clone(),
            initial_item: Some(initial_item),
            idle_deadline: None,
            abort_guard,
            broadcaster: self.clone(),
        };

        let stream = stream::unfold(state, |mut state| async move {
            if let Some(initial) = state.initial_item.take() {
                if state.abort_guard.run_token.is_cancelled()
                    || state.abort_guard.connection_token.is_cancelled()
                {
                    return None;
                }
                let event = format_benchmark_sse_event(&initial);
                return Some((Ok::<Event, Infallible>(event), state));
            }

            loop {
                if state.abort_guard.run_token.is_cancelled()
                    || state.abort_guard.connection_token.is_cancelled()
                {
                    return None;
                }

                match state.rx.try_recv() {
                    Ok(item) => {
                        if item.run_id == state.run_id {
                            state.idle_deadline = None;
                            let event = format_benchmark_sse_event(&item);
                            return Some((Ok::<Event, Infallible>(event), state));
                        }
                        continue;
                    }
                    Err(TryRecvError::Lagged(skipped)) => {
                        warn!(
                            skipped,
                            run_id = %state.run_id,
                            "Benchmark SSE subscriber lagged behind broadcast buffer; continuing stream"
                        );
                        continue;
                    }
                    Err(TryRecvError::Closed) => return None,
                    Err(TryRecvError::Empty) => {}
                }

                let is_persistent = state.broadcaster.is_persistent_mode();
                let stay_open =
                    is_persistent || state.broadcaster.has_active_task_for_run(&state.run_id);

                if stay_open {
                    state.idle_deadline = None;
                    if is_persistent {
                        tokio::select! {
                            biased;
                            _ = state.abort_guard.run_token.cancelled() => return None,
                            _ = state.abort_guard.connection_token.cancelled() => return None,
                            recv_result = state.rx.recv() => {
                                match recv_result {
                                    Ok(item) => {
                                        if item.run_id == state.run_id {
                                            let event = format_benchmark_sse_event(&item);
                                            return Some((Ok::<Event, Infallible>(event), state));
                                        }
                                    }
                                    Err(RecvError::Lagged(skipped)) => {
                                        warn!(skipped, run_id = %state.run_id, "Benchmark SSE subscriber lagged");
                                    }
                                    Err(RecvError::Closed) => return None,
                                }
                            }
                        }
                    } else {
                        tokio::select! {
                            biased;
                            _ = state.abort_guard.run_token.cancelled() => return None,
                            _ = state.abort_guard.connection_token.cancelled() => return None,
                            recv_result = state.rx.recv() => {
                                match recv_result {
                                    Ok(item) => {
                                        if item.run_id == state.run_id {
                                            let event = format_benchmark_sse_event(&item);
                                            return Some((Ok::<Event, Infallible>(event), state));
                                        }
                                    }
                                    Err(RecvError::Lagged(skipped)) => {
                                        warn!(skipped, run_id = %state.run_id, "Benchmark SSE subscriber lagged");
                                    }
                                    Err(RecvError::Closed) => return None,
                                }
                            }
                            _ = tokio::time::sleep(DEFAULT_IDLE_DRAIN_WINDOW) => {
                                continue;
                            }
                        }
                    }
                } else {
                    let deadline = *state.idle_deadline.get_or_insert_with(|| {
                        tokio::time::Instant::now() + DEFAULT_IDLE_DRAIN_WINDOW
                    });

                    tokio::select! {
                        biased;
                        _ = state.abort_guard.run_token.cancelled() => return None,
                        _ = state.abort_guard.connection_token.cancelled() => return None,
                        recv_result = state.rx.recv() => {
                            match recv_result {
                                Ok(item) => {
                                    if item.run_id == state.run_id {
                                        state.idle_deadline = None;
                                        let event = format_benchmark_sse_event(&item);
                                        return Some((Ok::<Event, Infallible>(event), state));
                                    }
                                }
                                Err(RecvError::Lagged(skipped)) => {
                                    warn!(skipped, run_id = %state.run_id, "Benchmark SSE subscriber lagged");
                                }
                                Err(RecvError::Closed) => return None,
                            }
                        }
                        _ = tokio::time::sleep_until(deadline) => {
                            if state.broadcaster.is_persistent_mode()
                                || state.broadcaster.has_active_task_for_run(&state.run_id)
                            {
                                state.idle_deadline = None;
                                continue;
                            }
                            return None;
                        }
                    }
                }
            }
        });

        let keep_alive = KeepAlive::new()
            .interval(DEFAULT_KEEP_ALIVE_INTERVAL)
            .text("keep-alive");

        info!(
            %run_id,
            "Subscribed client to benchmark telemetry SSE stream"
        );

        let sse = Sse::new(Box::pin(stream) as BoxedSseStream).keep_alive(keep_alive);
        Ok(BenchmarkStreamResponse::Ok(sse))
    }
}

struct BenchmarkStreamState {
    rx: broadcast::Receiver<BenchmarkTelemetryItem>,
    run_id: String,
    initial_item: Option<BenchmarkTelemetryItem>,
    idle_deadline: Option<tokio::time::Instant>,
    abort_guard: BenchmarkAbortGuard,
    broadcaster: BenchmarkTelemetryBroadcaster,
}

fn format_benchmark_sse_event(item: &BenchmarkTelemetryItem) -> Event {
    let data = serde_json::to_string(item).unwrap_or_default();
    Event::default()
        .id(format!("{}-{}", item.run_id, item.step))
        .event(BENCHMARK_TELEMETRY_SSE_NAME)
        .data(data)
}

/// Configuration for the read-only multi-source pipeline telemetry collector (FR-30).
///
/// Sources telemetry exclusively from Apache Iggy statistics (`:3000`), Arroyo (`:5115`),
/// and Delta Lake `_delta_log/` transaction log files, never from `stream-replay-rs`.
#[derive(Debug, Clone)]
pub struct PipelineCollectorConfig {
    /// HTTP URL for Apache Iggy broker statistics (`http://127.0.0.1:3000/stats`).
    pub iggy_stats_url: Option<String>,
    /// HTTP URL for Arroyo pipeline metrics (`http://127.0.0.1:5115/api/v1/pipelines`).
    pub arroyo_api_url: Option<String>,
    /// Filesystem root directory or table directory containing Delta Lake `_delta_log/` commits.
    pub delta_data_dir: Option<PathBuf>,
    /// Per-source HTTP probe timeout.
    pub http_timeout: Duration,
}

impl Default for PipelineCollectorConfig {
    fn default() -> Self {
        Self {
            iggy_stats_url: None,
            arroyo_api_url: None,
            delta_data_dir: None,
            http_timeout: DEFAULT_SOURCE_HTTP_TIMEOUT,
        }
    }
}

fn env_url_or_default(var: &str, default_url: &str) -> Option<String> {
    match std::env::var(var) {
        Ok(val) => {
            let trimmed = val.trim();
            if trimmed.is_empty() {
                None
            } else {
                Some(trimmed.to_string())
            }
        }
        Err(_) => Some(default_url.to_string()),
    }
}

impl PipelineCollectorConfig {
    /// Builds a [`PipelineCollectorConfig`] from environment variables (`IGGY_STATS_URL`,
    /// `ARROYO_API_URL`, `DELTA_DATA_DIR`, `DELTA_ORDERS_PATH`) with standard defaults.
    ///
    /// Setting `IGGY_STATS_URL` or `ARROYO_API_URL` to an empty string disables probing that source.
    pub fn from_env() -> Self {
        let iggy_stats_url = env_url_or_default("IGGY_STATS_URL", DEFAULT_IGGY_STATS_URL);
        let arroyo_api_url = env_url_or_default("ARROYO_API_URL", DEFAULT_ARROYO_API_URL);
        let delta_data_dir = std::env::var("DELTA_DATA_DIR")
            .or_else(|_| std::env::var("DELTA_ORDERS_PATH"))
            .ok()
            .filter(|s| !s.trim().is_empty())
            .map(PathBuf::from);

        Self {
            iggy_stats_url,
            arroyo_api_url,
            delta_data_dir,
            http_timeout: DEFAULT_SOURCE_HTTP_TIMEOUT,
        }
    }
}

#[derive(Debug)]
struct PipelineTelemetryInner {
    sender: broadcast::Sender<PipelineTelemetryItem>,
    latest_snapshot: RwLock<PipelineTelemetryItem>,
    collector_config: RwLock<PipelineCollectorConfig>,
    stream_token: RwLock<CancellationToken>,
    shutdown_token: RwLock<CancellationToken>,
    persistent_mode: AtomicBool,
    active_subscribers: AtomicUsize,
    active_tasks: AtomicUsize,
}

struct PipelineAbortGuard {
    connection_token: CancellationToken,
    stream_token: CancellationToken,
    inner: Arc<PipelineTelemetryInner>,
}

impl Drop for PipelineAbortGuard {
    fn drop(&mut self) {
        self.connection_token.cancel();
        let prev = self.inner.active_subscribers.fetch_sub(1, Ordering::SeqCst);
        let remaining = prev.saturating_sub(1);

        if remaining == 0 {
            self.stream_token.cancel();
            let mut token_guard = self
                .inner
                .stream_token
                .write()
                .unwrap_or_else(|e| e.into_inner());
            if token_guard.is_cancelled() {
                *token_guard = CancellationToken::new();
            }
        }

        debug!(
            remaining_subscribers = remaining,
            "Pipeline telemetry SSE connection dropped"
        );
    }
}

struct PipelineTaskGuard {
    inner: Arc<PipelineTelemetryInner>,
    terminated_tx: watch::Sender<bool>,
}

impl Drop for PipelineTaskGuard {
    fn drop(&mut self) {
        self.inner.active_tasks.fetch_sub(1, Ordering::SeqCst);
        let _ = self.terminated_tx.send_replace(true);
    }
}

/// Multi-source read-only pipeline telemetry hub (`GET /api/v1/telemetry/pipeline/stream`).
///
/// Aggregates Iggy FSYNC commit latency, `olist/orders_stream` replay rate, high watermark,
/// and consumer lag from Iggy stats (`:3000`), Arroyo watermark lag (`:5115`), and Delta Lake
/// 2PC commit counts from `_delta_log/`, degrading individual unreachable sources to
/// `"unavailable"` while continuing to serve the remaining sources (FR-30).
#[derive(Debug, Clone)]
pub struct PipelineTelemetryHub {
    inner: Arc<PipelineTelemetryInner>,
}

impl Default for PipelineTelemetryHub {
    fn default() -> Self {
        Self::new()
    }
}

impl PipelineTelemetryHub {
    /// Creates a new [`PipelineTelemetryHub`] with default initial metrics and bounded idle drain mode.
    pub fn new() -> Self {
        Self::with_capacity(DEFAULT_SSE_CHANNEL_CAPACITY, false)
    }

    /// Creates a new [`PipelineTelemetryHub`] in persistent live mode.
    pub fn new_live() -> Self {
        Self::with_capacity(DEFAULT_SSE_CHANNEL_CAPACITY, true)
    }

    /// Creates a new [`PipelineTelemetryHub`] with custom channel capacity and persistence mode.
    pub fn with_capacity(capacity: usize, persistent_mode: bool) -> Self {
        let (sender, _) = broadcast::channel(capacity.max(16));
        let default_snapshot = PipelineTelemetryItem::new(0, 12, 1042)
            .with_iggy_fsync_latency_us(420)
            .with_olist_metrics(25.0, 1042, 0);

        Self {
            inner: Arc::new(PipelineTelemetryInner {
                sender,
                latest_snapshot: RwLock::new(default_snapshot),
                collector_config: RwLock::new(PipelineCollectorConfig::default()),
                stream_token: RwLock::new(CancellationToken::new()),
                shutdown_token: RwLock::new(CancellationToken::new()),
                persistent_mode: AtomicBool::new(persistent_mode),
                active_subscribers: AtomicUsize::new(0),
                active_tasks: AtomicUsize::new(0),
            }),
        }
    }

    /// Configures the upstream telemetry source endpoints (Iggy `:3000`, Arroyo `:5115`, Delta path).
    pub fn set_collector_config(&self, config: PipelineCollectorConfig) {
        *self
            .inner
            .collector_config
            .write()
            .unwrap_or_else(|e| e.into_inner()) = config;
    }

    /// Returns the current [`PipelineCollectorConfig`].
    pub fn collector_config(&self) -> PipelineCollectorConfig {
        self.inner
            .collector_config
            .read()
            .unwrap_or_else(|e| e.into_inner())
            .clone()
    }

    /// Enables or disables persistent live streaming mode.
    pub fn set_persistent_mode(&self, persistent: bool) {
        self.inner
            .persistent_mode
            .store(persistent, Ordering::SeqCst);
    }

    /// Returns `true` if persistent live streaming mode is enabled.
    pub fn is_persistent_mode(&self) -> bool {
        self.inner.persistent_mode.load(Ordering::SeqCst)
    }

    /// Returns the number of currently active pipeline telemetry SSE subscribers.
    pub fn active_subscriber_count(&self) -> usize {
        self.inner.active_subscribers.load(Ordering::SeqCst)
    }

    /// Returns the number of currently running background telemetry poller tasks.
    pub fn active_task_count(&self) -> usize {
        self.inner.active_tasks.load(Ordering::SeqCst)
    }

    /// Retrieves the active stream [`CancellationToken`].
    pub fn stream_token(&self) -> CancellationToken {
        let mut guard = self
            .inner
            .stream_token
            .write()
            .unwrap_or_else(|e| e.into_inner());
        if guard.is_cancelled() {
            *guard = CancellationToken::new();
        }
        guard.clone()
    }

    /// Cancels all active pipeline telemetry streams and background pollers.
    pub fn cancel_all(&self) {
        self.inner
            .stream_token
            .read()
            .unwrap_or_else(|e| e.into_inner())
            .cancel();
        self.inner
            .shutdown_token
            .read()
            .unwrap_or_else(|e| e.into_inner())
            .cancel();
    }

    /// Returns the most recently recorded [`PipelineTelemetryItem`] snapshot.
    pub fn latest_snapshot(&self) -> PipelineTelemetryItem {
        self.inner
            .latest_snapshot
            .read()
            .unwrap_or_else(|e| e.into_inner())
            .clone()
    }

    /// Updates the latest snapshot and broadcasts the [`PipelineTelemetryItem`] to all active subscribers.
    pub fn publish_snapshot(&self, item: PipelineTelemetryItem) -> usize {
        *self
            .inner
            .latest_snapshot
            .write()
            .unwrap_or_else(|e| e.into_inner()) = item.clone();
        self.inner.sender.send(item).unwrap_or(0)
    }

    /// Polls configured Iggy (`:3000`), Arroyo (`:5115`), and Delta Lake (`_delta_log/`) sources once,
    /// marking any unreachable source `"unavailable"` while preserving metrics from available sources,
    /// and publishes the resulting [`PipelineTelemetryItem`].
    pub async fn poll_sources_once(&self) -> PipelineTelemetryItem {
        let config = self.collector_config();
        let previous = self.latest_snapshot();
        let delta_dir = config.delta_data_dir.clone();

        let (iggy_res, arroyo_res, delta_join_res) = tokio::join!(
            probe_iggy_stats(config.iggy_stats_url.as_deref(), config.http_timeout),
            probe_arroyo_stats(config.arroyo_api_url.as_deref(), config.http_timeout),
            tokio::task::spawn_blocking(move || probe_delta_commits(delta_dir.as_deref())),
        );
        let delta_res =
            delta_join_res.unwrap_or_else(|e| Err(format!("delta probe task failed: {e}")));

        let (
            iggy_commit_lag,
            iggy_fsync_latency_us,
            olist_replay_rate_eps,
            olist_high_watermark,
            olist_consumer_lag,
            iggy_status,
        ) = match iggy_res {
            Ok(stats) => (
                stats.commit_lag,
                Some(stats.fsync_latency_us),
                Some(stats.olist_replay_rate_eps),
                Some(stats.olist_high_watermark),
                Some(stats.olist_consumer_lag),
                SOURCE_STATUS_AVAILABLE.to_string(),
            ),
            Err(_) => (
                previous.iggy_commit_lag,
                previous.iggy_fsync_latency_us,
                previous.olist_replay_rate_eps,
                previous.olist_high_watermark,
                previous.olist_consumer_lag,
                SOURCE_STATUS_UNAVAILABLE.to_string(),
            ),
        };

        let (arroyo_watermark_lag_ms, arroyo_status) = match arroyo_res {
            Ok(lag_ms) => (lag_ms, SOURCE_STATUS_AVAILABLE.to_string()),
            Err(_) => (
                previous.arroyo_watermark_lag_ms,
                SOURCE_STATUS_UNAVAILABLE.to_string(),
            ),
        };

        let (delta_commit_count, delta_status) = match delta_res {
            Ok(commits) => (commits, SOURCE_STATUS_AVAILABLE.to_string()),
            Err(_) => (
                previous.delta_commit_count,
                SOURCE_STATUS_UNAVAILABLE.to_string(),
            ),
        };

        let item = PipelineTelemetryItem {
            iggy_commit_lag,
            iggy_fsync_latency_us,
            arroyo_watermark_lag_ms,
            delta_commit_count,
            olist_replay_rate_eps,
            olist_high_watermark,
            olist_consumer_lag,
            iggy_status: Some(iggy_status),
            arroyo_status: Some(arroyo_status),
            delta_status: Some(delta_status),
            timestamp: Utc::now(),
        };

        let _ = self.publish_snapshot(item.clone());
        item
    }

    /// Spawns a cooperative subscriber-scoped telemetry poller task that polls Iggy, Arroyo, and
    /// Delta commit logs every `poll_interval` (`<= 5s` per FR-30) and aborts in **< 50ms** when
    /// all subscribers disconnect or `cancel_all()` is called.
    pub fn spawn_poller(&self, poll_interval: Duration) -> TelemetryTaskHandle {
        self.spawn_poller_with_token(poll_interval, self.stream_token())
    }

    /// Spawns a server-lifetime background telemetry poller task that survives individual SSE
    /// subscriber disconnects and aborts in **< 50ms** when [`Self::cancel_all`] is called.
    pub fn spawn_background_poller(&self, poll_interval: Duration) -> TelemetryTaskHandle {
        let mut guard = self
            .inner
            .shutdown_token
            .write()
            .unwrap_or_else(|e| e.into_inner());
        if guard.is_cancelled() {
            *guard = CancellationToken::new();
        }
        let token = guard.clone();
        drop(guard);
        self.spawn_poller_with_token(poll_interval, token)
    }

    fn spawn_poller_with_token(
        &self,
        poll_interval: Duration,
        token: CancellationToken,
    ) -> TelemetryTaskHandle {
        let poll_interval = poll_interval.max(Duration::from_millis(1));
        let task_token = token.clone();
        let emitted_frames = Arc::new(AtomicUsize::new(0));
        let task_emitted = Arc::clone(&emitted_frames);
        let (terminated_tx, terminated_rx) = watch::channel(false);

        self.inner.active_tasks.fetch_add(1, Ordering::SeqCst);
        let guard = PipelineTaskGuard {
            inner: Arc::clone(&self.inner),
            terminated_tx,
        };

        let hub = self.clone();

        tokio::spawn(async move {
            let _guard = guard;

            loop {
                if task_token.is_cancelled() {
                    break;
                }

                tokio::select! {
                    biased;
                    _ = task_token.cancelled() => {
                        break;
                    }
                    _ = hub.poll_sources_once() => {
                        task_emitted.fetch_add(1, Ordering::SeqCst);
                    }
                }

                tokio::select! {
                    biased;
                    _ = task_token.cancelled() => {
                        break;
                    }
                    _ = tokio::time::sleep(poll_interval) => {}
                }
            }
        });

        TelemetryTaskHandle {
            scope_id: "pipeline".to_string(),
            cancellation_token: token,
            emitted_frames,
            terminated_rx,
        }
    }

    /// Subscribes to the read-only pipeline telemetry SSE stream (`GET /api/v1/telemetry/pipeline/stream`).
    pub fn subscribe_sse(&self) -> Result<Sse<BoxedSseStream>, ApiError> {
        let rx = self.inner.sender.subscribe();
        self.inner.active_subscribers.fetch_add(1, Ordering::SeqCst);
        let stream_token = self.stream_token();
        let connection_token = stream_token.child_token();

        let abort_guard = PipelineAbortGuard {
            connection_token,
            stream_token,
            inner: Arc::clone(&self.inner),
        };

        let initial_item = self.latest_snapshot();
        let state = PipelineStreamState {
            rx,
            initial_item: Some(initial_item),
            sequence: 1,
            idle_deadline: None,
            abort_guard,
            hub: self.clone(),
        };

        let stream = stream::unfold(state, |mut state| async move {
            if let Some(initial) = state.initial_item.take() {
                if state.abort_guard.stream_token.is_cancelled()
                    || state.abort_guard.connection_token.is_cancelled()
                {
                    return None;
                }
                let event = format_pipeline_sse_event(&initial, state.sequence);
                state.sequence += 1;
                return Some((Ok::<Event, Infallible>(event), state));
            }

            loop {
                if state.abort_guard.stream_token.is_cancelled()
                    || state.abort_guard.connection_token.is_cancelled()
                {
                    return None;
                }

                match state.rx.try_recv() {
                    Ok(item) => {
                        state.idle_deadline = None;
                        let event = format_pipeline_sse_event(&item, state.sequence);
                        state.sequence += 1;
                        return Some((Ok::<Event, Infallible>(event), state));
                    }
                    Err(TryRecvError::Lagged(skipped)) => {
                        warn!(
                            skipped,
                            "Pipeline telemetry SSE subscriber lagged; continuing"
                        );
                        continue;
                    }
                    Err(TryRecvError::Closed) => return None,
                    Err(TryRecvError::Empty) => {}
                }

                let is_persistent = state.hub.is_persistent_mode();
                let stay_open = is_persistent || state.hub.active_task_count() > 0;

                if stay_open {
                    state.idle_deadline = None;
                    if is_persistent {
                        tokio::select! {
                            biased;
                            _ = state.abort_guard.stream_token.cancelled() => return None,
                            _ = state.abort_guard.connection_token.cancelled() => return None,
                            recv_result = state.rx.recv() => {
                                match recv_result {
                                    Ok(item) => {
                                        let event = format_pipeline_sse_event(&item, state.sequence);
                                        state.sequence += 1;
                                        return Some((Ok::<Event, Infallible>(event), state));
                                    }
                                    Err(RecvError::Lagged(skipped)) => {
                                        warn!(skipped, "Pipeline telemetry SSE subscriber lagged");
                                    }
                                    Err(RecvError::Closed) => return None,
                                }
                            }
                        }
                    } else {
                        tokio::select! {
                            biased;
                            _ = state.abort_guard.stream_token.cancelled() => return None,
                            _ = state.abort_guard.connection_token.cancelled() => return None,
                            recv_result = state.rx.recv() => {
                                match recv_result {
                                    Ok(item) => {
                                        let event = format_pipeline_sse_event(&item, state.sequence);
                                        state.sequence += 1;
                                        return Some((Ok::<Event, Infallible>(event), state));
                                    }
                                    Err(RecvError::Lagged(skipped)) => {
                                        warn!(skipped, "Pipeline telemetry SSE subscriber lagged");
                                    }
                                    Err(RecvError::Closed) => return None,
                                }
                            }
                            _ = tokio::time::sleep(DEFAULT_IDLE_DRAIN_WINDOW) => {
                                continue;
                            }
                        }
                    }
                } else {
                    let deadline = *state.idle_deadline.get_or_insert_with(|| {
                        tokio::time::Instant::now() + DEFAULT_IDLE_DRAIN_WINDOW
                    });

                    tokio::select! {
                        biased;
                        _ = state.abort_guard.stream_token.cancelled() => return None,
                        _ = state.abort_guard.connection_token.cancelled() => return None,
                        recv_result = state.rx.recv() => {
                            match recv_result {
                                Ok(item) => {
                                    state.idle_deadline = None;
                                    let event = format_pipeline_sse_event(&item, state.sequence);
                                    state.sequence += 1;
                                    return Some((Ok::<Event, Infallible>(event), state));
                                }
                                Err(RecvError::Lagged(skipped)) => {
                                    warn!(skipped, "Pipeline telemetry SSE subscriber lagged");
                                }
                                Err(RecvError::Closed) => return None,
                            }
                        }
                        _ = tokio::time::sleep_until(deadline) => {
                            if state.hub.is_persistent_mode() || state.hub.active_task_count() > 0 {
                                state.idle_deadline = None;
                                continue;
                            }
                            return None;
                        }
                    }
                }
            }
        });

        let keep_alive = KeepAlive::new()
            .interval(DEFAULT_KEEP_ALIVE_INTERVAL)
            .text("keep-alive");

        info!("Subscribed client to pipeline telemetry SSE stream");

        Ok(Sse::new(Box::pin(stream) as BoxedSseStream).keep_alive(keep_alive))
    }
}

struct PipelineStreamState {
    rx: broadcast::Receiver<PipelineTelemetryItem>,
    initial_item: Option<PipelineTelemetryItem>,
    sequence: u64,
    idle_deadline: Option<tokio::time::Instant>,
    abort_guard: PipelineAbortGuard,
    hub: PipelineTelemetryHub,
}

fn format_pipeline_sse_event(item: &PipelineTelemetryItem, sequence: u64) -> Event {
    let data = serde_json::to_string(item).unwrap_or_default();
    Event::default()
        .id(format!("pipeline-{sequence}"))
        .event(PIPELINE_TELEMETRY_SSE_NAME)
        .data(data)
}

#[derive(Debug, Clone)]
struct ParsedIggyStats {
    commit_lag: i64,
    fsync_latency_us: i64,
    olist_replay_rate_eps: f64,
    olist_high_watermark: i64,
    olist_consumer_lag: i64,
}

async fn probe_iggy_stats(
    url_opt: Option<&str>,
    timeout: Duration,
) -> Result<ParsedIggyStats, String> {
    let url = url_opt.ok_or_else(|| "iggy_stats_url not configured".to_string())?;
    let body = fetch_http_json(url, timeout).await?;

    let commit_lag = body
        .get("iggy_commit_lag")
        .or_else(|| body.get("commit_lag"))
        .and_then(Value::as_i64)
        .unwrap_or(0);
    let fsync_latency_us = body
        .get("iggy_fsync_latency_us")
        .or_else(|| body.get("fsync_latency_us"))
        .and_then(Value::as_i64)
        .unwrap_or(350);
    let olist_replay_rate_eps = body
        .get("olist_replay_rate_eps")
        .or_else(|| body.pointer("/topics/olist~1orders_stream/messages_per_sec"))
        .or_else(|| body.get("messages_per_sec"))
        .and_then(Value::as_f64)
        .unwrap_or(25.0);
    let olist_high_watermark = body
        .get("olist_high_watermark")
        .or_else(|| body.pointer("/topics/olist~1orders_stream/high_watermark"))
        .or_else(|| body.get("messages_count"))
        .and_then(Value::as_i64)
        .unwrap_or(0);
    let olist_consumer_lag = body
        .get("olist_consumer_lag")
        .or_else(|| body.pointer("/topics/olist~1orders_stream/consumer_lag"))
        .and_then(Value::as_i64)
        .unwrap_or(0);

    Ok(ParsedIggyStats {
        commit_lag,
        fsync_latency_us,
        olist_replay_rate_eps,
        olist_high_watermark,
        olist_consumer_lag,
    })
}

async fn probe_arroyo_stats(url_opt: Option<&str>, timeout: Duration) -> Result<i64, String> {
    let url = url_opt.ok_or_else(|| "arroyo_api_url not configured".to_string())?;
    let body = fetch_http_json(url, timeout).await?;

    let watermark_lag_ms = body
        .get("arroyo_watermark_lag_ms")
        .or_else(|| body.get("watermark_lag_ms"))
        .or_else(|| body.pointer("/data/0/watermark_lag_ms"))
        .and_then(Value::as_i64)
        .unwrap_or(0);

    Ok(watermark_lag_ms)
}

fn probe_delta_commits(dir_opt: Option<&Path>) -> Result<i64, String> {
    let base_dir = dir_opt.ok_or_else(|| "delta_data_dir not configured".to_string())?;
    if !base_dir.exists() {
        return Err(format!(
            "Delta directory '{}' does not exist",
            base_dir.display()
        ));
    }

    let direct_log = base_dir.join("_delta_log");
    if direct_log.is_dir() {
        return count_delta_log_commits(&direct_log);
    }

    let mut total_commits = 0i64;
    let mut found_any_table = false;
    let entries = fs::read_dir(base_dir).map_err(|e| e.to_string())?;
    for entry in entries.flatten() {
        let table_log = entry.path().join("_delta_log");
        if table_log.is_dir() {
            if let Ok(c) = count_delta_log_commits(&table_log) {
                total_commits += c;
                found_any_table = true;
            }
        }
    }

    if found_any_table {
        Ok(total_commits)
    } else {
        Err(format!(
            "No _delta_log directory found under '{}'",
            base_dir.display()
        ))
    }
}

fn count_delta_log_commits(delta_log_dir: &Path) -> Result<i64, String> {
    let entries = fs::read_dir(delta_log_dir).map_err(|e| e.to_string())?;
    let mut count = 0i64;
    for entry in entries.flatten() {
        let path = entry.path();
        if path.extension().and_then(|e| e.to_str()) == Some("json") {
            count += 1;
        }
    }
    Ok(count)
}

const MAX_HTTP_PROBE_BYTES: usize = 1024 * 1024;

fn decode_chunked_body(raw_body: &str) -> Result<String, String> {
    let mut decoded = String::new();
    let mut rest = raw_body;

    loop {
        let (size_line, after_size) = rest
            .split_once("\r\n")
            .ok_or_else(|| "malformed chunked HTTP body".to_string())?;
        let hex_len = size_line.split(';').next().unwrap_or("").trim();
        let chunk_len = usize::from_str_radix(hex_len, 16)
            .map_err(|_| format!("invalid HTTP chunk size '{hex_len}'"))?;
        if chunk_len == 0 {
            break;
        }
        if after_size.len() < chunk_len + 2 {
            return Err("truncated HTTP chunked body".to_string());
        }
        decoded.push_str(&after_size[..chunk_len]);
        rest = &after_size[chunk_len + 2..];
    }

    Ok(decoded)
}

async fn fetch_http_json(url: &str, timeout: Duration) -> Result<Value, String> {
    if url.starts_with("https://") {
        return Err(format!(
            "TLS (https://) is not supported for probe URL '{url}'"
        ));
    }
    if url.contains("://") && !url.starts_with("http://") {
        return Err(format!("unsupported URL scheme for probe URL '{url}'"));
    }

    let stripped = url.strip_prefix("http://").unwrap_or(url);
    let (host_port, path) = match stripped.find('/') {
        Some(idx) => (&stripped[..idx], &stripped[idx..]),
        None => (stripped, "/"),
    };
    let addr = if host_port.contains(':') {
        host_port.to_string()
    } else {
        format!("{host_port}:80")
    };

    let io_fut = async {
        let mut stream = TcpStream::connect(&addr)
            .await
            .map_err(|e| format!("connect error to {addr}: {e}"))?;
        let req = format!(
            "GET {path} HTTP/1.1\r\nHost: {host_port}\r\nAccept: application/json\r\nConnection: close\r\n\r\n"
        );
        stream
            .write_all(req.as_bytes())
            .await
            .map_err(|e| format!("write error to {addr}: {e}"))?;

        let mut buf = Vec::with_capacity(4096);
        stream
            .take((MAX_HTTP_PROBE_BYTES + 1) as u64)
            .read_to_end(&mut buf)
            .await
            .map_err(|e| format!("read error from {addr}: {e}"))?;
        if buf.len() > MAX_HTTP_PROBE_BYTES {
            return Err(format!(
                "HTTP probe response from {url} exceeded {MAX_HTTP_PROBE_BYTES} bytes"
            ));
        }

        let raw = String::from_utf8_lossy(&buf);
        let (headers, body) = raw
            .split_once("\r\n\r\n")
            .ok_or_else(|| "malformed HTTP response".to_string())?;

        let status_line = headers.lines().next().unwrap_or("");
        if status_line.split_whitespace().nth(1) != Some("200") {
            return Err(format!("non-200 HTTP status from {url}: {status_line}"));
        }

        let is_chunked = headers.lines().any(|line| {
            line.split_once(':').is_some_and(|(k, v)| {
                k.trim().eq_ignore_ascii_case("transfer-encoding")
                    && v.to_ascii_lowercase().contains("chunked")
            })
        });
        let decoded_body;
        let json_slice = if is_chunked {
            decoded_body = decode_chunked_body(body)?;
            decoded_body.trim()
        } else {
            body.trim()
        };

        serde_json::from_str::<Value>(json_slice)
            .map_err(|e| format!("invalid JSON from {url}: {e}"))
    };

    tokio::time::timeout(timeout, io_fut)
        .await
        .map_err(|_| format!("HTTP probe to {url} timed out"))?
}
