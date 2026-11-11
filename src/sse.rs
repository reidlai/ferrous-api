//! Native Axum Server-Sent Events (SSE) broadcaster and Tokio `CancellationToken`
//! cooperative preempt-and-abort hub (`<50ms` task cancellation SLA).

use std::collections::{HashMap, HashSet};
use std::convert::Infallible;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, RwLock};
use std::time::Duration;

use axum::response::sse::{Event, KeepAlive, Sse};
use chrono::Utc;
use ferrous_cqrses::id::generate_uuidv7;
use futures::stream;
use serde_json::json;
use tokio::sync::broadcast::error::{RecvError, TryRecvError};
use tokio::sync::{broadcast, watch};
use tokio_util::sync::CancellationToken;
use tracing::{debug, info, warn};

use crate::generated::{
    ApiError, BoxedSseStream, DomainEventStreamItem, EventStreamParams, InvalidParam,
    ProblemDetails,
};

/// Default ring-buffer capacity for the in-process domain event broadcast channel.
pub const DEFAULT_SSE_CHANNEL_CAPACITY: usize = 1024;

/// Default SSE keep-alive ping interval.
pub const DEFAULT_KEEP_ALIVE_INTERVAL: Duration = Duration::from_secs(15);

/// Default idle drain window for non-persistent streams when no agent tasks are running.
pub const DEFAULT_IDLE_DRAIN_WINDOW: Duration = Duration::from_millis(25);

/// Standard SSE event name for domain events and LLM agent token streams.
pub const DOMAIN_EVENT_SSE_NAME: &str = "domain_event";

/// Default global scope identifier when no `order_id` query filter is provided.
pub const GLOBAL_STREAM_SCOPE: &str = "global";

/// Canonical URI instance path for the domain event stream endpoint.
pub const EVENT_STREAM_INSTANCE: &str = "/api/v1/events/stream";

impl ProblemDetails {
    /// Sets the RFC 7807 `instance` URI reference on [`ProblemDetails`].
    pub fn with_instance(mut self, instance: impl Into<String>) -> Self {
        self.instance = Some(instance.into());
        self
    }
}

/// Parsed and validated filter predicates for `GET /api/v1/events/stream`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EventStreamFilter {
    order_id: Option<String>,
    event_types: Option<HashSet<String>>,
    scope_id: String,
}

impl EventStreamFilter {
    /// Validates [`EventStreamParams`] and constructs a typed [`EventStreamFilter`].
    ///
    /// Rejects blank or whitespace-only `order_id` or `event_types` values with
    /// RFC 7807 [`ProblemDetails`] (`HTTP 400 Bad Request`).
    pub fn from_params(params: &EventStreamParams) -> Result<Self, Box<ProblemDetails>> {
        let mut errors = Vec::new();

        let order_id = match &params.order_id {
            Some(raw) => {
                let trimmed = raw.trim();
                if trimmed.is_empty() {
                    errors.push(InvalidParam::new("order_id", "order_id cannot be blank"));
                    None
                } else {
                    Some(trimmed.to_string())
                }
            }
            None => None,
        };

        let event_types = match &params.event_types {
            Some(raw) => {
                let trimmed = raw.trim();
                if trimmed.is_empty() || raw.split(',').any(|segment| segment.trim().is_empty()) {
                    errors.push(InvalidParam::new(
                        "event_types",
                        "event_types cannot be blank or contain empty segments",
                    ));
                    None
                } else {
                    let set: HashSet<String> = raw
                        .split(',')
                        .map(|segment| segment.trim().to_string())
                        .collect();
                    Some(set)
                }
            }
            None => None,
        };

        if !errors.is_empty() {
            return Err(Box::new(
                ProblemDetails::bad_request(
                    "One or more query parameters failed validation for EventStreamParams",
                    Some(errors),
                )
                .with_instance(EVENT_STREAM_INSTANCE),
            ));
        }

        let scope_id = order_id
            .clone()
            .unwrap_or_else(|| GLOBAL_STREAM_SCOPE.to_string());

        Ok(Self {
            order_id,
            event_types,
            scope_id,
        })
    }

    /// Returns true if the given [`DomainEventStreamItem`] satisfies the `order_id` and
    /// `event_types` filter criteria.
    pub fn matches(&self, item: &DomainEventStreamItem) -> bool {
        if let Some(ref expected_order_id) = self.order_id {
            if &item.aggregate_id != expected_order_id {
                return false;
            }
        }

        if let Some(ref allowed_types) = self.event_types {
            if !allowed_types.contains(&item.event_type) {
                return false;
            }
        }

        true
    }

    /// Returns the scope identifier (`order_id` or `"global"`).
    pub fn scope_id(&self) -> &str {
        &self.scope_id
    }
}

/// Shared inner state for [`EventStreamBroadcaster`].
#[derive(Debug)]
struct EventStreamBroadcasterInner {
    sender: broadcast::Sender<DomainEventStreamItem>,
    scopes: RwLock<HashMap<String, CancellationToken>>,
    explicit_scopes: RwLock<HashSet<String>>,
    scope_subscribers: RwLock<HashMap<String, usize>>,
    scope_tasks: RwLock<HashMap<String, usize>>,
    persistent_mode: AtomicBool,
    active_subscribers: AtomicUsize,
    active_tasks: AtomicUsize,
}

impl EventStreamBroadcasterInner {
    fn scope_subscriber_count(&self, scope_id: &str) -> usize {
        self.scope_subscribers
            .read()
            .unwrap_or_else(|e| e.into_inner())
            .get(scope_id)
            .copied()
            .unwrap_or(0)
    }

    fn scope_task_count(&self, scope_id: &str) -> usize {
        self.scope_tasks
            .read()
            .unwrap_or_else(|e| e.into_inner())
            .get(scope_id)
            .copied()
            .unwrap_or(0)
    }

    fn cleanup_scope_if_idle(&self, scope_id: &str) {
        let subs = self.scope_subscriber_count(scope_id);
        let tasks = self.scope_task_count(scope_id);
        if tasks == 0 {
            self.explicit_scopes
                .write()
                .unwrap_or_else(|e| e.into_inner())
                .remove(scope_id);
        }
        if subs == 0 && tasks == 0 {
            self.scopes
                .write()
                .unwrap_or_else(|e| e.into_inner())
                .remove(scope_id);
        }
    }
}

/// RAII drop guard embedded inside each active SSE stream.
///
/// When the HTTP client disconnects or drops the SSE stream body, [`SseAbortGuard::drop`]
/// synchronously triggers `CancellationToken::cancel()` on the connection token and, when the
/// last subscriber for the scope disconnects, cancels the linked scope token to preempt any
/// running LLM agent task in **< 50 milliseconds**.
#[derive(Debug)]
pub struct SseAbortGuard {
    scope_id: String,
    connection_token: CancellationToken,
    scope_token: CancellationToken,
    inner: Arc<EventStreamBroadcasterInner>,
}

impl SseAbortGuard {
    /// Returns a reference to the connection-specific [`CancellationToken`].
    pub fn connection_token(&self) -> &CancellationToken {
        &self.connection_token
    }

    /// Returns a reference to the scope [`CancellationToken`] shared with agent tasks.
    pub fn scope_token(&self) -> &CancellationToken {
        &self.scope_token
    }

    /// Returns the scope identifier (`order_id` or `"global"`).
    pub fn scope_id(&self) -> &str {
        &self.scope_id
    }
}

impl Drop for SseAbortGuard {
    fn drop(&mut self) {
        self.connection_token.cancel();

        let scope_remaining = {
            let mut sub_counts = self
                .inner
                .scope_subscribers
                .write()
                .unwrap_or_else(|e| e.into_inner());
            if let Some(count) = sub_counts.get_mut(&self.scope_id) {
                *count = count.saturating_sub(1);
                let rem = *count;
                if rem == 0 {
                    sub_counts.remove(&self.scope_id);
                }
                rem
            } else {
                0
            }
        };

        let prev = self.inner.active_subscribers.fetch_sub(1, Ordering::SeqCst);
        let remaining = prev.saturating_sub(1);

        if scope_remaining == 0 {
            self.scope_token.cancel();
            self.inner
                .explicit_scopes
                .write()
                .unwrap_or_else(|e| e.into_inner())
                .remove(&self.scope_id);
            self.inner.cleanup_scope_if_idle(&self.scope_id);
        }

        // If a global subscriber disconnects and no subscribers remain anywhere, cancel all active
        // scopes so no spawned agent task is ever orphaned.
        if self.scope_id == GLOBAL_STREAM_SCOPE && remaining == 0 {
            self.inner
                .explicit_scopes
                .write()
                .unwrap_or_else(|e| e.into_inner())
                .clear();
            let active_task_scopes: HashSet<String> = self
                .inner
                .scope_tasks
                .read()
                .unwrap_or_else(|e| e.into_inner())
                .keys()
                .cloned()
                .collect();
            let mut scopes = self
                .inner
                .scopes
                .write()
                .unwrap_or_else(|e| e.into_inner());
            for token in scopes.values() {
                token.cancel();
            }
            scopes.retain(|k, _| active_task_scopes.contains(k));
        }

        debug!(
            scope_id = %self.scope_id,
            scope_remaining_subscribers = scope_remaining,
            total_remaining_subscribers = remaining,
            "SSE connection dropped; processed CancellationToken abort guard"
        );
    }
}

/// RAII guard tracking active background agent tasks on [`EventStreamBroadcaster`].
struct ActiveTaskGuard {
    scope_id: String,
    inner: Arc<EventStreamBroadcasterInner>,
    terminated_tx: watch::Sender<bool>,
}

impl Drop for ActiveTaskGuard {
    fn drop(&mut self) {
        let scope_remaining_tasks = {
            let mut task_counts = self
                .inner
                .scope_tasks
                .write()
                .unwrap_or_else(|e| e.into_inner());
            if let Some(count) = task_counts.get_mut(&self.scope_id) {
                *count = count.saturating_sub(1);
                let rem = *count;
                if rem == 0 {
                    task_counts.remove(&self.scope_id);
                }
                rem
            } else {
                0
            }
        };

        if scope_remaining_tasks == 0 {
            self.inner.cleanup_scope_if_idle(&self.scope_id);
        }

        self.inner.active_tasks.fetch_sub(1, Ordering::SeqCst);
        let _ = self.terminated_tx.send_replace(true);
    }
}

/// Handle to a spawned LLM agent thought/token streaming task linked to a [`CancellationToken`].
#[derive(Debug, Clone)]
pub struct AgentTaskHandle {
    scope_id: String,
    cancellation_token: CancellationToken,
    emitted_chunks: Arc<AtomicUsize>,
    terminated_rx: watch::Receiver<bool>,
}

impl AgentTaskHandle {
    /// Returns the scope/order identifier associated with this agent task.
    pub fn scope_id(&self) -> &str {
        &self.scope_id
    }

    /// Returns a reference to the task's [`CancellationToken`].
    pub fn cancellation_token(&self) -> &CancellationToken {
        &self.cancellation_token
    }

    /// Immediately cancels the agent task via its [`CancellationToken`].
    pub fn cancel(&self) {
        self.cancellation_token.cancel();
    }

    /// Returns `true` if the task's [`CancellationToken`] has been triggered.
    pub fn is_cancelled(&self) -> bool {
        self.cancellation_token.is_cancelled()
    }

    /// Returns `true` if the spawned agent task has exited its execution loop.
    pub fn is_terminated(&self) -> bool {
        *self.terminated_rx.borrow()
    }

    /// Returns the number of SSE chunks emitted by the agent task so far.
    pub fn emitted_count(&self) -> usize {
        self.emitted_chunks.load(Ordering::SeqCst)
    }

    /// Waits asynchronously until the spawned agent task has cleanly exited.
    pub async fn wait_for_termination(&self) {
        let mut rx = self.terminated_rx.clone();
        let _ = rx.wait_for(|&terminated| terminated).await;
    }
}

/// In-process multi-consumer SSE broadcast hub and Tokio [`CancellationToken`] registry.
#[derive(Debug, Clone)]
pub struct EventStreamBroadcaster {
    inner: Arc<EventStreamBroadcasterInner>,
}

impl Default for EventStreamBroadcaster {
    fn default() -> Self {
        Self::new()
    }
}

impl EventStreamBroadcaster {
    /// Creates a new [`EventStreamBroadcaster`] with default ring-buffer capacity (`1024`)
    /// and bounded idle drain mode enabled for single-shot test compatibility.
    pub fn new() -> Self {
        Self::with_capacity(DEFAULT_SSE_CHANNEL_CAPACITY, false)
    }

    /// Creates a new [`EventStreamBroadcaster`] in persistent live mode where SSE streams
    /// remain open indefinitely until cancelled or dropped by the client.
    pub fn new_live() -> Self {
        Self::with_capacity(DEFAULT_SSE_CHANNEL_CAPACITY, true)
    }

    /// Creates a new [`EventStreamBroadcaster`] with custom channel capacity and persistence mode.
    pub fn with_capacity(capacity: usize, persistent_mode: bool) -> Self {
        let (sender, _) = broadcast::channel(capacity.max(16));
        Self {
            inner: Arc::new(EventStreamBroadcasterInner {
                sender,
                scopes: RwLock::new(HashMap::new()),
                explicit_scopes: RwLock::new(HashSet::new()),
                scope_subscribers: RwLock::new(HashMap::new()),
                scope_tasks: RwLock::new(HashMap::new()),
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

    /// Returns the number of currently active SSE stream subscribers.
    pub fn active_subscriber_count(&self) -> usize {
        self.inner.active_subscribers.load(Ordering::SeqCst)
    }

    /// Returns the number of currently running background agent tasks.
    pub fn active_task_count(&self) -> usize {
        self.inner.active_tasks.load(Ordering::SeqCst)
    }

    /// Retrieves or creates an active (non-cancelled) [`CancellationToken`] for `scope_id`
    /// and marks the scope as explicitly registered.
    pub fn scope_token(&self, scope_id: &str) -> CancellationToken {
        self.inner
            .explicit_scopes
            .write()
            .unwrap_or_else(|e| e.into_inner())
            .insert(scope_id.to_string());
        self.get_or_create_scope_token(scope_id)
    }

    fn get_or_create_scope_token(&self, scope_id: &str) -> CancellationToken {
        let mut scopes = self
            .inner
            .scopes
            .write()
            .unwrap_or_else(|e| e.into_inner());

        if let Some(existing) = scopes.get(scope_id) {
            if !existing.is_cancelled() {
                return existing.clone();
            }
        }

        let token = CancellationToken::new();
        scopes.insert(scope_id.to_string(), token.clone());
        token
    }

    fn has_explicit_scope(&self, scope_id: &str) -> bool {
        self.inner
            .explicit_scopes
            .read()
            .unwrap_or_else(|e| e.into_inner())
            .contains(scope_id)
    }

    fn increment_scope_task(&self, scope_id: &str) {
        let mut task_counts = self
            .inner
            .scope_tasks
            .write()
            .unwrap_or_else(|e| e.into_inner());
        *task_counts.entry(scope_id.to_string()).or_insert(0) += 1;
        self.inner.active_tasks.fetch_add(1, Ordering::SeqCst);
    }

    /// Cancels the [`CancellationToken`] registered for `scope_id`, immediately preempting
    /// any linked SSE stream and running agent task in **< 50ms**.
    ///
    /// Returns `true` if an active scope token existed and was cancelled.
    pub fn cancel_scope(&self, scope_id: &str) -> bool {
        self.inner
            .explicit_scopes
            .write()
            .unwrap_or_else(|e| e.into_inner())
            .remove(scope_id);

        let token = self
            .inner
            .scopes
            .read()
            .unwrap_or_else(|e| e.into_inner())
            .get(scope_id)
            .cloned();

        if let Some(token) = token {
            token.cancel();
            self.inner.cleanup_scope_if_idle(scope_id);
            true
        } else {
            false
        }
    }

    /// Cancels all registered scope [`CancellationToken`]s across the broadcaster.
    pub fn cancel_all(&self) {
        self.inner
            .explicit_scopes
            .write()
            .unwrap_or_else(|e| e.into_inner())
            .clear();

        let active_sub_scopes: HashSet<String> = self
            .inner
            .scope_subscribers
            .read()
            .unwrap_or_else(|e| e.into_inner())
            .keys()
            .cloned()
            .collect();
        let active_task_scopes: HashSet<String> = self
            .inner
            .scope_tasks
            .read()
            .unwrap_or_else(|e| e.into_inner())
            .keys()
            .cloned()
            .collect();

        let mut scopes = self
            .inner
            .scopes
            .write()
            .unwrap_or_else(|e| e.into_inner());
        for token in scopes.values() {
            token.cancel();
        }
        scopes.retain(|k, _| active_sub_scopes.contains(k) || active_task_scopes.contains(k));
    }

    /// Returns `true` if the token for `scope_id` exists and has been cancelled.
    pub fn is_scope_cancelled(&self, scope_id: &str) -> bool {
        let scopes = self
            .inner
            .scopes
            .read()
            .unwrap_or_else(|e| e.into_inner());
        scopes
            .get(scope_id)
            .map(CancellationToken::is_cancelled)
            .unwrap_or(false)
    }

    /// Publishes a [`DomainEventStreamItem`] to all active SSE subscribers.
    ///
    /// Returns the number of active subscribers that received the broadcast (`0` when no
    /// subscribers are connected, succeeding as a non-failing no-op).
    pub fn publish(&self, item: DomainEventStreamItem) -> usize {
        self.inner.sender.send(item).unwrap_or(0)
    }

    /// Constructs and broadcasts a [`DomainEventStreamItem`] with a monotonic UUIDv7 `event_id`.
    pub fn publish_domain_event(
        &self,
        event_type: impl Into<String>,
        aggregate_id: impl Into<String>,
        payload: serde_json::Value,
    ) -> DomainEventStreamItem {
        let item = DomainEventStreamItem {
            event_id: generate_uuidv7(),
            event_type: event_type.into(),
            aggregate_id: aggregate_id.into(),
            payload,
            timestamp: Utc::now(),
        };
        let _ = self.publish(item.clone());
        item
    }

    /// Publishes an `AgentThought` chunk for the given `order_id`.
    pub fn publish_agent_thought(
        &self,
        order_id: impl Into<String>,
        thought: impl Into<String>,
    ) -> DomainEventStreamItem {
        let order_id = order_id.into();
        let thought = thought.into();
        self.publish_domain_event(
            "AgentThought",
            order_id,
            json!({
                "thought": thought,
            }),
        )
    }

    /// Publishes an `AgentToken` delta chunk for the given `order_id`.
    pub fn publish_agent_token(
        &self,
        order_id: impl Into<String>,
        token_chunk: impl Into<String>,
        step: usize,
    ) -> DomainEventStreamItem {
        let order_id = order_id.into();
        let token_chunk = token_chunk.into();
        self.publish_domain_event(
            "AgentToken",
            order_id,
            json!({
                "token": token_chunk,
                "step": step,
            }),
        )
    }

    /// Publishes an `AgentProposal` event for the given `order_id`.
    pub fn publish_agent_proposal(
        &self,
        order_id: impl Into<String>,
        proposal: serde_json::Value,
    ) -> DomainEventStreamItem {
        self.publish_domain_event("AgentProposal", order_id, proposal)
    }

    /// Spawns a cooperative, cancellable LLM agent task that streams the provided `(event_type, chunk)`
    /// items at `step_interval` and aborts in **< 50ms** when the scope's [`CancellationToken`] fires.
    pub fn spawn_agent_token_stream<I, S1, S2>(
        &self,
        order_id: impl Into<String>,
        chunks: I,
        step_interval: Duration,
    ) -> AgentTaskHandle
    where
        I: IntoIterator<Item = (S1, S2)>,
        S1: Into<String>,
        S2: Into<String>,
    {
        let scope_id = order_id.into();
        let prepared_chunks: Vec<(String, String)> = chunks
            .into_iter()
            .map(|(t, c)| (t.into(), c.into()))
            .collect();

        let token = self.scope_token(&scope_id);
        let task_token = token.clone();
        let emitted_chunks = Arc::new(AtomicUsize::new(0));
        let task_emitted = Arc::clone(&emitted_chunks);
        let (terminated_tx, terminated_rx) = watch::channel(false);

        self.increment_scope_task(&scope_id);
        let guard = ActiveTaskGuard {
            scope_id: scope_id.clone(),
            inner: Arc::clone(&self.inner),
            terminated_tx,
        };

        let broadcaster = self.clone();
        let task_scope_id = scope_id.clone();

        tokio::spawn(async move {
            let _guard = guard;

            for (idx, (event_type, content)) in prepared_chunks.into_iter().enumerate() {
                if task_token.is_cancelled() {
                    break;
                }

                if idx > 0 && !step_interval.is_zero() {
                    tokio::select! {
                        biased;
                        _ = task_token.cancelled() => {
                            break;
                        }
                        _ = tokio::time::sleep(step_interval) => {}
                    }
                }

                if task_token.is_cancelled() {
                    break;
                }

                let step = idx + 1;
                let payload = build_agent_chunk_payload(&event_type, &content, step);
                broadcaster.publish_domain_event(event_type, task_scope_id.clone(), payload);
                task_emitted.fetch_add(1, Ordering::SeqCst);
            }
        });

        AgentTaskHandle {
            scope_id,
            cancellation_token: token,
            emitted_chunks,
            terminated_rx,
        }
    }

    /// Spawns a continuous LLM agent token generator for `order_id` that emits `AgentThought`
    /// and `AgentToken` chunks every `step_interval` until preempted via its [`CancellationToken`].
    pub fn spawn_continuous_agent_stream(
        &self,
        order_id: impl Into<String>,
        step_interval: Duration,
    ) -> AgentTaskHandle {
        let step_interval = step_interval.max(Duration::from_millis(1));
        let scope_id = order_id.into();
        let token = self.scope_token(&scope_id);
        let task_token = token.clone();
        let emitted_chunks = Arc::new(AtomicUsize::new(0));
        let task_emitted = Arc::clone(&emitted_chunks);
        let (terminated_tx, terminated_rx) = watch::channel(false);

        self.increment_scope_task(&scope_id);
        let guard = ActiveTaskGuard {
            scope_id: scope_id.clone(),
            inner: Arc::clone(&self.inner),
            terminated_tx,
        };

        let broadcaster = self.clone();
        let task_scope_id = scope_id.clone();

        tokio::spawn(async move {
            let _guard = guard;
            let mut step = 0usize;

            loop {
                if task_token.is_cancelled() {
                    break;
                }

                step += 1;
                let event_type = if step == 1 {
                    "AgentThought"
                } else {
                    "AgentToken"
                };
                let text = format!("token-{step}");
                let payload = build_agent_chunk_payload(event_type, &text, step);

                broadcaster.publish_domain_event(event_type, task_scope_id.clone(), payload);
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

        AgentTaskHandle {
            scope_id,
            cancellation_token: token,
            emitted_chunks,
            terminated_rx,
        }
    }

    /// Validates [`EventStreamParams`], subscribes to the broadcast ring buffer, attaches
    /// an [`SseAbortGuard`] linked to the scope's [`CancellationToken`], and returns a
    /// native Axum [`Sse<BoxedSseStream>`] configured with [`KeepAlive`].
    pub fn subscribe_sse(&self, params: &EventStreamParams) -> Result<Sse<BoxedSseStream>, ApiError> {
        let filter = EventStreamFilter::from_params(params).map_err(ApiError::BadRequest)?;
        let scope_id = filter.scope_id().to_string();

        // Eagerly subscribe to the broadcast channel before returning the response so events
        // published immediately after HTTP connection establishment are buffered.
        let rx = self.inner.sender.subscribe();
        let scope_token = self.get_or_create_scope_token(&scope_id);
        let connection_token = scope_token.child_token();

        {
            let mut sub_counts = self
                .inner
                .scope_subscribers
                .write()
                .unwrap_or_else(|e| e.into_inner());
            *sub_counts.entry(scope_id.clone()).or_insert(0) += 1;
        }
        self.inner.active_subscribers.fetch_add(1, Ordering::SeqCst);

        let abort_guard = SseAbortGuard {
            scope_id: scope_id.clone(),
            connection_token: connection_token.clone(),
            scope_token: scope_token.clone(),
            inner: Arc::clone(&self.inner),
        };

        let initial_event = DomainEventStreamItem {
            event_id: generate_uuidv7(),
            event_type: "StreamConnected".to_string(),
            aggregate_id: scope_id.clone(),
            payload: json!({ "status": "active" }),
            timestamp: Utc::now(),
        };

        let broadcaster = self.clone();

        let state = SseStreamState {
            rx,
            filter,
            initial_event: Some(initial_event),
            idle_deadline: None,
            abort_guard,
            broadcaster,
        };

        let stream = stream::unfold(state, |mut state| async move {
            if let Some(initial) = state.initial_event.take() {
                if state.abort_guard.scope_token().is_cancelled()
                    || state.abort_guard.connection_token().is_cancelled()
                {
                    return None;
                }
                let event = format_sse_event(&initial);
                return Some((Ok::<Event, Infallible>(event), state));
            }

            loop {
                if state.abort_guard.scope_token().is_cancelled()
                    || state.abort_guard.connection_token().is_cancelled()
                {
                    return None;
                }

                // Drain any immediately queued broadcast items first without awaiting.
                match state.rx.try_recv() {
                    Ok(item) => {
                        if state.filter.matches(&item) {
                            state.idle_deadline = None;
                            let event = format_sse_event(&item);
                            return Some((Ok::<Event, Infallible>(event), state));
                        }
                        continue;
                    }
                    Err(TryRecvError::Lagged(skipped)) => {
                        warn!(
                            skipped,
                            scope_id = %state.abort_guard.scope_id(),
                            "SSE subscriber lagged behind broadcast buffer; continuing stream"
                        );
                        continue;
                    }
                    Err(TryRecvError::Closed) => return None,
                    Err(TryRecvError::Empty) => {}
                }

                let is_persistent = state.broadcaster.is_persistent_mode();
                let stay_open = is_persistent
                    || state.broadcaster.active_task_count() > 0
                    || state
                        .broadcaster
                        .has_explicit_scope(state.abort_guard.scope_id());

                if stay_open {
                    state.idle_deadline = None;
                    if is_persistent {
                        tokio::select! {
                            biased;
                            _ = state.abort_guard.scope_token().cancelled() => {
                                return None;
                            }
                            _ = state.abort_guard.connection_token().cancelled() => {
                                return None;
                            }
                            recv_result = state.rx.recv() => {
                                match recv_result {
                                    Ok(item) => {
                                        if state.filter.matches(&item) {
                                            let event = format_sse_event(&item);
                                            return Some((Ok::<Event, Infallible>(event), state));
                                        }
                                    }
                                    Err(RecvError::Lagged(skipped)) => {
                                        warn!(
                                            skipped,
                                            scope_id = %state.abort_guard.scope_id(),
                                            "SSE subscriber lagged behind broadcast buffer; continuing stream"
                                        );
                                    }
                                    Err(RecvError::Closed) => return None,
                                }
                            }
                        }
                    } else {
                        tokio::select! {
                            biased;
                            _ = state.abort_guard.scope_token().cancelled() => {
                                return None;
                            }
                            _ = state.abort_guard.connection_token().cancelled() => {
                                return None;
                            }
                            recv_result = state.rx.recv() => {
                                match recv_result {
                                    Ok(item) => {
                                        if state.filter.matches(&item) {
                                            let event = format_sse_event(&item);
                                            return Some((Ok::<Event, Infallible>(event), state));
                                        }
                                    }
                                    Err(RecvError::Lagged(skipped)) => {
                                        warn!(
                                            skipped,
                                            scope_id = %state.abort_guard.scope_id(),
                                            "SSE subscriber lagged behind broadcast buffer; continuing stream"
                                        );
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
                    let deadline = *state
                        .idle_deadline
                        .get_or_insert_with(|| tokio::time::Instant::now() + DEFAULT_IDLE_DRAIN_WINDOW);

                    tokio::select! {
                        biased;
                        _ = state.abort_guard.scope_token().cancelled() => {
                            return None;
                        }
                        _ = state.abort_guard.connection_token().cancelled() => {
                            return None;
                        }
                        recv_result = state.rx.recv() => {
                            match recv_result {
                                Ok(item) => {
                                    if state.filter.matches(&item) {
                                        state.idle_deadline = None;
                                        let event = format_sse_event(&item);
                                        return Some((Ok::<Event, Infallible>(event), state));
                                    }
                                }
                                Err(RecvError::Lagged(skipped)) => {
                                    warn!(
                                        skipped,
                                        scope_id = %state.abort_guard.scope_id(),
                                        "SSE subscriber lagged behind broadcast buffer; continuing stream"
                                    );
                                }
                                Err(RecvError::Closed) => return None,
                            }
                        }
                        _ = tokio::time::sleep_until(deadline) => {
                            if state.broadcaster.is_persistent_mode()
                                || state.broadcaster.active_task_count() > 0
                                || state
                                    .broadcaster
                                    .has_explicit_scope(state.abort_guard.scope_id())
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
            scope_id = %scope_id,
            "Subscribed SSE client with KeepAlive and CancellationToken SseAbortGuard"
        );

        Ok(Sse::new(Box::pin(stream) as BoxedSseStream).keep_alive(keep_alive))
    }
}

struct SseStreamState {
    rx: broadcast::Receiver<DomainEventStreamItem>,
    filter: EventStreamFilter,
    initial_event: Option<DomainEventStreamItem>,
    idle_deadline: Option<tokio::time::Instant>,
    abort_guard: SseAbortGuard,
    broadcaster: EventStreamBroadcaster,
}

fn build_agent_chunk_payload(event_type: &str, content: &str, step: usize) -> serde_json::Value {
    match event_type {
        "AgentThought" => json!({
            "thought": content,
            "chunk": content,
            "step": step,
        }),
        "AgentToken" => json!({
            "token": content,
            "chunk": content,
            "step": step,
        }),
        _ => json!({
            "chunk": content,
            "step": step,
        }),
    }
}

fn format_sse_event(item: &DomainEventStreamItem) -> Event {
    let data = serde_json::to_string(item).unwrap_or_default();
    Event::default()
        .id(item.event_id.to_string())
        .event(DOMAIN_EVENT_SSE_NAME)
        .data(data)
}
