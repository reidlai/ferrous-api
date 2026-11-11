//! Apache Iggy WAL publisher abstraction and implementations.
//!
//! Provides [`IggyPublisher`], [`PublishedMessage`], [`InMemoryIggyPublisher`] for zero-dependency
//! test isolation, and [`TcpIggyPublisher`] for live TCP broker command dispatch to `orders/intake`
//! partitioned strictly by `order_id`.

use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc, RwLock,
};
use std::time::Duration;

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use ferrous_cqrses::commands::OrderCommand;
use serde::{Deserialize, Serialize};
use thiserror::Error;
use tokio::io::AsyncWriteExt;
use tokio::net::TcpStream;
use tokio::time::timeout;
use tracing::{debug, error};
use uuid::Uuid;

use crate::generated::models::ProblemDetails;

/// Canonical Apache Iggy WAL stream/topic path for order mutation commands.
pub const ORDERS_INTAKE_TOPIC: &str = "orders/intake";

/// Errors occurring during command serialization or Apache Iggy WAL publication.
#[derive(Debug, Error, Clone, PartialEq, Eq)]
pub enum PublisherError {
    #[error("Failed to serialize OrderCommand payload: {0}")]
    Serialization(String),

    #[error("Apache Iggy broker unavailable: {0}")]
    BrokerUnavailable(String),

    #[error("I/O error writing to Apache Iggy WAL: {0}")]
    Io(String),
}

impl PublisherError {
    /// Converts a publisher failure into an RFC 7807 `ProblemDetails` response envelope
    /// with `status: 500` and `type: "urn:ferrous:error:broker-unavailable"`.
    pub fn into_problem_details(self, instance: Option<String>) -> ProblemDetails {
        ProblemDetails {
            r#type: "urn:ferrous:error:broker-unavailable".to_string(),
            title: "Internal Server Error".to_string(),
            status: 500,
            detail: Some(self.to_string()),
            instance,
            code: Some("BROKER_UNAVAILABLE".to_string()),
            invalid_params: None,
        }
    }
}

/// A message published to Apache Iggy WAL (`orders/intake`).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PublishedMessage {
    /// Target Iggy stream/topic identifier (e.g., `"orders/intake"`).
    pub topic: String,
    /// Partitioning key used by Iggy to route messages (strictly `order_id`).
    pub partition_key: String,
    /// Monotonically ordered UUIDv7 identifying the command.
    pub command_id: Uuid,
    /// Serialized `OrderCommand` JSON bytes.
    pub payload: Vec<u8>,
    /// UTC timestamp when the message was published.
    pub published_at: DateTime<Utc>,
}

impl PublishedMessage {
    /// Deserializes the raw message payload back into a strongly typed [`OrderCommand`].
    pub fn decode_command(&self) -> Result<OrderCommand, serde_json::Error> {
        serde_json::from_slice(&self.payload)
    }
}

/// Abstraction for publishing strongly typed [`OrderCommand`] events to Apache Iggy WAL.
#[async_trait]
pub trait IggyPublisher: Send + Sync + std::fmt::Debug {
    /// Publishes raw serialized command bytes to the specified topic and partition key.
    async fn publish_raw(
        &self,
        topic: &str,
        partition_key: &str,
        command_id: Uuid,
        payload: Vec<u8>,
    ) -> Result<(), PublisherError>;

    /// Serializes an [`OrderCommand`] and publishes it to `orders/intake` partitioned strictly by `order_id`.
    async fn publish_command(&self, command: &OrderCommand) -> Result<(), PublisherError> {
        let payload = serde_json::to_vec(command)
            .map_err(|e| PublisherError::Serialization(e.to_string()))?;

        self.publish_raw(
            ORDERS_INTAKE_TOPIC,
            command.order_id(),
            command.command_id(),
            payload,
        )
        .await
    }
}

/// Thread-safe in-memory Apache Iggy publisher for unit and integration testing without external sockets.
///
/// Stores published messages in an `Arc<RwLock<Vec<PublishedMessage>>>` as specified in the architecture.
#[derive(Debug, Clone)]
pub struct InMemoryIggyPublisher {
    messages: Arc<RwLock<Vec<PublishedMessage>>>,
    should_fail: Arc<AtomicBool>,
}

impl Default for InMemoryIggyPublisher {
    fn default() -> Self {
        Self::new()
    }
}

impl InMemoryIggyPublisher {
    /// Creates a new empty `InMemoryIggyPublisher`.
    pub fn new() -> Self {
        Self {
            messages: Arc::new(RwLock::new(Vec::new())),
            should_fail: Arc::new(AtomicBool::new(false)),
        }
    }

    /// Creates an `InMemoryIggyPublisher` configured to simulate broker unavailability errors.
    pub fn failing() -> Self {
        Self {
            messages: Arc::new(RwLock::new(Vec::new())),
            should_fail: Arc::new(AtomicBool::new(true)),
        }
    }

    /// Configures whether subsequent publish calls should simulate a broker error.
    pub fn set_should_fail(&self, fail: bool) {
        self.should_fail.store(fail, Ordering::SeqCst);
    }

    /// Returns a clone of the underlying `Arc<RwLock<Vec<PublishedMessage>>>` storage handle.
    pub fn messages_handle(&self) -> Arc<RwLock<Vec<PublishedMessage>>> {
        Arc::clone(&self.messages)
    }

    /// Returns a snapshot of all published messages recorded so far.
    pub fn messages(&self) -> Vec<PublishedMessage> {
        self.messages
            .read()
            .map(|guard| guard.clone())
            .unwrap_or_default()
    }

    /// Returns the number of published messages recorded in memory.
    pub fn len(&self) -> usize {
        self.messages.read().map(|guard| guard.len()).unwrap_or(0)
    }

    /// Returns `true` if no messages have been published yet.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Clears all recorded messages.
    pub fn clear(&self) {
        if let Ok(mut guard) = self.messages.write() {
            guard.clear();
        }
    }
}

#[async_trait]
impl IggyPublisher for InMemoryIggyPublisher {
    async fn publish_raw(
        &self,
        topic: &str,
        partition_key: &str,
        command_id: Uuid,
        payload: Vec<u8>,
    ) -> Result<(), PublisherError> {
        if self.should_fail.load(Ordering::SeqCst) {
            return Err(PublisherError::BrokerUnavailable(
                "Simulated Apache Iggy broker connection failure".to_string(),
            ));
        }

        let msg = PublishedMessage {
            topic: topic.to_string(),
            partition_key: partition_key.to_string(),
            command_id,
            payload,
            published_at: Utc::now(),
        };

        let mut guard = self
            .messages
            .write()
            .map_err(|_| PublisherError::Io("InMemoryIggyPublisher lock poisoned".to_string()))?;
        guard.push(msg);

        debug!(
            %command_id,
            topic,
            partition_key,
            "Recorded OrderCommand in InMemoryIggyPublisher"
        );
        Ok(())
    }
}

/// Live TCP Apache Iggy publisher dispatching framed command messages over native TCP sockets.
#[derive(Debug, Clone)]
pub struct TcpIggyPublisher {
    broker_address: String,
}

impl TcpIggyPublisher {
    /// Creates a new `TcpIggyPublisher` targeting the given Iggy TCP broker address (e.g., `"127.0.0.1:8090"`).
    pub fn new(broker_address: impl Into<String>) -> Self {
        Self {
            broker_address: broker_address.into(),
        }
    }

    /// Returns the configured Iggy TCP broker address.
    pub fn broker_address(&self) -> &str {
        &self.broker_address
    }
}

#[async_trait]
impl IggyPublisher for TcpIggyPublisher {
    async fn publish_raw(
        &self,
        topic: &str,
        partition_key: &str,
        command_id: Uuid,
        payload: Vec<u8>,
    ) -> Result<(), PublisherError> {
        let wire_envelope = PublishedMessage {
            topic: topic.to_string(),
            partition_key: partition_key.to_string(),
            command_id,
            payload,
            published_at: Utc::now(),
        };

        let frame_bytes = serde_json::to_vec(&wire_envelope)
            .map_err(|e| PublisherError::Serialization(e.to_string()))?;
        let frame_len = u32::try_from(frame_bytes.len())
            .map_err(|_| {
                PublisherError::Serialization(
                    "Serialized Iggy command frame exceeds u32::MAX bytes".to_string(),
                )
            })?
            .to_le_bytes();

        let broker_addr = self.broker_address.clone();
        timeout(Duration::from_millis(500), async move {
            let mut stream = TcpStream::connect(&broker_addr).await.map_err(|e| {
                error!(
                    broker = %broker_addr,
                    error = %e,
                    "Failed to connect to Apache Iggy TCP broker"
                );
                PublisherError::BrokerUnavailable(format!(
                    "Failed to connect to Apache Iggy broker at {}: {e}",
                    broker_addr
                ))
            })?;

            stream.write_all(&frame_len).await.map_err(|e| {
                PublisherError::Io(format!("Failed to write frame length to Iggy socket: {e}"))
            })?;
            stream.write_all(&frame_bytes).await.map_err(|e| {
                PublisherError::Io(format!("Failed to write command frame to Iggy socket: {e}"))
            })?;
            stream.flush().await.map_err(|e| {
                PublisherError::Io(format!("Failed to flush Iggy TCP socket: {e}"))
            })?;
            Ok(())
        })
        .await
        .map_err(|_| {
            PublisherError::BrokerUnavailable(format!(
                "Timed out publishing to Apache Iggy broker at {} (>500ms)",
                self.broker_address
            ))
        })??;

        debug!(
            %command_id,
            topic,
            partition_key,
            broker = %self.broker_address,
            "Dispatched OrderCommand over TCP to Apache Iggy WAL"
        );
        Ok(())
    }
}
