//! Handlers module root.
//!
//! Provides pure-Rust implementations of generated API server traits.

pub mod orders;
pub mod queries;
pub mod streams;
pub mod webhooks;

pub use orders::OrdersHandler;
pub use queries::QueriesHandler;
pub use streams::StreamsHandler;
pub use webhooks::{
    handle_settlement_callback, SettlementCallbackPayload, SettlementCallbackResponse,
    SettlementStatus,
};
