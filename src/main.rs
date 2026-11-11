//! Standalone HTTP server entrypoint for ferrous-api service.

use std::net::SocketAddr;
use std::path::Path;
use std::sync::Arc;

use ferrous_api::{
    create_router_with_app_state, AppState, IggyPublisher, InMemoryIggyPublisher, JwtValidator,
    SessionContext, TcpIggyPublisher, DEFAULT_DELTA_ORDERS_PATH, ORDERS_CURRENT_STATE_TABLE,
};
use tokio::signal;
use tracing::{info, warn};
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Initialize structured tracing subscriber
    tracing_subscriber::registry()
        .with(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "ferrous_api=info,tower_http=info".into()),
        )
        .with(tracing_subscriber::fmt::layer().json())
        .init();

    let port: u16 = std::env::var("PORT")
        .or_else(|_| std::env::var("FERROUS_API_PORT"))
        .ok()
        .and_then(|p| p.parse().ok())
        .unwrap_or(8080);

    let publisher: Arc<dyn IggyPublisher> = match std::env::var("IGGY_SERVER_ADDRESS") {
        Ok(addr) if !addr.trim().is_empty() => {
            let trimmed = addr.trim().to_string();
            info!(addr = %trimmed, "Configuring live TCP Apache Iggy WAL publisher");
            Arc::new(TcpIggyPublisher::new(trimmed))
        }
        _ => {
            info!("Configuring in-memory Apache Iggy WAL publisher");
            Arc::new(InMemoryIggyPublisher::new())
        }
    };

    let mut validator = match std::env::var("KANIDM_JWT_SECRET") {
        Ok(secret) if !secret.trim().is_empty() => JwtValidator::new_with_secret(
            ferrous_api::auth::DEFAULT_KANIDM_KID,
            secret.trim().as_bytes(),
        ),
        _ => JwtValidator::default(),
    };
    if let Ok(issuer) = std::env::var("KANIDM_ISSUER") {
        if !issuer.trim().is_empty() {
            validator = validator.with_issuer(issuer.trim());
        }
    }
    let jwt_validator = Arc::new(validator);

    let session_context = Arc::new(SessionContext::default());
    let delta_path = std::env::var("DELTA_ORDERS_PATH")
        .or_else(|_| std::env::var("DELTA_DATA_DIR").map(|d| format!("{d}/orders_current_state")))
        .unwrap_or_else(|_| DEFAULT_DELTA_ORDERS_PATH.to_string());

    if Path::new(&delta_path).join("_delta_log").exists() {
        match session_context.register_delta_table(ORDERS_CURRENT_STATE_TABLE, &delta_path) {
            Ok(meta) => {
                info!(
                    path = %delta_path,
                    version = meta.version,
                    files = meta.active_files.len(),
                    "Registered Delta Lake table in embedded DataFusion SessionContext"
                );
            }
            Err(err) => {
                warn!(
                    path = %delta_path,
                    error = %err,
                    "Failed to load Delta Lake directory; retaining seeded SessionContext table"
                );
            }
        }
    } else {
        info!(
            table = ORDERS_CURRENT_STATE_TABLE,
            "Initialized embedded DataFusion SessionContext with in-memory columnar table"
        );
    }

    let app_state = AppState::with_session_context(publisher, jwt_validator, session_context);
    app_state.event_broadcaster.set_persistent_mode(true);
    let event_broadcaster = Arc::clone(&app_state.event_broadcaster);

    let addr = SocketAddr::from(([0, 0, 0, 0], port));
    let app = create_router_with_app_state(app_state);

    info!(%addr, "Bootstrapping FerrousCX Axum API Service");

    let listener = tokio::net::TcpListener::bind(addr).await?;
    info!(%addr, "Listening for HTTP connections");

    axum::serve(listener, app)
        .with_graceful_shutdown(shutdown_signal(event_broadcaster))
        .await?;

    info!("FerrousCX Axum API Service shut down gracefully");
    Ok(())
}

async fn shutdown_signal(event_broadcaster: Arc<ferrous_api::EventStreamBroadcaster>) {
    let ctrl_c = async {
        signal::ctrl_c()
            .await
            .expect("Failed to install Ctrl+C signal handler");
    };

    #[cfg(unix)]
    let terminate = async {
        signal::unix::signal(signal::unix::SignalKind::terminate())
            .expect("Failed to install SIGTERM signal handler")
            .recv()
            .await;
    };

    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();

    tokio::select! {
        _ = ctrl_c => {
            info!("Received Ctrl+C interrupt signal; initiating graceful shutdown");
        },
        _ = terminate => {
            info!("Received SIGTERM terminate signal; initiating graceful shutdown");
        },
    }

    event_broadcaster.cancel_all();
}
