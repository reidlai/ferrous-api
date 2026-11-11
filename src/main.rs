//! Standalone HTTP server entrypoint for ferrous-api service.

use std::net::SocketAddr;
use std::sync::Arc;

use ferrous_api::{
    create_router_with_app_state, AppState, IggyPublisher, InMemoryIggyPublisher, JwtValidator,
    TcpIggyPublisher,
};
use tokio::signal;
use tracing::info;
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
    let app_state = AppState::new(publisher, jwt_validator);

    let addr = SocketAddr::from(([0, 0, 0, 0], port));
    let app = create_router_with_app_state(app_state);

    info!(%addr, "Bootstrapping FerrousCX Axum API Service");

    let listener = tokio::net::TcpListener::bind(addr).await?;
    info!(%addr, "Listening for HTTP connections");

    axum::serve(listener, app)
        .with_graceful_shutdown(shutdown_signal())
        .await?;

    info!("FerrousCX Axum API Service shut down gracefully");
    Ok(())
}

async fn shutdown_signal() {
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
}
