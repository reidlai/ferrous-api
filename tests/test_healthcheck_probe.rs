use std::net::TcpListener as StdTcpListener;
use std::time::Duration;

use ferrous_api::{create_default_router, run_healthcheck_probe};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;

#[tokio::test]
async fn test_healthcheck_ready_returns_ok() {
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind ephemeral port");
    let addr = listener.local_addr().expect("local addr");
    let app = create_default_router();

    let server_handle = tokio::spawn(async move {
        let _ = axum::serve(listener, app).await;
    });

    let result =
        tokio::task::spawn_blocking(move || run_healthcheck_probe(addr, Duration::from_secs(2)))
            .await
            .expect("spawn_blocking join");

    server_handle.abort();
    assert!(result.is_ok(), "expected Ok(()), got: {result:?}");
}

#[tokio::test]
async fn test_healthcheck_unreachable_returns_err() {
    let listener = StdTcpListener::bind("127.0.0.1:0").expect("bind ephemeral port");
    let addr = listener.local_addr().expect("local addr");
    drop(listener);

    let result = tokio::task::spawn_blocking(move || {
        run_healthcheck_probe(addr, Duration::from_millis(500))
    })
    .await
    .expect("spawn_blocking join");

    assert!(result.is_err(), "expected Err for unbound port, got Ok");
}

#[tokio::test]
async fn test_healthcheck_non_200_returns_err() {
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind ephemeral port");
    let addr = listener.local_addr().expect("local addr");

    let server_handle = tokio::spawn(async move {
        if let Ok((mut socket, _)) = listener.accept().await {
            let mut buf = [0u8; 512];
            let _ = socket.read(&mut buf).await;
            let _ = socket
                .write_all(
                    b"HTTP/1.1 503 Service Unavailable\r\nContent-Length: 0\r\nConnection: close\r\n\r\n",
                )
                .await;
            let _ = socket.shutdown().await;
        }
    });

    let result =
        tokio::task::spawn_blocking(move || run_healthcheck_probe(addr, Duration::from_secs(2)))
            .await
            .expect("spawn_blocking join");

    server_handle.abort();
    assert!(result.is_err(), "expected Err for HTTP 503, got Ok");
}

#[tokio::test]
async fn test_healthcheck_empty_eof_returns_err() {
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind ephemeral port");
    let addr = listener.local_addr().expect("local addr");

    let server_handle = tokio::spawn(async move {
        if let Ok((mut socket, _)) = listener.accept().await {
            let mut buf = [0u8; 512];
            let _ = socket.read(&mut buf).await;
            let _ = socket.shutdown().await;
        }
    });

    let result =
        tokio::task::spawn_blocking(move || run_healthcheck_probe(addr, Duration::from_secs(2)))
            .await
            .expect("spawn_blocking join");

    server_handle.abort();
    assert!(result.is_err(), "expected Err on empty EOF, got Ok");
}

#[tokio::test]
async fn test_binary_healthcheck_cli_ready_and_fallback_port() {
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind ephemeral port");
    let port = listener.local_addr().expect("local addr").port();
    let app = create_default_router();

    let server_handle = tokio::spawn(async move {
        let _ = axum::serve(listener, app).await;
    });

    let bin = env!("CARGO_BIN_EXE_ferrous-api");
    let port_str = port.to_string();

    // 1. PORT set to listening server -> exits 0
    let ok_status = tokio::task::spawn_blocking({
        let port_str = port_str.clone();
        move || {
            std::process::Command::new(bin)
                .arg("--healthcheck")
                .env("PORT", &port_str)
                .env_remove("FERROUS_API_PORT")
                .status()
                .expect("spawn ferrous-api --healthcheck")
        }
    })
    .await
    .expect("spawn_blocking join");
    assert!(ok_status.success(), "expected exit 0, got {ok_status:?}");

    // 2. Empty/invalid PORT falls back to FERROUS_API_PORT -> exits 0
    let fallback_status = tokio::task::spawn_blocking({
        let port_str = port_str.clone();
        move || {
            std::process::Command::new(bin)
                .arg("--healthcheck")
                .env("PORT", " ")
                .env("FERROUS_API_PORT", &port_str)
                .status()
                .expect("spawn ferrous-api --healthcheck")
        }
    })
    .await
    .expect("spawn_blocking join");
    assert!(
        fallback_status.success(),
        "expected fallback exit 0, got {fallback_status:?}"
    );

    server_handle.abort();

    // 3. Unbound port -> exits non-zero
    let closed_listener = StdTcpListener::bind("127.0.0.1:0").expect("bind ephemeral port");
    let closed_port = closed_listener.local_addr().expect("local addr").port();
    drop(closed_listener);

    let fail_status = tokio::task::spawn_blocking(move || {
        std::process::Command::new(bin)
            .arg("--healthcheck")
            .env("PORT", closed_port.to_string())
            .env_remove("FERROUS_API_PORT")
            .status()
            .expect("spawn ferrous-api --healthcheck")
    })
    .await
    .expect("spawn_blocking join");
    assert!(
        !fail_status.success(),
        "expected non-zero exit on closed port, got {fail_status:?}"
    );
}

#[test]
fn test_resolve_port_precedence_and_defaults() {
    let orig_port = std::env::var_os("PORT");
    let orig_ferrous_port = std::env::var_os("FERROUS_API_PORT");

    std::env::remove_var("PORT");
    std::env::remove_var("FERROUS_API_PORT");
    assert_eq!(ferrous_api::resolve_port(), ferrous_api::DEFAULT_PORT);

    std::env::set_var("PORT", "0");
    std::env::set_var("FERROUS_API_PORT", " 9091 ");
    assert_eq!(ferrous_api::resolve_port(), 9091);

    std::env::set_var("PORT", "8181");
    assert_eq!(ferrous_api::resolve_port(), 8181);

    match orig_port {
        Some(v) => std::env::set_var("PORT", v),
        None => std::env::remove_var("PORT"),
    }
    match orig_ferrous_port {
        Some(v) => std::env::set_var("FERROUS_API_PORT", v),
        None => std::env::remove_var("FERROUS_API_PORT"),
    }
}
