// Copyright 2026 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Post-Quantum Cryptography (PQC) Transport Verification Tests.
//!
//! This module verifies that `google-cloud-rust` transports support and negotiate
//! post-quantum key exchange algorithms when connecting over TLS 1.3:
//!
//! * `X25519MLKEM768` (hybrid, IANA group ID `0x11ec` / 4588): offered by the
//!   default `rustls` + `aws-lc-rs` configuration. See [run].
//! * `MLKEM1024` (pure ML-KEM, IANA group ID `0x0202` / 514): **not** offered
//!   by default. Applications opt in by installing a process-default
//!   `rustls::crypto::CryptoProvider` that includes
//!   `rustls::crypto::aws_lc_rs::kx_group::MLKEM1024` before building any
//!   clients. See [run_mlkem1024] and [run_mlkem1024_not_default].
//!
//! # Verification Mechanism
//!
//! An isolated instance of `gapic-showcase` is spawned with:
//! * `--tls`: Enables Auto-TLS, generating in-memory CA and server certificates.
//! * `--ca-cert-output-file <path>`: Exports the self-signed CA certificate PEM.
//! * `--tls-groups <group>`: Strictly restricts server-accepted key exchange
//!   groups to a single group (e.g. `0x11ec` or `0x0202`).
//!
//! If the client's TLS stack (`aws-lc-rs` via `rustls`) does not offer and negotiate
//! the pinned group during the TLS 1.3 `ClientHello`, the server rejects the
//! handshake with a TLS `HandshakeFailure` alert.
//!
//! # Requirements
//!
//! The `MLKEM1024` tests require `gapic-showcase` to be built with Go >= 1.27.
//! Earlier Go versions do not support pure `MLKEM1024` (`0x0202`) in
//! `crypto/tls`, and the server would fail to negotiate any group.
//!
//! # Scope of Tests
//!
//! 1. **HTTP/REST Unary (`reqwest`)**: Verifies HTTPS unary RPC execution.
//! 2. **gRPC Streaming (`tonic`)**: Verifies bidirectional streaming RPC execution
//!    over HTTP/2 TLS.
//! 3. **Default rejection**: Verifies that, without opting in, the default client
//!    configuration cannot connect to a server pinned to `MLKEM1024`.

use super::{Anonymous, NeverRetry};
use crate::Result;
use anyhow::Error;
use google_cloud_gax::options::RequestOptionsBuilder;
use google_cloud_gax::retry_policy::{AlwaysRetry, RetryPolicyExt};
use google_cloud_showcase_v1beta1::client::{Echo, Testing};
use google_cloud_showcase_v1beta1::model::EchoRequest;
use std::path::PathBuf;
use std::process::Stdio;
use std::time::Duration;
use tokio::io::{AsyncBufReadExt, BufReader};
use tokio::process::Command;

/// Configuration for an isolated, PQC-pinned showcase server.
struct ServerConfig {
    /// The value passed to `--tls-groups`, e.g. `0x11ec`.
    tls_group: &'static str,
    /// A human-readable name for the TLS group, used in logs and messages.
    group_name: &'static str,
    /// The `--port` value. Each server uses a dedicated port to prevent collisions
    /// with the standard showcase test suite (running concurrently on `:7469`).
    port: &'static str,
    /// The `--fallback-port` value, distinct from the default `:1337`.
    fallback_port: &'static str,
    /// The endpoint used by the clients.
    endpoint: &'static str,
}

/// Pinned to `X25519MLKEM768`, offered by the default client configuration.
const X25519MLKEM768: ServerConfig = ServerConfig {
    tls_group: "0x11ec",
    group_name: "X25519MLKEM768",
    port: ":7471",
    fallback_port: ":1339",
    endpoint: "https://localhost:7471",
};

/// Pinned to `MLKEM1024`, which requires an application opt-in.
const MLKEM1024: ServerConfig = ServerConfig {
    tls_group: "0x0202",
    group_name: "MLKEM1024",
    port: ":7472",
    fallback_port: ":1340",
    endpoint: "https://localhost:7472",
};

/// Pinned to `MLKEM1024`, used to verify the default configuration is rejected.
///
/// Uses separate ports from [MLKEM1024] so it never collides with that server.
const MLKEM1024_NOT_DEFAULT: ServerConfig = ServerConfig {
    tls_group: "0x0202",
    group_name: "MLKEM1024",
    port: ":7473",
    fallback_port: ":1341",
    endpoint: "https://localhost:7473",
};

/// Main entry point for the `X25519MLKEM768` PQC integration test suite.
///
/// Spawns an isolated `gapic-showcase` server configured with Auto-TLS and pinned to
/// `0x11ec` (`X25519MLKEM768`), configures CA trust, and runs transport verifications.
pub async fn run() -> Result<()> {
    run_with(&X25519MLKEM768).await
}

/// Entry point for the `MLKEM1024` PQC integration test suite.
///
/// Spawns an isolated `gapic-showcase` server pinned to `0x0202` (`MLKEM1024`),
/// and runs the transport verifications.
///
/// The caller must install a process-default `rustls::crypto::CryptoProvider` that
/// includes `MLKEM1024` before calling this function, as an application would.
/// Requires `gapic-showcase` built with Go >= 1.27.
pub async fn run_mlkem1024() -> Result<()> {
    run_with(&MLKEM1024).await
}

/// Verifies that `MLKEM1024` is **not** offered by the default client configuration.
///
/// Spawns an isolated `gapic-showcase` server pinned to `0x0202` (`MLKEM1024`) and
/// asserts that an HTTP unary RPC fails, because the server rejects the TLS
/// handshake. The CA certificate is trusted, so the failure can only be caused
/// by the key exchange negotiation.
///
/// The caller must **not** install a custom `rustls::crypto::CryptoProvider`.
/// Requires `gapic-showcase` built with Go >= 1.27.
pub async fn run_mlkem1024_not_default() -> Result<()> {
    let _guard = google_cloud_test_utils::tracing::enable_tracing();
    let config = &MLKEM1024_NOT_DEFAULT;
    let _server = start_server(config).await?;

    // The usual readiness check performs an RPC, which is expected to fail.
    // Wait until the server accepts TCP connections instead.
    wait_until_listening(config).await?;

    let client = Echo::builder()
        .with_endpoint(config.endpoint)
        .with_credentials(Anonymous::new().build())
        .with_retry_policy(NeverRetry)
        .with_tracing()
        .build()
        .await?;

    let result = client
        .echo()
        .set_content("this request should not reach the server")
        .send()
        .await;
    let Err(e) = result else {
        return Err(Error::msg(format!(
            "expected the TLS handshake to fail with a server pinned to {}, got {result:?}",
            config.group_name
        )));
    };
    tracing::info!(
        "Verified the default configuration does not negotiate {}: {e:?}",
        config.group_name
    );
    Ok(())
}

async fn run_with(config: &ServerConfig) -> Result<()> {
    let _guard = google_cloud_test_utils::tracing::enable_tracing();
    let _server = start_server(config).await?;

    // Wait until the server is ready.
    if let Err(e) = wait_until_ready(config).await {
        return Err(Error::msg(format!(
            "showcase PQC server ({}) is not ready: {e:?}",
            config.group_name
        )));
    }

    tracing::info!("testing PQC transport (HTTP unary)");
    test_pqc_http(config).await?;

    tracing::info!("testing PQC transport (gRPC streaming)");
    test_pqc_grpc(config).await?;

    Ok(())
}

/// A running showcase server, and the environment configured to trust its CA.
///
/// Dropping this value kills the server and restores `SSL_CERT_FILE`.
struct Server {
    _child: tokio::process::Child,
    _env: scoped_env::ScopedEnv<String>,
}

async fn start_server(config: &ServerConfig) -> Result<Server> {
    let path = super::install().await?;
    let showcase: PathBuf = [path.as_str(), "bin", "gapic-showcase"].iter().collect();

    // Use a distinct file per server, in case multiple servers share a process.
    let ca_cert_path = std::env::temp_dir().join(format!(
        "showcase_pqc_ca_{}_{}.pem",
        config.tls_group,
        std::process::id()
    ));
    if ca_cert_path.exists() {
        let _ = std::fs::remove_file(&ca_cert_path);
    }

    let ca_cert_path_str = ca_cert_path
        .to_str()
        .ok_or_else(|| Error::msg("temp dir path is not valid UTF-8"))?;

    tracing::info!(
        "starting {showcase:?} with Auto-TLS (PQC enabled and pinned to {} / {})",
        config.tls_group,
        config.group_name
    );
    let mut child = Command::new(&showcase)
        .args([
            "run",
            "--port",
            config.port,
            "--fallback-port",
            config.fallback_port,
            "--tls",
            "--ca-cert-output-file",
            ca_cert_path_str,
            "--tls-groups",
            config.tls_group,
        ])
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .kill_on_drop(true)
        .spawn()
        .map_err(anyhow::Error::from)?;
    tracing::info!("started showcase PQC server: {child:?}");

    // Wait for Showcase to write the autogenerated CA certificate.
    wait_for_ca_cert(&mut child).await?;
    let env = scoped_env::ScopedEnv::set("SSL_CERT_FILE".to_string(), ca_cert_path_str.to_string());

    Ok(Server {
        _child: child,
        _env: env,
    })
}

async fn wait_for_ca_cert(child: &mut tokio::process::Child) -> Result<()> {
    let stdout = child
        .stdout
        .take()
        .ok_or_else(|| Error::msg("failed to capture showcase stdout"))?;
    let mut reader = BufReader::new(stdout).lines();

    let (ca_tx, ca_rx) = tokio::sync::oneshot::channel();
    tokio::spawn(async move {
        let mut ca_tx = Some(ca_tx);
        while let Ok(Some(line)) = reader.next_line().await {
            tracing::debug!("[gapic-showcase] {line}");
            if line.contains("Wrote autogenerated CA certificate to")
                && let Some(tx) = ca_tx.take()
            {
                let _ = tx.send(());
            }
        }
    });

    match tokio::time::timeout(Duration::from_secs(30), ca_rx).await {
        Ok(Ok(())) => Ok(()),
        Ok(Err(_)) => Err(Error::msg(
            "showcase process exited or closed stdout before writing CA certificate",
        )),
        Err(_) => Err(Error::msg(
            "timed out waiting for showcase to write CA certificate within 30s",
        )),
    }
}

async fn wait_until_ready(config: &ServerConfig) -> Result<()> {
    let client = Testing::builder()
        .with_endpoint(config.endpoint)
        .with_credentials(Anonymous::new().build())
        .with_tracing()
        .build()
        .await?;

    let _list = client
        .list_sessions()
        .with_retry_policy(AlwaysRetry.with_attempt_limit(10))
        .with_attempt_timeout(Duration::from_secs(1))
        .send()
        .await?;
    Ok(())
}

/// Waits until the server accepts TCP connections, without performing a TLS handshake.
async fn wait_until_listening(config: &ServerConfig) -> Result<()> {
    let address = format!("localhost{}", config.port);
    for _ in 0..10 {
        if tokio::net::TcpStream::connect(&address).await.is_ok() {
            return Ok(());
        }
        tokio::time::sleep(Duration::from_secs(1)).await;
    }
    Err(Error::msg(format!(
        "showcase PQC server ({}) is not listening on {address}",
        config.group_name
    )))
}

/// Verifies that HTTP unary RPCs (`reqwest`) succeed over a TLS channel requiring the pinned group.
async fn test_pqc_http(config: &ServerConfig) -> Result<()> {
    let client = Echo::builder()
        .with_endpoint(config.endpoint)
        .with_credentials(Anonymous::new().build())
        .with_retry_policy(NeverRetry)
        .with_tracing()
        .build()
        .await?;

    const TEXT: &str = "testing PQC transport compliance (HTTP unary)";
    let response = client.echo().set_content(TEXT).send().await?;
    assert_eq!(response.content, TEXT);

    tracing::info!(
        "Verified HTTP unary RPC over PQC TLS channel ({})",
        config.tls_group
    );
    Ok(())
}

/// Verifies that gRPC bidirectional streaming RPCs (`tonic`) succeed over a TLS channel requiring the pinned group.
async fn test_pqc_grpc(config: &ServerConfig) -> Result<()> {
    let client = Echo::builder()
        .with_endpoint(config.endpoint)
        .with_credentials(Anonymous::new().build())
        .with_retry_policy(NeverRetry)
        .with_tracing()
        .build()
        .await?;

    const TOTAL_MESSAGES: usize = 5;
    let (sender, mut resp_stream) = client.chat().build();

    for i in 0..TOTAL_MESSAGES {
        sender
            .send(EchoRequest::new().set_content(format!("pqc-grpc-msg-{i}")))
            .await?;
    }
    drop(sender);

    let mut received = Vec::new();
    while let Some(res) = resp_stream.next().await {
        received.push(res?.content);
    }

    let expected: Vec<String> = (0..TOTAL_MESSAGES)
        .map(|i| format!("pqc-grpc-msg-{i}"))
        .collect();
    assert_eq!(
        received, expected,
        "gRPC streaming message exchange over PQC TLS channel must match"
    );

    tracing::info!(
        "Verified gRPC streaming over PQC TLS channel ({})",
        config.tls_group
    );
    Ok(())
}
