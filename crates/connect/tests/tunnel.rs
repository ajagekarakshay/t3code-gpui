use anyhow::Result;
use serde_json::json;
use std::path::Path;
use t3_connect::{Tunnel, TunnelConfiguration};
fn configuration(token: &str) -> TunnelConfiguration {
    serde_json::from_value(
        json!({"providerKind":"cloudflare_tunnel","connectorToken":token,"tunnelId":"test-tunnel"}),
    )
    .unwrap()
}
#[tokio::test]
async fn readiness_and_shutdown_belong_to_the_captured_process() -> Result<()> {
    let executable = Path::new(env!("CARGO_BIN_EXE_t3-connector-fixture"));
    let mut unrelated = tokio::process::Command::new(executable)
        .args([
            "tunnel",
            "--no-autoupdate",
            "--loglevel",
            "info",
            "--output",
            "default",
            "run",
        ])
        .env("TUNNEL_TOKEN", "fixture-connector-token")
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .kill_on_drop(true)
        .spawn()?;
    let tunnel = Tunnel::start(executable, &configuration("fixture-connector-token"))?;
    let mut state = tunnel.subscribe();
    tokio::time::timeout(std::time::Duration::from_secs(10), async {
        while !state.borrow().connected() {
            state.changed().await.unwrap();
        }
    })
    .await?;
    assert!(state.borrow().running());
    tunnel.stop().await?;
    assert!(!state.borrow().running());
    assert!(unrelated.try_wait()?.is_none());
    unrelated.kill().await?;
    unrelated.wait().await?;
    Ok(())
}
#[tokio::test]
async fn repeated_edge_rejections_request_recovery() -> Result<()> {
    let tunnel = Tunnel::start(
        Path::new(env!("CARGO_BIN_EXE_t3-connector-fixture")),
        &configuration("fixture-rejected-token"),
    )?;
    let mut state = tunnel.subscribe();
    tokio::time::timeout(std::time::Duration::from_secs(10), async {
        while !state.borrow().needs_recovery() {
            state.changed().await.unwrap();
        }
    })
    .await?;
    assert!(!state.borrow().connected());
    tunnel.stop().await?;
    Ok(())
}
