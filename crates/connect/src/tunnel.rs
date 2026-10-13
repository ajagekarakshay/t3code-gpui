use anyhow::{Context, Result, ensure};
use serde::{Deserialize, Serialize};
use std::{net::IpAddr, path::Path, process::Stdio};
use tokio::{
    io::AsyncReadExt,
    process::Command,
    sync::{oneshot, watch},
    task::JoinHandle,
};

/// Wire-compatible relay connector configuration. Never log its token.
#[derive(Clone, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct TunnelConfiguration {
    provider_kind: String,
    connector_token: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    tunnel_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    tunnel_name: Option<String>,
}
impl TunnelConfiguration {
    pub fn provider_kind(&self) -> &str {
        &self.provider_kind
    }
    pub fn tunnel_id(&self) -> Option<&str> {
        self.tunnel_id.as_deref()
    }
    pub fn tunnel_name(&self) -> Option<&str> {
        self.tunnel_name.as_deref()
    }
    pub fn validate(&self) -> Result<()> {
        ensure!(
            matches!(
                self.provider_kind.as_str(),
                "manual" | "cloudflare_tunnel" | "t3_relay"
            ),
            "Unsupported connector"
        );
        ensure!(
            !self.connector_token.trim().is_empty(),
            "Missing connector token"
        );
        Ok(())
    }
    pub fn validate_origin(host: &str, port: u16) -> Result<()> {
        let host = host
            .trim()
            .trim_start_matches('[')
            .trim_end_matches(']')
            .to_ascii_lowercase();
        ensure!(port != 0, "Tunnel origin port is required");
        ensure!(
            host == "localhost" || host.parse::<IpAddr>().is_ok_and(|ip| ip.is_loopback()),
            "Tunnel origin must be loopback"
        );
        Ok(())
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TunnelState {
    running: bool,
    connected: bool,
    rejections: u32,
}
impl TunnelState {
    pub fn running(&self) -> bool {
        self.running
    }
    pub fn connected(&self) -> bool {
        self.connected
    }
    pub fn needs_recovery(&self) -> bool {
        self.rejections >= 4
    }
}

/// Owns and reaps only the connector process it spawned.
pub struct Tunnel {
    pid: u32,
    state: watch::Receiver<TunnelState>,
    stop: Option<oneshot::Sender<()>>,
    task: Option<JoinHandle<()>>,
}
impl Tunnel {
    pub fn start(executable: &Path, configuration: &TunnelConfiguration) -> Result<Self> {
        configuration.validate()?;
        ensure!(
            configuration.provider_kind == "cloudflare_tunnel",
            "Unsupported connector"
        );
        let mut child = Command::new(executable)
            .args([
                "tunnel",
                "--no-autoupdate",
                "--loglevel",
                "info",
                "--output",
                "default",
                "run",
            ])
            .env("TUNNEL_TOKEN", &configuration.connector_token)
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::piped())
            .kill_on_drop(true)
            .spawn()?;
        let pid = child.id().context("Connector exited before startup")?;
        let mut stderr = child.stderr.take().context("Missing connector output")?;
        let (stop, mut stopped) = oneshot::channel();
        let initial = TunnelState {
            running: true,
            connected: false,
            rejections: 0,
        };
        let (changes, state) = watch::channel(initial.clone());
        let task = tokio::spawn(async move {
            let mut current = initial;
            let mut buffer = [0u8; 4096];
            let mut line = Vec::with_capacity(1024);
            let mut truncated = false;
            let mut output_open = true;
            loop {
                tokio::select! {
                    _ = &mut stopped => { let _ = child.kill().await; let _ = child.wait().await; break; }
                    _ = child.wait() => break,
                    read = stderr.read(&mut buffer), if output_open => {
                        let count = match read { Ok(0) | Err(_) => {output_open = false; continue;}, Ok(count) => count };
                        for byte in &buffer[..count] {
                            if *byte == b'\n' {
                                if !truncated {
                                    let value = String::from_utf8_lossy(&line);
                                    if value.contains("Registered tunnel connection") {
                                        current.connected = true; current.rejections = 0;
                                    } else if rejected(&value) {
                                        current.rejections = current.rejections.saturating_add(1);
                                        current.connected = false;
                                    }
                                    if *changes.borrow() != current { changes.send_replace(current.clone()); }
                                }
                                line.clear(); truncated = false;
                            } else if line.len() < 8192 { line.push(*byte); } else { truncated = true; }
                        }
                    }
                }
            }
            current.running = false;
            current.connected = false;
            changes.send_replace(current);
        });
        Ok(Self {
            pid,
            state,
            stop: Some(stop),
            task: Some(task),
        })
    }
    pub fn pid(&self) -> u32 {
        self.pid
    }
    pub fn state(&self) -> TunnelState {
        self.state.borrow().clone()
    }
    pub fn subscribe(&self) -> watch::Receiver<TunnelState> {
        self.state.clone()
    }
    pub async fn stop(mut self) -> Result<()> {
        if let Some(stop) = self.stop.take() {
            let _ = stop.send(());
        }
        if let Some(task) = self.task.take() {
            task.await?;
        }
        Ok(())
    }
}
impl Drop for Tunnel {
    fn drop(&mut self) {
        if let Some(stop) = self.stop.take() {
            let _ = stop.send(());
        }
    }
}
fn rejected(line: &str) -> bool {
    let line = line.to_ascii_lowercase();
    line.contains("register tunnel error from server side")
        && [
            "unauthorized:",
            "failed to get tunnel",
            "tunnel not found",
            "record for tunnel not found",
            "invalid tunnel secret",
        ]
        .iter()
        .any(|pattern| line.contains(pattern))
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn tunnels_only_expose_loopback() {
        for host in ["127.0.0.1", "::1", "[::1]", "localhost"] {
            assert!(TunnelConfiguration::validate_origin(host, 3773).is_ok());
        }
        for host in [
            "0.0.0.0",
            "192.168.1.1",
            "example.com",
            "localhost.evil.test",
        ] {
            assert!(TunnelConfiguration::validate_origin(host, 3773).is_err());
        }
    }
    #[test]
    fn recovery_requires_an_edge_registration_failure() {
        assert!(rejected(
            "ERR Register tunnel error from server side error=\"Unauthorized: stale token\""
        ));
        assert!(!rejected(
            "ERR Unable to reach the origin service: connection refused"
        ));
        assert!(!rejected("Tunnel not found in local configuration"));
    }
}
