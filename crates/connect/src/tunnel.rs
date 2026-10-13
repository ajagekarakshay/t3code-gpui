use anyhow::{Result, ensure};
use serde::{Deserialize, Serialize};
use std::{net::IpAddr, path::Path, process::Stdio};
use tokio::process::{Child, Command};

/// Wire-compatible relay connector configuration. Never log its token.
#[derive(Clone, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct TunnelConfiguration {
    provider_kind: String,
    connector_token: String,
    tunnel_id: Option<String>,
    tunnel_name: Option<String>,
}

impl TunnelConfiguration {
    pub fn tunnel_id(&self) -> Option<&str> {
        self.tunnel_id.as_deref()
    }

    pub fn validate_origin(host: &str, port: u16) -> Result<()> {
        ensure!(port != 0, "Tunnel origin port is required");
        ensure!(
            host == "localhost" || host.parse::<IpAddr>().is_ok_and(|ip| ip.is_loopback()),
            "Tunnel origin must be loopback"
        );
        Ok(())
    }
}

/// Owns only the connector process it spawned. Dropping it never kills unrelated servers.
pub struct Tunnel {
    child: Child,
}

impl Tunnel {
    pub fn start(executable: &Path, configuration: &TunnelConfiguration) -> Result<Self> {
        ensure!(
            matches!(
                configuration.provider_kind.as_str(),
                "cloudflare_tunnel" | "t3_relay"
            ),
            "Unsupported connector"
        );
        ensure!(
            !configuration.connector_token.trim().is_empty(),
            "Missing connector token"
        );
        let child = Command::new(executable)
            .args(["tunnel", "--no-autoupdate", "run"])
            // Keep credentials out of the process command line and logs.
            .env("TUNNEL_TOKEN", &configuration.connector_token)
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .kill_on_drop(true)
            .spawn()?;
        Ok(Self { child })
    }

    pub async fn stop(mut self) -> Result<()> {
        if self.child.try_wait()?.is_none() {
            self.child.kill().await?;
        }
        let _ = self.child.wait().await?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn tunnels_only_expose_loopback() {
        for host in ["127.0.0.1", "::1", "localhost"] {
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
}
