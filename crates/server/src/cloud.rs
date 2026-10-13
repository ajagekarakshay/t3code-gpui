use crate::{auth, http::Services, store::required};
use anyhow::{Context, Result, ensure};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::{net::SocketAddr, path::PathBuf, sync::Arc, time::Duration};
use t3_connect::{Tunnel, TunnelConfiguration};

#[derive(Clone, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
struct RelayConfiguration {
    relay_url: String,
    relay_issuer: Option<String>,
    cloud_user_id: String,
    environment_credential: String,
    cloud_mint_public_key: String,
    endpoint_runtime: Option<TunnelConfiguration>,
}
impl RelayConfiguration {
    fn issuer(&self) -> Result<String> {
        issuer(self.relay_issuer.as_deref().unwrap_or(&self.relay_url))
    }
    fn validate(&self) -> Result<()> {
        issuer(&self.relay_url)?;
        self.issuer()?;
        ensure!(
            !self.cloud_user_id.trim().is_empty() && !self.environment_credential.trim().is_empty(),
            "Missing relay authority"
        );
        t3_connect::validate_relay_key(&self.cloud_mint_public_key)?;
        if let Some(runtime) = &self.endpoint_runtime {
            runtime.validate()?;
        }
        Ok(())
    }
}

pub(crate) struct CloudState {
    home: PathBuf,
    origin: Option<Value>,
    tunnel: Option<Tunnel>,
    configuration: Option<TunnelConfiguration>,
    generation: u64,
    status: Value,
    restart_delay: u64,
}
impl CloudState {
    pub(crate) fn new(home: PathBuf) -> Self {
        Self {
            home,
            origin: None,
            tunnel: None,
            configuration: None,
            generation: 0,
            status: json!({"status":"disabled"}),
            restart_delay: 1,
        }
    }
    pub(crate) async fn stop(&mut self) {
        self.generation += 1;
        self.configuration = None;
        if let Some(tunnel) = self.tunnel.take() {
            let _ = tunnel.stop().await;
        }
        self.status = json!({"status":"disabled"});
    }
}

pub(crate) fn issuer(value: &str) -> Result<String> {
    let parsed = url::Url::parse(value.trim())?;
    ensure!(
        parsed.scheme() == "https"
            && parsed.host_str().is_some()
            && parsed.username().is_empty()
            && parsed.password().is_none()
            && parsed.query().is_none()
            && parsed.fragment().is_none(),
        "Relay issuer must use HTTPS"
    );
    Ok(t3_connect::normalize_issuer(value))
}
fn validate_endpoint(endpoint: &Value) -> Result<()> {
    ensure!(
        matches!(
            required(endpoint, "providerKind")?,
            "cloudflare_tunnel" | "manual"
        ),
        "Unsupported link provider"
    );
    let http = url::Url::parse(required(endpoint, "httpBaseUrl")?)?;
    let ws = url::Url::parse(required(endpoint, "wsBaseUrl")?)?;
    ensure!(
        matches!(http.scheme(), "http" | "https") && matches!(ws.scheme(), "ws" | "wss"),
        "Invalid endpoint schemes"
    );
    for value in [&http, &ws] {
        ensure!(
            value.username().is_empty()
                && value.password().is_none()
                && value.host_str().is_some()
                && value.query().is_none()
                && value.fragment().is_none(),
            "Invalid endpoint authority"
        );
    }
    Ok(())
}
impl Services {
    fn relay_configuration(&self) -> Result<RelayConfiguration> {
        serde_json::from_str(
            &self
                .store
                .metadata("relayConfig")?
                .context("Environment is not linked")?,
        )
        .map_err(Into::into)
    }
    pub(crate) fn link_proof(&self, input: &Value, request_url: &str) -> Result<String> {
        let relay = issuer(required(input, "relayIssuer")?)?;
        validate_endpoint(&input["endpoint"])?;
        let origin = &input["origin"];
        let port = origin["localHttpPort"]
            .as_u64()
            .filter(|v| *v <= 65535)
            .context("Invalid tunnel port")? as u16;
        TunnelConfiguration::validate_origin(required(origin, "localHttpHost")?, port)?;
        let request = url::Url::parse(request_url)?;
        let request_host = request.host_str().context("Missing environment host")?;
        TunnelConfiguration::validate_origin(
            request_host,
            request
                .port_or_known_default()
                .context("Invalid request port")?,
        )?;
        ensure!(
            request.port_or_known_default() == Some(port),
            "Tunnel port must match this environment"
        );
        let scopes = if input["endpoint"]["providerKind"] == "cloudflare_tunnel" {
            json!(["agent_activity_notifications", "managed_tunnels"])
        } else {
            json!(["agent_activity_notifications"])
        };
        let timestamp = auth::epoch();
        let id = required(&self.descriptor, "environmentId")?;
        self.identity.sign(t3_connect::LINK_PROOF_TYPE,&json!({"iss":format!("t3-env:{id}"),"aud":relay,"sub":id,"jti":uuid::Uuid::new_v4().to_string(),"iat":timestamp,"exp":timestamp+300,"challenge":input["challenge"].as_str().context("Missing challenge")?,"descriptor":self.descriptor,"environmentId":id,"environmentPublicKey":self.identity.public_pem()?.trim(),"endpoint":input["endpoint"],"origin":origin,"scopes":scopes}))
    }
    pub(crate) async fn configure_relay(self: &Arc<Self>, input: Value) -> Result<Value> {
        let configuration: RelayConfiguration = serde_json::from_value(input)?;
        configuration.validate()?;
        let mut cloud = self.cloud.lock().await;
        self.store
            .set_metadata("relayConfig", &serde_json::to_string(&configuration)?)?;
        self.activate_tunnel(&mut cloud, configuration.endpoint_runtime.as_ref(), false)
            .await;
        Ok(json!({"ok":true,"endpointRuntimeStatus":cloud.status}))
    }
    pub(crate) async fn link_state(&self) -> Result<Value> {
        let _cloud = self.cloud.lock().await;
        let configuration = self
            .store
            .metadata("relayConfig")?
            .map(|v| serde_json::from_str::<RelayConfiguration>(&v))
            .transpose()?;
        let preferences: Value = self
            .store
            .metadata("relayPreferences")?
            .map(|v| serde_json::from_str(&v))
            .transpose()?
            .unwrap_or(json!({}));
        Ok(
            json!({"linked":configuration.is_some(),"cloudUserId":configuration.as_ref().map(|v| &v.cloud_user_id),"relayUrl":configuration.as_ref().map(|v| &v.relay_url),"relayIssuer":configuration.as_ref().map(|v|v.issuer()).transpose()?,"managedTunnelActive":configuration.as_ref().is_some_and(|v| v.endpoint_runtime.is_some()),"publishAgentActivity":preferences["publishAgentActivity"].as_bool().unwrap_or(false),"holdWebhooksWhileOffline":preferences["holdWebhooksWhileOffline"].as_bool().unwrap_or(false)}),
        )
    }
    pub(crate) async fn unlink(&self) -> Result<Value> {
        let mut cloud = self.cloud.lock().await;
        // Remove authority and preferences together before reporting the environment unlinked.
        self.store
            .remove_metadata(&["relayConfig", "relayPreferences"])?;
        cloud.stop().await;
        Ok(json!({"ok":true,"endpointRuntimeStatus":cloud.status}))
    }
    pub(crate) async fn save_cloud_preferences(&self, input: Value) -> Result<()> {
        let _serialized = self.cloud.lock().await;
        input["publishAgentActivity"]
            .as_bool()
            .context("Missing activity preference")?;
        let mut preferences: Value = self
            .store
            .metadata("relayPreferences")?
            .map(|v| serde_json::from_str(&v))
            .transpose()?
            .unwrap_or(json!({"holdWebhooksWhileOffline":false}));
        if let Some(hold) = input.get("holdWebhooksWhileOffline") {
            hold.as_bool().context("Invalid webhook preference")?;
            let configuration = self.relay_configuration()?;
            let id = required(&self.descriptor, "environmentId")?;
            self.relay_request(
                &configuration,
                &format!("/v1/environments/{id}/link-preferences"),
                &json!({"holdWebhooksWhileOffline":hold}),
            )
            .await?;
            preferences["holdWebhooksWhileOffline"] = hold.clone();
        }
        preferences["publishAgentActivity"] = input["publishAgentActivity"].clone();
        self.store
            .set_metadata("relayPreferences", &preferences.to_string())
    }
    fn relay_claims(&self, input: &Value, typ: &str, scope: &str) -> Result<(Value, String)> {
        let configuration = self.relay_configuration()?;
        let relay = configuration.issuer()?;
        let id = required(&self.descriptor, "environmentId")?;
        let claims = t3_connect::verify_relay_proof(
            &configuration.cloud_mint_public_key,
            required(input, "proof")?,
            typ,
            &relay,
            &format!("t3-env:{id}"),
            auth::epoch(),
        )?;
        ensure!(
            claims["environmentId"] == id
                && claims["sub"] == configuration.cloud_user_id
                && claims["scope"] == json!([scope]),
            "Relay authority mismatch"
        );
        Ok((claims, relay))
    }
    pub(crate) async fn relay_health(&self, input: &Value) -> Result<Value> {
        let _serialized = self.cloud.lock().await;
        let (claims, relay) =
            self.relay_claims(input, t3_connect::HEALTH_REQUEST_TYPE, "environment:status")?;
        self.store.consume_relay_proof("health", &claims)?;
        let timestamp = auth::epoch();
        let checked = auth::iso(timestamp);
        let id = required(&self.descriptor, "environmentId")?;
        let proof=self.identity.sign(t3_connect::HEALTH_RESPONSE_TYPE,&json!({"iss":format!("t3-env:{id}"),"aud":relay,"sub":id,"jti":uuid::Uuid::new_v4().to_string(),"iat":timestamp,"exp":timestamp+300,"environmentId":id,"requestNonce":claims["nonce"],"status":"online","descriptor":self.descriptor,"checkedAt":checked}))?;
        Ok(
            json!({"environmentId":id,"status":"online","descriptor":self.descriptor,"checkedAt":checked,"proof":proof}),
        )
    }
    pub(crate) async fn relay_mint(&self, input: &Value) -> Result<Value> {
        let _serialized = self.cloud.lock().await;
        let (claims, relay) =
            self.relay_claims(input, t3_connect::MINT_REQUEST_TYPE, "environment:connect")?;
        let thumbprint = required(&claims, "clientProofKeyThumbprint")?;
        ensure!(
            claims["cnf"]["jkt"] == thumbprint,
            "Client proof key mismatch"
        );
        self.store.consume_relay_proof("mint", &claims)?;
        let scopes = auth::STANDARD_SCOPES
            .iter()
            .map(|s| s.to_string())
            .collect::<Vec<_>>();
        let grant = self.store.grant(&scopes, Some(thumbprint), 120, false)?;
        let timestamp = auth::epoch();
        let id = required(&self.descriptor, "environmentId")?;
        let proof=self.identity.sign(t3_connect::MINT_RESPONSE_TYPE,&json!({"iss":format!("t3-env:{id}"),"aud":relay,"sub":id,"jti":uuid::Uuid::new_v4().to_string(),"iat":timestamp,"exp":timestamp+120,"environmentId":id,"requestNonce":claims["nonce"],"clientProofKeyThumbprint":thumbprint,"credential":grant["credential"]}))?;
        Ok(json!({"credential":grant["credential"],"expiresAt":grant["expiresAt"],"proof":proof}))
    }
    pub(crate) async fn start_connect(self: &Arc<Self>, address: SocketAddr) -> Result<()> {
        let mut cloud = self.cloud.lock().await;
        ensure!(cloud.origin.is_none(), "Connect runtime is already started");
        let host = if address.ip().is_loopback() {
            address.ip().to_string()
        } else {
            "127.0.0.1".into()
        };
        cloud.origin = Some(json!({"localHttpHost":host,"localHttpPort":address.port()}));
        if self.store.metadata("relayConfig")?.is_some() {
            let mut configuration = self.relay_configuration()?;
            if configuration
                .endpoint_runtime
                .as_ref()
                .is_some_and(|v| v.provider_kind() == "cloudflare_tunnel")
            {
                // Register the actual bound origin before starting an old token after a restart.
                match self
                    .reconcile_tunnel(&mut configuration, cloud.origin.as_ref().unwrap(), false)
                    .await
                {
                    Ok(()) => {
                        self.store
                            .set_metadata("relayConfig", &serde_json::to_string(&configuration)?)?;
                    }
                    Err(error) => {
                        tracing::warn!(%error,"Could not register tunnel recovery; keeping existing connector configuration");
                    }
                }
            }
            self.activate_tunnel(&mut cloud, configuration.endpoint_runtime.as_ref(), false)
                .await;
        }
        Ok(())
    }
    async fn relay_request(
        &self,
        configuration: &RelayConfiguration,
        path: &str,
        input: &Value,
    ) -> Result<Value> {
        let url = format!(
            "{}{path}",
            t3_connect::normalize_issuer(&configuration.relay_url)
        );
        let mut response = reqwest::Client::builder()
            .redirect(reqwest::redirect::Policy::none())
            .timeout(Duration::from_secs(10))
            .build()?
            .post(url)
            .bearer_auth(&configuration.environment_credential)
            .json(input)
            .send()
            .await?
            .error_for_status()?;
        // Do not include response bodies or connector tokens in errors or logs.
        let mut body = Vec::new();
        while let Some(chunk) = response.chunk().await? {
            ensure!(
                body.len() + chunk.len() <= 1024 * 1024,
                "Relay response is too large"
            );
            body.extend_from_slice(&chunk);
        }
        if body.is_empty() {
            Ok(Value::Null)
        } else {
            Ok(serde_json::from_slice(&body)?)
        }
    }
    async fn reconcile_tunnel(
        &self,
        configuration: &mut RelayConfiguration,
        origin: &Value,
        mut recover: bool,
    ) -> Result<()> {
        let id = required(&self.descriptor, "environmentId")?;
        let runtime = configuration
            .endpoint_runtime
            .as_ref()
            .context("Missing connector configuration")?;
        let tunnel_id = runtime.tunnel_id();
        if tunnel_id.is_none() {
            recover = true;
        }
        let make_proof = |action: &str| -> Result<String> {
            let timestamp = auth::epoch();
            let mut claims = json!({"iss":format!("t3-env:{id}"),"aud":configuration.issuer()?,"sub":id,"jti":uuid::Uuid::new_v4().to_string(),"iat":timestamp,"exp":timestamp+60,"environmentId":id,"cloudUserId":configuration.cloud_user_id,"action":action,"origin":origin});
            if action == "register" {
                claims["tunnelId"] = json!(tunnel_id);
            }
            self.identity.sign(t3_connect::RECOVERY_PROOF_TYPE, &claims)
        };
        if !recover {
            let response=self.relay_request(configuration,&format!("/v1/environments/{id}/tunnel/recovery"),&json!({"cloudUserId":configuration.cloud_user_id,"tunnelId":tunnel_id,"origin":origin,"proof":make_proof("register")?})).await?;
            match required(&response, "status")? {
                "ready" => return Ok(()),
                "recovery_required" => {}
                _ => anyhow::bail!("Invalid relay recovery response"),
            }
        }
        let response=self.relay_request(configuration,&format!("/v1/environments/{id}/tunnel"),&json!({"cloudUserId":configuration.cloud_user_id,"origin":origin,"proof":make_proof("recover")?})).await?;
        validate_endpoint(&response["endpoint"])?;
        let runtime: TunnelConfiguration =
            serde_json::from_value(response["endpointRuntime"].clone())?;
        runtime.validate()?;
        ensure!(
            runtime.provider_kind() == "cloudflare_tunnel",
            "Invalid recovered connector"
        );
        configuration.endpoint_runtime = Some(runtime);
        Ok(())
    }
    fn activate_tunnel<'a>(
        self: &'a Arc<Self>,
        cloud: &'a mut CloudState,
        configuration: Option<&'a TunnelConfiguration>,
        force: bool,
    ) -> futures::future::BoxFuture<'a, ()> {
        Box::pin(async move {
            if !force
                && cloud.configuration.as_ref() == configuration
                && cloud.tunnel.as_ref().is_some_and(|v| v.state().running())
            {
                return;
            }
            cloud.stop().await;
            cloud.configuration = configuration.cloned();
            let Some(configuration) = configuration else {
                return;
            };
            if configuration.provider_kind() != "cloudflare_tunnel" {
                cloud.status =
                    json!({"status":"unsupported","providerKind":configuration.provider_kind()});
                return;
            }
            let executable = resolve_cloudflared(&cloud.home).await;
            let Some(executable) = executable else {
                cloud.status = runtime_status(
                    configuration,
                    json!({"status":"failed","failure":"not-installed","reason":"The relay client is not installed."}),
                );
                return;
            };
            match Tunnel::start(&executable, configuration) {
                Err(_) => {
                    cloud.status = runtime_status(
                        configuration,
                        json!({"status":"failed","failure":"spawn-failed","reason":"Could not start the relay client."}),
                    );
                }
                Ok(tunnel) => {
                    cloud.status = runtime_status(
                        configuration,
                        json!({"status":"running","pid":tunnel.pid()}),
                    );
                    let changes = tunnel.subscribe();
                    let generation = cloud.generation;
                    cloud.tunnel = Some(tunnel);
                    let services = Arc::downgrade(self);
                    let task = tokio::spawn(monitor_tunnel(services, changes, generation));
                    let mut tasks = self.tasks.lock().await;
                    tasks.retain(|task| !task.is_finished());
                    tasks.push(task);
                }
            }
        })
    }
}
fn runtime_status(configuration: &TunnelConfiguration, mut status: Value) -> Value {
    status["providerKind"] = json!(configuration.provider_kind());
    if let Some(id) = configuration.tunnel_id() {
        status["tunnelId"] = json!(id);
    }
    if let Some(name) = configuration.tunnel_name() {
        status["tunnelName"] = json!(name);
    }
    status
}
async fn monitor_tunnel(
    weak_services: std::sync::Weak<Services>,
    mut changes: tokio::sync::watch::Receiver<t3_connect::TunnelState>,
    generation: u64,
) {
    let started = tokio::time::Instant::now();
    let mut registration = Box::pin(tokio::time::sleep(Duration::from_secs(180)));
    loop {
        let timer = tokio::select! {result=changes.changed()=>{if result.is_err(){return;}false},_=&mut registration=>true};
        let state = changes.borrow().clone();
        let Some(services) = weak_services.upgrade() else {
            return;
        };
        let mut cloud = services.cloud.lock().await;
        if cloud.generation != generation {
            return;
        }
        let Ok(mut configuration) = services.relay_configuration() else {
            return;
        };
        if state.connected() {
            cloud.restart_delay = 1;
            registration
                .as_mut()
                .reset(tokio::time::Instant::now() + Duration::from_secs(180));
            continue;
        }
        if !state.running() {
            if started.elapsed() >= Duration::from_secs(30) {
                cloud.restart_delay = 1;
            }
            let delay = cloud.restart_delay;
            cloud.restart_delay = (delay * 2).min(60);
            if let Some(runtime) = &configuration.endpoint_runtime {
                cloud.status = runtime_status(
                    runtime,
                    json!({"status":"failed","failure":"spawn-failed","reason":"The relay client stopped. Retrying."}),
                );
            }
            drop(cloud);
            drop(services);
            tokio::time::sleep(Duration::from_secs(delay)).await;
            let Some(services) = weak_services.upgrade() else {
                return;
            };
            let mut cloud = services.cloud.lock().await;
            if cloud.generation != generation {
                return;
            }
            services
                .activate_tunnel(&mut cloud, configuration.endpoint_runtime.as_ref(), true)
                .await;
            return;
        }
        if state.needs_recovery() || timer {
            if let Some(origin) = &cloud.origin {
                match services
                    .reconcile_tunnel(&mut configuration, origin, true)
                    .await
                {
                    Ok(()) => {
                        if services
                            .store
                            .set_metadata(
                                "relayConfig",
                                &serde_json::to_string(&configuration).unwrap(),
                            )
                            .is_ok()
                        {
                            services
                                .activate_tunnel(
                                    &mut cloud,
                                    configuration.endpoint_runtime.as_ref(),
                                    true,
                                )
                                .await;
                            return;
                        }
                    }
                    Err(error) => tracing::warn!(%error,"Tunnel recovery failed; retrying later"),
                }
            }
            registration
                .as_mut()
                .reset(tokio::time::Instant::now() + Duration::from_secs(60));
        }
    }
}
async fn resolve_cloudflared(home: &std::path::Path) -> Option<PathBuf> {
    let binary = if cfg!(windows) {
        "cloudflared.exe"
    } else {
        "cloudflared"
    };
    let platform = match std::env::consts::OS {
        "macos" => "darwin",
        "windows" => "win32",
        other => other,
    };
    let arch = match std::env::consts::ARCH {
        "aarch64" => "arm64",
        "x86_64" => "x64",
        other => other,
    };
    let mut candidates = Vec::new();
    if let Some(path) = std::env::var_os("T3CODE_CLOUDFLARED_PATH") {
        candidates.push(PathBuf::from(path));
    }
    candidates.push(
        home.join("tools/cloudflared/2026.10.0")
            .join(format!("{platform}-{arch}"))
            .join(binary),
    );
    if let Some(path) = std::env::var_os("PATH") {
        candidates.extend(std::env::split_paths(&path).map(|p| p.join(binary)));
    }
    for path in candidates {
        if !path.is_file() {
            continue;
        }
        let output = tokio::time::timeout(
            Duration::from_secs(3),
            tokio::process::Command::new(&path)
                .arg("version")
                .kill_on_drop(true)
                .output(),
        )
        .await;
        if let Ok(Ok(output)) = output {
            let text = String::from_utf8_lossy(&output.stdout);
            let version = text
                .split_whitespace()
                .find(|part| part.split('.').count() == 3);
            if let Some(version) = version {
                let pieces: Vec<u32> = version.split('.').filter_map(|v| v.parse().ok()).collect();
                if pieces.len() == 3 && pieces.as_slice() >= [2025, 6, 1].as_slice() {
                    return Some(path);
                }
            }
        }
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{Server, ServerOptions};
    use axum::{Json, Router, extract::State, http::HeaderMap, routing::post};

    #[derive(Clone)]
    struct RelayPeer {
        public_key: String,
        environment: String,
        requests: tokio::sync::mpsc::Sender<Value>,
    }
    async fn register(
        State(peer): State<RelayPeer>,
        headers: HeaderMap,
        Json(input): Json<Value>,
    ) -> Json<Value> {
        verify_request(&peer, &headers, &input, "register").await;
        Json(json!({"status":"recovery_required"}))
    }
    async fn recover(
        State(peer): State<RelayPeer>,
        headers: HeaderMap,
        Json(input): Json<Value>,
    ) -> Json<Value> {
        verify_request(&peer, &headers, &input, "recover").await;
        Json(
            json!({"endpoint":{"providerKind":"cloudflare_tunnel","httpBaseUrl":"https://recovered.fixture.test","wsBaseUrl":"wss://recovered.fixture.test"},"endpointRuntime":{"providerKind":"cloudflare_tunnel","connectorToken":"replacement-token","tunnelId":"replacement-tunnel"}}),
        )
    }
    async fn verify_request(peer: &RelayPeer, headers: &HeaderMap, input: &Value, action: &str) {
        assert_eq!(headers["authorization"], "Bearer environment-credential");
        let verified = t3_connect::verify_relay_proof(
            &peer.public_key,
            input["proof"].as_str().unwrap(),
            t3_connect::RECOVERY_PROOF_TYPE,
            &format!("t3-env:{}", peer.environment),
            "https://relay.fixture.test",
            auth::epoch(),
        )
        .unwrap();
        assert_eq!(verified["action"], action);
        assert_eq!(verified["origin"], input["origin"]);
        assert_eq!(verified["cloudUserId"], "linked-user");
        assert_eq!(
            verified["exp"].as_i64().unwrap() - verified["iat"].as_i64().unwrap(),
            60
        );
        peer.requests.send(verified).await.unwrap();
    }
    #[tokio::test]
    async fn restart_registers_the_new_origin_and_recovers_a_deleted_tunnel() -> Result<()> {
        let directory = tempfile::tempdir()?;
        let server = Server::open(ServerOptions::new(directory.path()))?;
        let services = &server.services;
        let environment = required(&services.descriptor, "environmentId")?.to_owned();
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
        let address = listener.local_addr()?;
        let (requests, mut received) = tokio::sync::mpsc::channel(2);
        let peer = RelayPeer {
            public_key: services.identity.public_pem()?,
            environment: environment.clone(),
            requests,
        };
        let router = Router::new()
            .route(
                &format!("/v1/environments/{environment}/tunnel/recovery"),
                post(register),
            )
            .route(
                &format!("/v1/environments/{environment}/tunnel"),
                post(recover),
            )
            .with_state(peer);
        let task = tokio::spawn(async move { axum::serve(listener, router).await.unwrap() });
        // HTTP is confined to this in-process peer. Public configuration validates HTTPS.
        let mut configuration = RelayConfiguration {
            relay_url: format!("http://{address}"),
            relay_issuer: Some("https://relay.fixture.test".into()),
            cloud_user_id: "linked-user".into(),
            environment_credential: "environment-credential".into(),
            cloud_mint_public_key: t3_connect::EnvironmentIdentity::generate().public_pem()?,
            endpoint_runtime: Some(serde_json::from_value(
                json!({"providerKind":"cloudflare_tunnel","connectorToken":"stale-token","tunnelId":"deleted-tunnel"}),
            )?),
        };
        let origin = json!({"localHttpHost":"127.0.0.1","localHttpPort":42837});
        let result = services
            .reconcile_tunnel(&mut configuration, &origin, false)
            .await;
        task.abort();
        let _ = task.await;
        result?;
        let registration = received.recv().await.context("Missing registration")?;
        let recovery = received.recv().await.context("Missing recovery")?;
        assert_eq!(registration["tunnelId"], "deleted-tunnel");
        assert_eq!(recovery["origin"], origin);
        assert!(recovery.get("tunnelId").is_none());
        assert_eq!(
            configuration.endpoint_runtime.as_ref().unwrap().tunnel_id(),
            Some("replacement-tunnel")
        );
        server.shutdown().await;
        Ok(())
    }
}
