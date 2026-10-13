use crate::TunnelConfiguration;
use anyhow::{Context, Result, bail, ensure};
use base64::{Engine as _, engine::general_purpose};
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use serde_json::{Value, json};
use std::time::Duration;

const OAUTH_SCOPES: &str = "openid profile email offline_access";
const DEVICE_GRANT: &str = "urn:ietf:params:oauth:grant-type:device_code";
const MAX_RESPONSE_BYTES: usize = 1024 * 1024;
const DEFAULT_POLL_SECONDS: u64 = 5;
const SLOW_DOWN_SECONDS: u64 = 5;

#[derive(Clone, Debug)]
pub struct ConnectPublicConfig {
    relay_url: url::Url,
    relay_issuer: String,
    token_endpoint: url::Url,
    device_authorization_endpoint: url::Url,
    client_id: String,
}

impl ConnectPublicConfig {
    pub fn new(
        relay_url: impl AsRef<str>,
        clerk_publishable_key: impl AsRef<str>,
        client_id: impl Into<String>,
    ) -> Result<Self> {
        let relay_url = secure_origin(relay_url.as_ref(), "relay")?;
        let frontend = clerk_frontend_api(clerk_publishable_key.as_ref())?;
        let client_id = client_id.into();
        ensure!(
            !client_id.trim().is_empty(),
            "Missing Clerk OAuth client ID"
        );
        Ok(Self {
            relay_issuer: crate::normalize_issuer(relay_url.as_str()),
            relay_url,
            token_endpoint: frontend.join("/oauth/token")?,
            device_authorization_endpoint: frontend.join("/oauth/device_authorization")?,
            client_id,
        })
    }

    pub fn from_environment() -> Result<Option<Self>> {
        let relay = std::env::var("T3CODE_RELAY_URL")
            .ok()
            .or_else(|| option_env!("T3CODE_RELAY_URL").map(str::to_owned));
        let clerk = std::env::var("T3CODE_CLERK_PUBLISHABLE_KEY")
            .ok()
            .or_else(|| option_env!("T3CODE_CLERK_PUBLISHABLE_KEY").map(str::to_owned));
        let client = std::env::var("T3CODE_CLERK_CLI_OAUTH_CLIENT_ID")
            .ok()
            .or_else(|| option_env!("T3CODE_CLERK_CLI_OAUTH_CLIENT_ID").map(str::to_owned));
        match (relay, clerk, client) {
            (None, None, None) => Ok(None),
            (Some(relay), Some(clerk), Some(client)) => Self::new(relay, clerk, client).map(Some),
            _ => bail!(
                "T3 Connect requires T3CODE_RELAY_URL, T3CODE_CLERK_PUBLISHABLE_KEY, and T3CODE_CLERK_CLI_OAUTH_CLIENT_ID"
            ),
        }
    }

    pub fn relay_url(&self) -> &url::Url {
        &self.relay_url
    }

    pub fn relay_issuer(&self) -> &str {
        &self.relay_issuer
    }

    #[cfg(any(test, feature = "test-support"))]
    #[doc(hidden)]
    pub fn for_test(base: &str) -> Result<Self> {
        let base = url::Url::parse(base)?;
        Ok(Self {
            relay_url: base.clone(),
            relay_issuer: "https://relay.fixture.test".into(),
            token_endpoint: base.join("/oauth/token")?,
            device_authorization_endpoint: base.join("/oauth/device_authorization")?,
            client_id: "test-client".into(),
        })
    }
}

fn secure_origin(value: &str, label: &str) -> Result<url::Url> {
    let mut parsed = url::Url::parse(value.trim())?;
    ensure!(
        parsed.scheme() == "https"
            && parsed.host_str().is_some()
            && parsed.username().is_empty()
            && parsed.password().is_none()
            && parsed.query().is_none()
            && parsed.fragment().is_none(),
        "T3 Connect {label} URL must be an HTTPS origin"
    );
    parsed.set_path("/");
    Ok(parsed)
}

fn clerk_frontend_api(publishable_key: &str) -> Result<url::Url> {
    let encoded = publishable_key
        .strip_prefix("pk_live_")
        .or_else(|| publishable_key.strip_prefix("pk_test_"))
        .context("Invalid Clerk publishable key")?;
    let decoded = general_purpose::URL_SAFE_NO_PAD
        .decode(encoded)
        .or_else(|_| general_purpose::STANDARD_NO_PAD.decode(encoded))?;
    let host = String::from_utf8(decoded)?;
    let host = host.trim_end_matches('$');
    ensure!(
        !host.is_empty() && !host.contains('/') && !host.contains(['?', '#']),
        "Invalid Clerk frontend host"
    );
    secure_origin(&format!("https://{host}"), "Clerk frontend")
}

#[derive(Clone, Deserialize)]
pub struct DeviceAuthorization {
    device_code: String,
    pub user_code: String,
    pub verification_uri: String,
    pub verification_uri_complete: Option<String>,
    pub expires_in: u64,
    #[serde(default = "default_poll_seconds")]
    pub interval: u64,
}

fn default_poll_seconds() -> u64 {
    DEFAULT_POLL_SECONDS
}

impl DeviceAuthorization {
    pub fn verification_url(&self) -> &str {
        self.verification_uri_complete
            .as_deref()
            .unwrap_or(&self.verification_uri)
    }
}

#[derive(Clone, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct AccountCredential {
    access_token: String,
    refresh_token: String,
    expires_at_epoch_ms: i64,
    identity: Option<String>,
}

impl AccountCredential {
    pub fn identity(&self) -> Option<&str> {
        self.identity.as_deref()
    }

    pub fn expired(&self) -> bool {
        self.expires_at_epoch_ms <= chrono::Utc::now().timestamp_millis()
    }

    pub fn needs_refresh(&self) -> bool {
        self.expires_at_epoch_ms <= chrono::Utc::now().timestamp_millis() + 5 * 60 * 1000
    }

    #[cfg(feature = "test-support")]
    #[doc(hidden)]
    pub fn for_test(access_token: impl Into<String>) -> Self {
        Self {
            access_token: access_token.into(),
            refresh_token: "test-refresh".into(),
            expires_at_epoch_ms: chrono::Utc::now().timestamp_millis() + 60_000,
            identity: Some("test@example.com".into()),
        }
    }
}

#[derive(Deserialize)]
struct TokenResponse {
    access_token: String,
    refresh_token: Option<String>,
    id_token: Option<String>,
    expires_in: i64,
}

#[derive(Deserialize)]
struct OAuthError {
    error: String,
    error_description: Option<String>,
}

#[derive(Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RelayLinkResponse {
    pub ok: bool,
    pub cloud_user_id: String,
    pub environment_id: String,
    pub endpoint: Value,
    pub endpoint_runtime: Option<TunnelConfiguration>,
    pub relay_issuer: String,
    pub environment_credential: String,
    pub cloud_mint_public_key: String,
}

#[derive(Clone)]
pub struct AccountClient {
    client: reqwest::Client,
    config: ConnectPublicConfig,
}

impl AccountClient {
    pub fn new(config: ConnectPublicConfig) -> Result<Self> {
        Ok(Self {
            client: reqwest::Client::builder()
                .redirect(reqwest::redirect::Policy::none())
                .timeout(Duration::from_secs(30))
                .build()?,
            config,
        })
    }

    pub fn config(&self) -> &ConnectPublicConfig {
        &self.config
    }

    pub async fn begin_device_authorization(&self) -> Result<DeviceAuthorization> {
        let response = self
            .client
            .post(self.config.device_authorization_endpoint.clone())
            .form(&[
                ("client_id", self.config.client_id.as_str()),
                ("scope", OAUTH_SCOPES),
            ])
            .send()
            .await?;
        let mut authorization: DeviceAuthorization = read_success(response).await?;
        authorization.interval = authorization.interval.max(1);
        Ok(authorization)
    }

    pub async fn complete_device_authorization(
        &self,
        authorization: DeviceAuthorization,
    ) -> Result<AccountCredential> {
        let deadline = tokio::time::Instant::now() + Duration::from_secs(authorization.expires_in);
        let mut interval = authorization.interval;
        loop {
            tokio::time::sleep(Duration::from_secs(interval)).await;
            ensure!(
                tokio::time::Instant::now() < deadline,
                "T3 Connect authorization expired"
            );
            let response = self
                .client
                .post(self.config.token_endpoint.clone())
                .form(&[
                    ("grant_type", DEVICE_GRANT),
                    ("device_code", authorization.device_code.as_str()),
                    ("client_id", self.config.client_id.as_str()),
                ])
                .send()
                .await;
            let Ok(response) = response else {
                interval = interval.saturating_add(SLOW_DOWN_SECONDS);
                continue;
            };
            if response.status().is_server_error() {
                let _ = limited_bytes(response).await;
                interval = interval.saturating_add(SLOW_DOWN_SECONDS);
                continue;
            }
            if response.status().is_success() {
                let token: TokenResponse = read_json(response).await?;
                return credential(token, None);
            }
            let failure: OAuthError = read_json(response).await?;
            match failure.error.as_str() {
                "authorization_pending" => {}
                "slow_down" => interval = interval.saturating_add(SLOW_DOWN_SECONDS),
                "expired_token" => bail!("T3 Connect authorization expired"),
                "access_denied" => bail!("T3 Connect authorization was denied"),
                _ => bail!(
                    "T3 Connect authorization failed: {}",
                    failure.error_description.unwrap_or(failure.error)
                ),
            }
        }
    }

    pub async fn refresh(&self, credential_value: &AccountCredential) -> Result<AccountCredential> {
        ensure!(
            !credential_value.refresh_token.is_empty(),
            "T3 Connect credential cannot be refreshed"
        );
        let response = self
            .client
            .post(self.config.token_endpoint.clone())
            .form(&[
                ("grant_type", "refresh_token"),
                ("refresh_token", credential_value.refresh_token.as_str()),
                ("client_id", self.config.client_id.as_str()),
            ])
            .send()
            .await?;
        let token: TokenResponse = read_success(response).await?;
        credential(token, Some(credential_value))
    }

    pub async fn create_link_challenge(
        &self,
        credential: &AccountCredential,
        managed: bool,
    ) -> Result<String> {
        let response: Value = self
            .relay_request(
                reqwest::Method::POST,
                "/v1/client/environment-link-challenges",
                credential,
                Some(json!({
                    "notificationsEnabled": true,
                    "liveActivitiesEnabled": true,
                    "managedTunnelsEnabled": managed
                })),
            )
            .await?;
        response["challenge"]
            .as_str()
            .filter(|value| !value.is_empty())
            .map(str::to_owned)
            .context("Relay returned an invalid link challenge")
    }

    pub async fn link_environment(
        &self,
        credential: &AccountCredential,
        proof: &str,
        managed: bool,
    ) -> Result<RelayLinkResponse> {
        self.relay_request(
            reqwest::Method::POST,
            "/v1/client/environment-links",
            credential,
            Some(json!({
                "proof": proof,
                "notificationsEnabled": true,
                "liveActivitiesEnabled": true,
                "managedTunnelsEnabled": managed
            })),
        )
        .await
    }

    pub async fn unlink_environment(
        &self,
        credential: &AccountCredential,
        environment_id: &str,
    ) -> Result<()> {
        let path = format!(
            "/v1/client/environment-links/{}",
            url::form_urlencoded::byte_serialize(environment_id.as_bytes()).collect::<String>()
        );
        let _: Value = self
            .relay_request(reqwest::Method::DELETE, &path, credential, None)
            .await?;
        Ok(())
    }

    async fn relay_request<T: DeserializeOwned>(
        &self,
        method: reqwest::Method,
        path: &str,
        credential: &AccountCredential,
        body: Option<Value>,
    ) -> Result<T> {
        let url = self.config.relay_url.join(path)?;
        let mut request = self
            .client
            .request(method, url)
            .bearer_auth(&credential.access_token);
        if let Some(body) = body {
            request = request.json(&body);
        }
        read_success(request.send().await?).await
    }
}

fn credential(
    token: TokenResponse,
    previous: Option<&AccountCredential>,
) -> Result<AccountCredential> {
    ensure!(token.expires_in > 0, "OAuth token has an invalid lifetime");
    let identity = token
        .id_token
        .as_deref()
        .and_then(id_token_identity)
        .or_else(|| previous.and_then(|value| value.identity.clone()));
    Ok(AccountCredential {
        access_token: token.access_token,
        refresh_token: token
            .refresh_token
            .or_else(|| previous.map(|value| value.refresh_token.clone()))
            .unwrap_or_default(),
        expires_at_epoch_ms: chrono::Utc::now().timestamp_millis() + token.expires_in * 1000,
        identity,
    })
}

fn id_token_identity(token: &str) -> Option<String> {
    let encoded = token.split('.').nth(1)?;
    let decoded = general_purpose::URL_SAFE_NO_PAD.decode(encoded).ok()?;
    let claims: Value = serde_json::from_slice(&decoded).ok()?;
    ["email", "preferred_username", "sub"]
        .into_iter()
        .find_map(|key| claims[key].as_str().filter(|value| !value.is_empty()))
        .map(str::to_owned)
}

async fn read_success<T: DeserializeOwned>(response: reqwest::Response) -> Result<T> {
    let status = response.status();
    let bytes = limited_bytes(response).await?;
    if !status.is_success() {
        let detail = serde_json::from_slice::<Value>(&bytes)
            .ok()
            .and_then(|value| {
                value["message"]
                    .as_str()
                    .or_else(|| value["error_description"].as_str())
                    .map(str::to_owned)
            })
            .unwrap_or_else(|| status.to_string());
        bail!("T3 Connect request failed: {detail}");
    }
    serde_json::from_slice(&bytes).context("T3 Connect returned an invalid response")
}

async fn read_json<T: DeserializeOwned>(response: reqwest::Response) -> Result<T> {
    serde_json::from_slice(&limited_bytes(response).await?)
        .context("T3 Connect returned an invalid response")
}

async fn limited_bytes(response: reqwest::Response) -> Result<Vec<u8>> {
    if let Some(length) = response.content_length() {
        ensure!(
            length <= MAX_RESPONSE_BYTES as u64,
            "T3 Connect response exceeded the size limit"
        );
    }
    let mut response = response;
    let mut bytes = Vec::new();
    while let Some(chunk) = response.chunk().await? {
        ensure!(
            bytes.len() + chunk.len() <= MAX_RESPONSE_BYTES,
            "T3 Connect response exceeded the size limit"
        );
        bytes.extend_from_slice(&chunk);
    }
    Ok(bytes)
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::{Json, Router, extract::State, routing::post};
    use std::sync::Arc;
    use tokio::sync::Mutex;

    #[derive(Default)]
    struct Fixture {
        polls: usize,
    }

    async fn device() -> Json<Value> {
        Json(json!({
            "device_code":"device-secret",
            "user_code":"T3-CODE",
            "verification_uri":"https://accounts.example/device",
            "verification_uri_complete":"https://accounts.example/device?code=T3-CODE",
            "expires_in":30,
            "interval":1
        }))
    }

    async fn token(State(state): State<Arc<Mutex<Fixture>>>) -> (reqwest::StatusCode, Json<Value>) {
        let mut state = state.lock().await;
        state.polls += 1;
        if state.polls == 1 {
            return (
                reqwest::StatusCode::BAD_REQUEST,
                Json(json!({"error":"authorization_pending"})),
            );
        }
        let claims = general_purpose::URL_SAFE_NO_PAD
            .encode(serde_json::to_vec(&json!({"email":"person@example.com"})).unwrap());
        (
            reqwest::StatusCode::OK,
            Json(json!({
                "access_token":"account-access",
                "refresh_token":"account-refresh",
                "id_token":format!("header.{claims}.signature"),
                "expires_in":3600,
                "token_type":"Bearer"
            })),
        )
    }

    #[tokio::test]
    async fn device_authorization_waits_and_returns_a_persistable_credential() -> Result<()> {
        let state = Arc::new(Mutex::new(Fixture::default()));
        let router = Router::new()
            .route("/oauth/device_authorization", post(device))
            .route("/oauth/token", post(token))
            .with_state(state);
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
        let address = listener.local_addr()?;
        let task = tokio::spawn(async move { axum::serve(listener, router).await.unwrap() });
        let client =
            AccountClient::new(ConnectPublicConfig::for_test(&format!("http://{address}"))?)?;
        let mut authorization = client.begin_device_authorization().await?;
        assert_eq!(authorization.user_code, "T3-CODE");
        authorization.interval = 0;
        let credential = client.complete_device_authorization(authorization).await?;
        assert_eq!(credential.identity(), Some("person@example.com"));
        assert!(!credential.expired());
        task.abort();
        let _ = task.await;
        Ok(())
    }

    #[test]
    fn public_config_decodes_the_clerk_frontend_host() {
        let config = ConnectPublicConfig::new(
            "https://relay.t3.codes",
            "pk_live_Y2xlcmsudDMuY29kZXMk",
            "public-client",
        )
        .unwrap();
        assert_eq!(
            config.token_endpoint.as_str(),
            "https://clerk.t3.codes/oauth/token"
        );
    }
}
