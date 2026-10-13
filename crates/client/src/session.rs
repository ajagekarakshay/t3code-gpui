use anyhow::{Context, Result, ensure};
use reqwest::{Client as HttpClient, Method};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use t3_connect::DpopKey;
use url::Url;

/// A direct or hosted T3 pairing link, with its credential kept out of URL errors.
pub struct PairingTarget {
    origin: Url,
    credential: String,
}
impl PairingTarget {
    pub fn new(host: &str, credential: impl Into<String>) -> Result<Self> {
        let credential = credential.into();
        ensure!(!credential.trim().is_empty(), "A pairing code is required");
        Ok(Self {
            origin: normalize_origin(host)?,
            credential,
        })
    }
    pub fn from_url(input: &str) -> Result<Self> {
        let url = Url::parse(input.trim()).context("Invalid pairing link")?;
        ensure!(
            matches!(url.scheme(), "http" | "https" | "ws" | "wss"),
            "Unsupported pairing link protocol"
        );
        let fragment = url::form_urlencoded::parse(url.fragment().unwrap_or_default().as_bytes());
        let credential = fragment
            .chain(url.query_pairs())
            .find(|(key, _)| key == "token")
            .map(|(_, value)| value.into_owned())
            .context("The pairing link has no token")?;
        let host = url
            .query_pairs()
            .find(|(key, _)| key == "host")
            .map(|(_, value)| value.into_owned())
            .unwrap_or_else(|| url.origin().ascii_serialization());
        Self::new(&host, credential)
    }
    pub fn origin(&self) -> &Url {
        &self.origin
    }

    pub async fn exchange(self) -> Result<SavedSession> {
        let http = http_client()?;
        let key = DpopKey::generate();
        let descriptor =
            request_json(http.get(self.origin.join("/.well-known/t3/environment")?)).await?;
        let environment_id = text(&descriptor, "environmentId")?.to_owned();
        let label = descriptor["label"]
            .as_str()
            .unwrap_or("Remote environment")
            .to_owned();
        let endpoint = self.origin.join("/oauth/token")?;
        let proof = key.sign("POST", endpoint.as_str(), epoch(), None)?;
        let result = request_json(http.post(endpoint).header("DPoP", proof).form(&[
            (
                "grant_type",
                "urn:ietf:params:oauth:grant-type:token-exchange",
            ),
            (
                "subject_token_type",
                "urn:t3:params:oauth:token-type:environment-bootstrap",
            ),
            (
                "requested_token_type",
                "urn:ietf:params:oauth:token-type:access_token",
            ),
            ("subject_token", self.credential.as_str()),
            ("client_name", "T3 Code Native"),
            ("client_kind", "desktop"),
        ]))
        .await?;
        ensure!(
            result["token_type"] == "DPoP",
            "The environment did not bind the session to this device"
        );
        let lifetime = result["expires_in"]
            .as_i64()
            .filter(|v| *v > 0)
            .context("Invalid session expiry")?;
        Ok(SavedSession {
            origin: self.origin,
            environment_id,
            label,
            access_token: text(&result, "access_token")?.into(),
            proof_key: key.private_key_bytes().to_vec(),
            scopes: text(&result, "scope")?
                .split_whitespace()
                .map(str::to_owned)
                .collect(),
            expires_at: epoch()
                .checked_add(lifetime)
                .context("Invalid session lifetime")?,
        })
    }
}

/// Contains secrets. Serialize only into the operating system's credential store.
#[derive(Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SavedSession {
    origin: Url,
    environment_id: String,
    label: String,
    access_token: String,
    proof_key: Vec<u8>,
    scopes: Vec<String>,
    expires_at: i64,
}
impl SavedSession {
    pub fn environment_id(&self) -> &str {
        &self.environment_id
    }
    pub fn label(&self) -> &str {
        &self.label
    }
    pub fn origin(&self) -> &Url {
        &self.origin
    }
    pub fn scopes(&self) -> &[String] {
        &self.scopes
    }
    pub fn expired(&self) -> bool {
        self.expires_at <= epoch()
    }

    pub async fn get(&self, path: &str) -> Result<Value> {
        request_json(self.authorize(Method::GET, path)?).await
    }
    pub async fn post(&self, path: &str, input: &Value) -> Result<Value> {
        request_json(self.authorize(Method::POST, path)?.json(input)).await
    }
    fn authorize(&self, method: Method, path: &str) -> Result<reqwest::RequestBuilder> {
        ensure!(
            !self.expired(),
            "This environment session has expired. Pair again to reconnect."
        );
        ensure!(
            path.starts_with('/') && !path.starts_with("//"),
            "An environment-relative endpoint is required"
        );
        let origin = normalize_origin(self.origin.as_str())?;
        let url = origin.join(path)?;
        ensure!(
            url.origin() == origin.origin(),
            "Endpoint changes environment authority"
        );
        let key = DpopKey::from_private_key_bytes(&self.proof_key)?;
        let proof = key.sign(
            method.as_str(),
            url.as_str(),
            epoch(),
            Some(&self.access_token),
        )?;
        Ok(http_client()?
            .request(method, url)
            .header("Authorization", format!("DPoP {}", self.access_token))
            .header("DPoP", proof))
    }
    pub(crate) async fn websocket_url(&self) -> Result<Url> {
        let ticket = self
            .post("/api/auth/websocket-ticket", &Value::Null)
            .await?;
        let mut url = self.origin.join("/ws")?;
        url.set_scheme(if self.origin.scheme() == "https" {
            "wss"
        } else {
            "ws"
        })
        .map_err(|_| anyhow::anyhow!("Invalid WebSocket scheme"))?;
        url.query_pairs_mut()
            .append_pair("wsTicket", text(&ticket, "ticket")?)
            .append_pair("clientKind", "desktop")
            .append_pair("clientName", "T3 Code Native");
        Ok(url)
    }
}

pub(crate) fn epoch() -> i64 {
    chrono::Utc::now().timestamp()
}
pub(crate) fn text<'a>(value: &'a Value, field: &str) -> Result<&'a str> {
    value[field]
        .as_str()
        .filter(|v| !v.is_empty())
        .with_context(|| format!("Environment response is missing {field}"))
}
fn normalize_origin(input: &str) -> Result<Url> {
    let mut url = Url::parse(input.trim()).context("Invalid environment address")?;
    let scheme = match url.scheme() {
        "http" | "ws" => "http",
        "https" | "wss" => "https",
        _ => anyhow::bail!("Environment address must use HTTP or HTTPS"),
    };
    url.set_scheme(scheme)
        .map_err(|_| anyhow::anyhow!("Invalid environment protocol"))?;
    ensure!(
        url.host_str().is_some() && url.username().is_empty() && url.password().is_none(),
        "Environment address must have a host and no embedded credentials"
    );
    url.set_path("/");
    url.set_query(None);
    url.set_fragment(None);
    Ok(url)
}
fn http_client() -> Result<HttpClient> {
    Ok(HttpClient::builder()
        .redirect(reqwest::redirect::Policy::none())
        .timeout(std::time::Duration::from_secs(15))
        .build()?)
}
async fn request_json(request: reqwest::RequestBuilder) -> Result<Value> {
    // Strip request URLs from transport errors: pairing links and tickets are credentials.
    let mut response = request.send().await.map_err(|error| error.without_url())?;
    let status = response.status();
    let mut bytes = Vec::new();
    while let Some(chunk) = response
        .chunk()
        .await
        .map_err(|error| error.without_url())?
    {
        ensure!(
            bytes.len() + chunk.len() <= 8 * 1024 * 1024,
            "Environment response exceeded the size limit"
        );
        bytes.extend_from_slice(&chunk);
    }
    let value: Value =
        serde_json::from_slice(&bytes).context("Environment returned invalid JSON")?;
    ensure!(
        status.is_success(),
        "{}",
        value["message"]
            .as_str()
            .unwrap_or("Environment request failed")
    );
    Ok(value)
}
