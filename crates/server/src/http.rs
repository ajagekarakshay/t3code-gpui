use crate::{
    auth::{self, Session},
    store::{Store, required},
};
use anyhow::{Context, Result, ensure};
use axum::{
    Json, Router,
    extract::{
        Path, Query, State, WebSocketUpgrade,
        ws::{Message, WebSocket},
    },
    http::{HeaderMap, StatusCode, Uri},
    response::{IntoResponse, Response},
    routing::{get, post},
};
use futures::{SinkExt, StreamExt};
use serde_json::{Value, json};
use std::{
    collections::{BTreeMap, HashMap},
    path::PathBuf,
    sync::Arc,
};
use t3_acp::{AgentCommand, Session as AgentSession, SessionCommand};
use t3_connect::{AccountClient, AccountCredential, EnvironmentIdentity};
use tokio::sync::{Mutex, broadcast};

/// Server configuration belongs to its environment, including agent executables.
pub struct ServerOptions {
    home: PathBuf,
    label: String,
    agents: BTreeMap<String, AgentCommand>,
}
impl ServerOptions {
    pub fn new(home: impl Into<PathBuf>) -> Self {
        Self {
            home: home.into(),
            label: "Local environment".into(),
            agents: t3_acp::supported_agents(),
        }
    }
    pub fn label(mut self, label: impl Into<String>) -> Self {
        self.label = label.into();
        self
    }
    pub fn agents(mut self, agents: BTreeMap<String, AgentCommand>) -> Self {
        self.agents = agents;
        self
    }
}

pub(crate) struct Services {
    _home_lock: std::fs::File,
    pub(crate) home: PathBuf,
    pub(crate) store: Store,
    pub(crate) agents: BTreeMap<String, AgentCommand>,
    pub(crate) sessions: Mutex<HashMap<String, AgentSession>>,
    auth_connections: std::sync::Mutex<HashMap<String, usize>>,
    pub(crate) changes: broadcast::Sender<Value>,
    pub(crate) effects: Mutex<()>,
    pub(crate) filesystem: crate::filesystem::WorkspaceFilesystem,
    pub(crate) tasks: Mutex<Vec<tokio::task::JoinHandle<()>>>,
    pub(crate) identity: EnvironmentIdentity,
    pub(crate) descriptor: Value,
    pub(crate) cloud: Mutex<crate::cloud::CloudState>,
    pub(crate) activity: crate::activity::ActivityQueue,
}

#[derive(Clone)]
pub struct Server {
    pub(crate) services: Arc<Services>,
}
impl Server {
    pub fn open(options: ServerOptions) -> Result<Self> {
        let home = crate::home::prepare(&options.home)?;
        let home_lock = crate::home::lock(&home)?;
        let store = Store::open(&home.join("rust-state.sqlite"))?;
        let id = if let Some(id) = store.metadata("environmentId")? {
            id
        } else {
            let id = uuid::Uuid::new_v4().to_string();
            store.set_metadata("environmentId", &id)?;
            id
        };
        let identity = if let Some(pem) = store.metadata("environmentPrivateKey")? {
            EnvironmentIdentity::from_pem(&pem)?
        } else {
            let identity = EnvironmentIdentity::generate();
            store.set_metadata("environmentPrivateKey", &identity.private_pem()?)?;
            identity
        };
        let os = match std::env::consts::OS {
            "macos" => "darwin",
            "windows" => "windows",
            "linux" => "linux",
            _ => "unknown",
        };
        let arch = match std::env::consts::ARCH {
            "aarch64" => "arm64",
            "x86_64" => "x64",
            _ => "other",
        };
        // Do not advertise full V2 parity while the Rust projections are being ported.
        let descriptor = json!({"environmentId":id,"label":options.label,"platform":{"os":os,"arch":arch},"serverVersion":env!("CARGO_PKG_VERSION"),"orchestrationProtocolVersion":2,"capabilities":{"repositoryIdentity":false,"serverResolvedCommandContext":true},"rustMigration":true});
        let (changes, _) = broadcast::channel(256);
        Ok(Self {
            services: Arc::new(Services {
                _home_lock: home_lock,
                home: home.clone(),
                store,
                agents: options.agents,
                sessions: Mutex::default(),
                auth_connections: std::sync::Mutex::default(),
                changes,
                effects: Mutex::default(),
                filesystem: crate::filesystem::WorkspaceFilesystem::default(),
                tasks: Mutex::default(),
                identity,
                descriptor,
                cloud: Mutex::new(crate::cloud::CloudState::new(home)),
                activity: crate::activity::ActivityQueue::default(),
            }),
        })
    }

    pub fn router(&self) -> Router {
        Router::new()
            .route("/.well-known/t3/environment", get(descriptor))
            .route("/oauth/token", post(exchange))
            .route("/api/auth/session", get(session))
            .route("/api/auth/websocket-ticket", post(ticket))
            .route("/api/auth/pairing-token", post(pair))
            .route("/api/auth/pairing-links", get(pairing_links))
            .route("/api/auth/pairing-links/revoke", post(revoke_pairing_link))
            .route("/api/auth/clients", get(auth_clients))
            .route("/api/auth/clients/revoke", post(revoke_auth_client))
            .route(
                "/api/auth/clients/revoke-others",
                post(revoke_other_auth_clients),
            )
            .route("/api/orchestration/shell", get(shell))
            .route("/api/orchestration/threads/{id}", get(compatible_thread))
            .route(
                "/api/orchestration/threads/{id}/bounded",
                get(compatible_thread_bounded),
            )
            .route(
                "/api/orchestration/threads/{id}/history",
                get(compatible_thread_history),
            )
            .route("/api/rust/threads/{id}", get(thread))
            .route("/api/projects", get(projects))
            .route("/api/projects/mutate", post(dispatch))
            .route("/api/rust/dispatch", post(dispatch))
            .route("/api/connect/link-proof", post(link_proof))
            .route("/api/connect/relay-config", post(relay_configuration))
            .route("/api/connect/link-state", get(link_state))
            .route("/api/connect/unlink", post(unlink))
            .route("/api/connect/preferences", post(cloud_preferences))
            .route("/api/t3-connect/health", post(relay_health))
            .route("/api/connect/mint-credential", post(relay_mint))
            .route("/api/t3-connect/mint-credential", post(relay_mint))
            .route("/ws", get(websocket))
            .with_state(self.services.clone())
            .layer(axum::middleware::from_fn(
                |request: axum::extract::Request, next: axum::middleware::Next| async move {
                    let mut response = next.run(request).await;
                    response.headers_mut().insert(
                        "cache-control",
                        axum::http::HeaderValue::from_static("no-store"),
                    );
                    response
                },
            ))
    }

    pub fn descriptor(&self) -> &Value {
        &self.services.descriptor
    }
    pub fn bootstrap_credential(&self) -> Result<String> {
        let scopes = auth::STANDARD_SCOPES
            .iter()
            .chain(auth::ADMIN_SCOPES)
            .map(|s| s.to_string())
            .collect::<Vec<_>>();
        Ok(required(
            &self.services.store.grant(&scopes, None, 86400, true)?,
            "credential",
        )?
        .into())
    }
    pub fn shell(&self) -> Result<Value> {
        self.services.store.shell()
    }
    pub fn pairing_url(&self, origin: &str) -> Result<String> {
        let mut url = url::Url::parse(origin)?;
        ensure!(
            matches!(url.scheme(), "http" | "https")
                && url.host_str().is_some()
                && url.username().is_empty()
                && url.password().is_none(),
            "Invalid pairing origin"
        );
        let grant = self.services.store.grant(
            &auth::STANDARD_SCOPES
                .iter()
                .chain(auth::ADMIN_SCOPES)
                .map(|scope| scope.to_string())
                .collect::<Vec<_>>(),
            None,
            120,
            false,
        )?;
        url.set_path("/pair");
        url.set_query(None);
        url.set_fragment(Some(&format!("token={}", required(&grant, "credential")?)));
        Ok(url.to_string())
    }
    pub fn thread(&self, id: &str) -> Result<Value> {
        self.services.store.thread(id)
    }
    pub async fn dispatch(&self, command: Value) -> Result<Value> {
        self.services.dispatch(command).await
    }
    pub async fn configure_agent(&self, thread_id: &str, id: String, value: Value) -> Result<()> {
        self.services.configure_agent(thread_id, id, value).await
    }
    pub async fn authenticate_agent(&self, thread_id: &str, method: String) -> Result<()> {
        self.services.authenticate_agent(thread_id, method).await
    }
    pub fn subscribe(&self) -> broadcast::Receiver<Value> {
        self.services.changes.subscribe()
    }
    pub async fn cancel(&self, id: &str) -> Result<()> {
        self.services.cancel(id).await
    }
    pub async fn answer_permission(
        &self,
        thread: &str,
        id: &str,
        option: Option<String>,
    ) -> Result<()> {
        self.services.answer_permission(thread, id, option).await?;
        Ok(())
    }
    pub async fn start_connect(&self, address: std::net::SocketAddr) -> Result<()> {
        self.services.start_connect(address).await
    }
    pub async fn connect_link(
        &self,
        client: &AccountClient,
        credential: &AccountCredential,
        address: std::net::SocketAddr,
        managed: bool,
    ) -> Result<Value> {
        self.services
            .link_account(client, credential, address, managed)
            .await
    }
    pub async fn connect_unlink(
        &self,
        client: Option<(&AccountClient, &AccountCredential)>,
    ) -> Result<Value> {
        self.services.unlink_account(client).await
    }
    pub async fn connect_state(&self) -> Result<Value> {
        self.services.link_state().await
    }
    pub async fn connect_preferences(
        &self,
        publish_agent_activity: bool,
        hold_webhooks_while_offline: Option<bool>,
    ) -> Result<Value> {
        let mut preferences = json!({"publishAgentActivity":publish_agent_activity});
        if let Some(hold) = hold_webhooks_while_offline {
            preferences["holdWebhooksWhileOffline"] = json!(hold);
        }
        self.services.save_cloud_preferences(preferences).await?;
        self.services.link_state().await
    }
    #[cfg(feature = "test-support")]
    #[doc(hidden)]
    pub async fn publish_terminal_activity_for_test(&self, thread_id: &str) -> Result<()> {
        self.services
            .publish_activity(thread_id, crate::activity::ActivityKind::Terminal)
            .await
    }
    pub async fn shutdown(&self) {
        self.services.cloud.lock().await.stop().await;
        for session in self.services.sessions.lock().await.values() {
            let _ = session.send(SessionCommand::Shutdown).await;
        }
        for task in self.services.tasks.lock().await.drain(..) {
            task.abort();
            let _ = task.await;
        }
        self.services.sessions.lock().await.clear();
    }
}

struct ApiError(StatusCode, Value);
impl ApiError {
    fn new(status: StatusCode, message: impl std::fmt::Display) -> Self {
        let tag = match status {
            StatusCode::UNAUTHORIZED => "EnvironmentHttpUnauthorizedError",
            StatusCode::FORBIDDEN => "EnvironmentHttpForbiddenError",
            StatusCode::CONFLICT => "EnvironmentHttpConflictError",
            StatusCode::SERVICE_UNAVAILABLE => "EnvironmentCloudEndpointUnavailableError",
            _ => "EnvironmentHttpBadRequestError",
        };
        Self(status, json!({"_tag":tag,"message":message.to_string()}))
    }
}
impl From<anyhow::Error> for ApiError {
    fn from(error: anyhow::Error) -> Self {
        if let Some(crate::cloud::ConnectError::Unavailable(status)) =
            error.downcast_ref::<crate::cloud::ConnectError>()
        {
            let mut response = Self::new(StatusCode::SERVICE_UNAVAILABLE, &error);
            response.1["endpointRuntimeStatus"] = status.clone();
            return response;
        }
        let status = if error.is::<crate::cloud::ConnectError>() {
            StatusCode::UNAUTHORIZED
        } else if error.is::<auth::ProofReplayed>() {
            StatusCode::CONFLICT
        } else {
            StatusCode::BAD_REQUEST
        };
        Self::new(status, error)
    }
}
impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        (self.0, Json(self.1)).into_response()
    }
}
type ApiResult = std::result::Result<Json<Value>, ApiError>;

fn require_thread(s: &Services, id: &str) -> Result<(), ApiError> {
    if s.store.thread_available(id)? {
        Ok(())
    } else {
        Err(ApiError(
            StatusCode::NOT_FOUND,
            json!({
                "_tag":"EnvironmentResourceNotFoundError",
                "code":"not_found",
                "reason":"thread_not_found",
                "traceId":uuid::Uuid::new_v4().to_string()
            }),
        ))
    }
}

fn server_auth_descriptor() -> Value {
    json!({
        "policy":"remote-reachable",
        "bootstrapMethods":["desktop-bootstrap","one-time-token"],
        "sessionMethods":["bearer-access-token","dpop-access-token"],
        "sessionCookieName":"t3-session",
        "serverUpdateScope":"environment:maintain"
    })
}

impl Services {
    fn server_config(&self) -> Result<Value> {
        let checked_at = crate::store::now();
        let providers = self
            .agents
            .keys()
            .map(|instance_id| {
                let (driver, display_name) = if instance_id == "claude-code" {
                    ("claude-agent", "Claude Code")
                } else {
                    (instance_id.as_str(), "Codex")
                };
                json!({
                    "instanceId":instance_id,
                    "driver":driver,
                    "displayName":display_name,
                    "supportedRuntimeModes":["full-access"],
                    "enabled":true,
                    "installed":true,
                    "version":null,
                    "status":"ready",
                    "auth":{"status":"unknown"},
                    "checkedAt":checked_at,
                    "models":[{"slug":"default","name":"Default","isCustom":false,"isDefault":true,"capabilities":null}],
                    "slashCommands":[],
                    "skills":[]
                })
            })
            .collect::<Vec<_>>();
        let cwd = std::env::current_dir()?.to_string_lossy().into_owned();
        Ok(json!({
            "environment":self.descriptor,
            "auth":server_auth_descriptor(),
            "cwd":cwd,
            "keybindingsConfigPath":self.home.join("keybindings.json").to_string_lossy(),
            "keybindings":[],
            "issues":[],
            "providers":providers,
            "availableEditors":[],
            "observability":{
                "logsDirectoryPath":self.home.join("logs").to_string_lossy(),
                "localTracingEnabled":false,
                "otlpTracesEnabled":false,
                "otlpMetricsEnabled":false,
                "otlpLogsEnabled":false
            },
            "settings":{}
        }))
    }
}

fn request_url(headers: &HeaderMap, uri: &Uri) -> Result<String> {
    let host = headers
        .get("host")
        .context("Missing request host")?
        .to_str()?;
    let scheme = if headers
        .get("x-forwarded-proto")
        .is_some_and(|v| v == "https")
    {
        "https"
    } else {
        "http"
    };
    Ok(format!("{scheme}://{host}{uri}"))
}
fn authenticate(
    services: &Services,
    headers: &HeaderMap,
    uri: &Uri,
    method: &str,
    scope: Option<&str>,
) -> Result<Session, ApiError> {
    let session = services
        .store
        .authenticate(
            headers.get("authorization").and_then(|v| v.to_str().ok()),
            headers.get("dpop").and_then(|v| v.to_str().ok()),
            method,
            &request_url(headers, uri)?,
        )
        .map_err(|error| ApiError::new(StatusCode::UNAUTHORIZED, error))?;
    if let Some(scope) = scope {
        session
            .require(scope)
            .map_err(|error| ApiError::new(StatusCode::FORBIDDEN, error))?;
    }
    Ok(session)
}
async fn descriptor(State(services): State<Arc<Services>>) -> Json<Value> {
    Json(services.descriptor.clone())
}
async fn exchange(
    State(services): State<Arc<Services>>,
    headers: HeaderMap,
    uri: Uri,
    axum::Form(input): axum::Form<HashMap<String, String>>,
) -> ApiResult {
    let result = services
        .store
        .exchange(
            &serde_json::to_value(input).map_err(anyhow::Error::from)?,
            headers.get("dpop").and_then(|v| v.to_str().ok()),
            &request_url(&headers, &uri)?,
        )
        .map_err(|error| ApiError::new(StatusCode::UNAUTHORIZED, error))?;
    let _ = services.changes.send(json!({"authAccess":true}));
    Ok(Json(result))
}
async fn session(State(s): State<Arc<Services>>, h: HeaderMap, u: Uri) -> ApiResult {
    let mut result = authenticate(&s, &h, &u, "GET", None)?.metadata();
    result["auth"] = server_auth_descriptor();
    result["sessionMethod"] = result["method"].clone();
    Ok(Json(result))
}
async fn ticket(State(s): State<Arc<Services>>, h: HeaderMap, u: Uri) -> ApiResult {
    let session = authenticate(&s, &h, &u, "POST", None)?;
    Ok(Json(s.store.ticket(&session)?))
}
async fn pair(
    State(s): State<Arc<Services>>,
    h: HeaderMap,
    u: Uri,
    Json(input): Json<Value>,
) -> ApiResult {
    let session = authenticate(&s, &h, &u, "POST", Some("access:write"))?;
    let scopes: Vec<String> = if input.get("scopes").is_some() {
        serde_json::from_value(input["scopes"].clone()).map_err(anyhow::Error::from)?
    } else {
        auth::STANDARD_SCOPES
            .iter()
            .map(|scope| (*scope).into())
            .collect()
    };
    let result = s.store.delegate(
        &session,
        &scopes,
        input["label"]
            .as_str()
            .filter(|label| !label.trim().is_empty()),
    )?;
    let _ = s.changes.send(json!({"authAccess":true}));
    Ok(Json(result))
}
async fn pairing_links(State(s): State<Arc<Services>>, h: HeaderMap, u: Uri) -> ApiResult {
    let session = authenticate(&s, &h, &u, "GET", Some("access:read"))?;
    Ok(Json(
        s.store
            .auth_access(session.id(), &s.connected_session_ids()?)?["pairingLinks"]
            .clone(),
    ))
}
async fn revoke_pairing_link(
    State(s): State<Arc<Services>>,
    h: HeaderMap,
    u: Uri,
    Json(input): Json<Value>,
) -> ApiResult {
    authenticate(&s, &h, &u, "POST", Some("access:write"))?;
    let revoked = s.store.revoke_pairing_link(required(&input, "id")?)?;
    if revoked {
        let _ = s.changes.send(json!({"authAccess":true}));
    }
    Ok(Json(json!({"revoked":revoked})))
}
async fn auth_clients(State(s): State<Arc<Services>>, h: HeaderMap, u: Uri) -> ApiResult {
    let session = authenticate(&s, &h, &u, "GET", Some("access:read"))?;
    Ok(Json(
        s.store
            .auth_access(session.id(), &s.connected_session_ids()?)?["clientSessions"]
            .clone(),
    ))
}
async fn revoke_auth_client(
    State(s): State<Arc<Services>>,
    h: HeaderMap,
    u: Uri,
    Json(input): Json<Value>,
) -> ApiResult {
    let session = authenticate(&s, &h, &u, "POST", Some("access:write"))?;
    let revoked = s
        .store
        .revoke_session(session.id(), required(&input, "sessionId")?)?;
    if revoked {
        let _ = s.changes.send(json!({"authAccess":true}));
    }
    Ok(Json(json!({"revoked":revoked})))
}
async fn revoke_other_auth_clients(
    State(s): State<Arc<Services>>,
    h: HeaderMap,
    u: Uri,
) -> ApiResult {
    let session = authenticate(&s, &h, &u, "POST", Some("access:write"))?;
    let revoked_count = s.store.revoke_other_sessions(session.id())?;
    if revoked_count > 0 {
        let _ = s.changes.send(json!({"authAccess":true}));
    }
    Ok(Json(json!({"revokedCount":revoked_count})))
}
async fn shell(State(s): State<Arc<Services>>, h: HeaderMap, u: Uri) -> ApiResult {
    authenticate(&s, &h, &u, "GET", Some("orchestration:read"))?;
    Ok(Json(s.store.compatible_shell()?))
}
async fn compatible_thread(
    State(s): State<Arc<Services>>,
    h: HeaderMap,
    u: Uri,
    Path(id): Path<String>,
) -> ApiResult {
    authenticate(&s, &h, &u, "GET", Some("orchestration:read"))?;
    require_thread(&s, &id)?;
    Ok(Json(s.store.compatible_thread(&id)?))
}
async fn compatible_thread_bounded(
    State(s): State<Arc<Services>>,
    h: HeaderMap,
    u: Uri,
    Path(id): Path<String>,
    Query(query): Query<HashMap<String, String>>,
) -> ApiResult {
    authenticate(&s, &h, &u, "GET", Some("orchestration:read"))?;
    require_thread(&s, &id)?;
    Ok(Json(
        s.store.compatible_thread_bounded(
            &id,
            query
                .get("compactTurnItems")
                .is_some_and(|value| value == "1"),
        )?,
    ))
}
async fn compatible_thread_history(
    State(s): State<Arc<Services>>,
    h: HeaderMap,
    u: Uri,
    Path(id): Path<String>,
    Query(query): Query<HashMap<String, String>>,
) -> ApiResult {
    authenticate(&s, &h, &u, "GET", Some("orchestration:read"))?;
    require_thread(&s, &id)?;
    let cursor = query.get("cursor").context("Missing history cursor")?;
    let view = query.get("view").map(String::as_str);
    if !view.is_none_or(|view| matches!(view, "conversation" | "activity")) {
        return Err(ApiError::new(
            StatusCode::BAD_REQUEST,
            "Invalid history view",
        ));
    }
    Ok(Json(s.store.compatible_thread_history(
        &id,
        cursor,
        query.get("throughEntryId").map(String::as_str),
        view == Some("conversation"),
    )?))
}
async fn thread(
    State(s): State<Arc<Services>>,
    h: HeaderMap,
    u: Uri,
    Path(id): Path<String>,
) -> ApiResult {
    authenticate(&s, &h, &u, "GET", Some("orchestration:read"))?;
    Ok(Json(s.store.thread(&id)?))
}
async fn projects(State(s): State<Arc<Services>>, h: HeaderMap, u: Uri) -> ApiResult {
    authenticate(&s, &h, &u, "GET", Some("orchestration:read"))?;
    Ok(Json(
        json!({"projects":s.store.shell()?["projects"],"updatedAt":crate::store::now()}),
    ))
}
async fn dispatch(
    State(s): State<Arc<Services>>,
    h: HeaderMap,
    u: Uri,
    Json(input): Json<Value>,
) -> ApiResult {
    authenticate(&s, &h, &u, "POST", Some("orchestration:operate"))?;
    Ok(Json(s.dispatch(input).await?))
}

fn reject_forwarded(headers: &HeaderMap) -> Result<()> {
    for name in [
        "forwarded",
        "x-forwarded-host",
        "x-forwarded-port",
        "x-forwarded-proto",
    ] {
        ensure!(
            !headers.contains_key(name),
            "Forwarded authority is not accepted for relay control"
        );
    }
    Ok(())
}
async fn link_proof(
    State(s): State<Arc<Services>>,
    h: HeaderMap,
    u: Uri,
    Json(input): Json<Value>,
) -> ApiResult {
    authenticate(&s, &h, &u, "POST", Some("relay:write"))?;
    reject_forwarded(&h)?;
    Ok(Json(json!(s.link_proof(&input, &request_url(&h, &u)?)?)))
}
async fn relay_configuration(
    State(s): State<Arc<Services>>,
    h: HeaderMap,
    u: Uri,
    Json(input): Json<Value>,
) -> ApiResult {
    authenticate(&s, &h, &u, "POST", Some("relay:write"))?;
    Ok(Json(s.configure_relay(input).await?))
}
async fn link_state(State(s): State<Arc<Services>>, h: HeaderMap, u: Uri) -> ApiResult {
    authenticate(&s, &h, &u, "GET", Some("relay:read"))?;
    Ok(Json(s.link_state().await?))
}
async fn unlink(State(s): State<Arc<Services>>, h: HeaderMap, u: Uri) -> ApiResult {
    authenticate(&s, &h, &u, "POST", Some("relay:write"))?;
    Ok(Json(s.unlink().await?))
}
async fn cloud_preferences(
    State(s): State<Arc<Services>>,
    h: HeaderMap,
    u: Uri,
    Json(input): Json<Value>,
) -> ApiResult {
    authenticate(&s, &h, &u, "POST", Some("relay:write"))?;
    s.save_cloud_preferences(input).await?;
    Ok(Json(Value::Null))
}
async fn relay_health(State(s): State<Arc<Services>>, Json(input): Json<Value>) -> ApiResult {
    Ok(Json(s.relay_health(&input).await?))
}
async fn relay_mint(State(s): State<Arc<Services>>, Json(input): Json<Value>) -> ApiResult {
    Ok(Json(s.relay_mint(&input).await?))
}

async fn websocket(
    State(s): State<Arc<Services>>,
    h: HeaderMap,
    u: Uri,
    Query(query): Query<HashMap<String, String>>,
    upgrade: WebSocketUpgrade,
) -> Result<Response, ApiError> {
    let session = if h.contains_key("authorization") {
        authenticate(&s, &h, &u, "GET", None)?
    } else {
        s.store
            .redeem_ticket(query.get("wsTicket").context("WebSocket ticket required")?)
            .map_err(|e| ApiError::new(StatusCode::UNAUTHORIZED, e))?
    };
    Ok(upgrade
        .max_message_size(8 * 1024 * 1024)
        .on_upgrade(move |socket| serve_socket(s, session, socket)))
}

struct RpcSubscription {
    id: Value,
    tag: String,
    input: Value,
    awaiting_ack: bool,
    dirty: bool,
}
impl Services {
    fn connected_session_ids(&self) -> Result<Vec<String>> {
        Ok(self
            .auth_connections
            .lock()
            .map_err(|_| anyhow::anyhow!("Auth connection lock poisoned"))?
            .keys()
            .cloned()
            .collect())
    }
}
struct AuthConnectionGuard {
    services: Arc<Services>,
    session_id: String,
}
impl AuthConnectionGuard {
    fn open(services: Arc<Services>, session_id: String) -> Result<Self> {
        *services
            .auth_connections
            .lock()
            .map_err(|_| anyhow::anyhow!("Auth connection lock poisoned"))?
            .entry(session_id.clone())
            .or_default() += 1;
        let _ = services.changes.send(json!({"authAccess":true}));
        Ok(Self {
            services,
            session_id,
        })
    }
}
impl Drop for AuthConnectionGuard {
    fn drop(&mut self) {
        if let Ok(mut connections) = self.services.auth_connections.lock()
            && let Some(count) = connections.get_mut(&self.session_id)
        {
            *count -= 1;
            if *count == 0 {
                connections.remove(&self.session_id);
            }
        }
        let _ = self.services.changes.send(json!({"authAccess":true}));
    }
}
fn request_key(id: &Value) -> Option<String> {
    (id.is_string() || id.as_u64().is_some()).then(|| id.to_string())
}
fn rpc_failure(id: &Value, error: &anyhow::Error) -> Value {
    // Effect's CauseEncoded is an array. A defect is decodable without a provider-specific error schema.
    json!({"_tag":"Exit","requestId":id,"exit":{"_tag":"Failure","cause":[{"_tag":"Die","defect":{"message":error.to_string()}}]}})
}
async fn send_rpc(
    sender: &mut futures::stream::SplitSink<WebSocket, Message>,
    response: Value,
) -> bool {
    matches!(
        tokio::time::timeout(
            std::time::Duration::from_secs(10),
            sender.send(Message::Text(response.to_string().into()))
        )
        .await,
        Ok(Ok(()))
    )
}
async fn flush_subscription(
    s: &Arc<Services>,
    session: &Session,
    subscription: &mut RpcSubscription,
    sender: &mut futures::stream::SplitSink<WebSocket, Message>,
) -> bool {
    if subscription.awaiting_ack || !subscription.dirty {
        return true;
    }
    let response = match rpc(s, session, &subscription.tag, &subscription.input).await {
        Ok(value) => {
            if subscription.tag == "rust.subscribeEvents" {
                subscription.input["afterSequence"] = value["cursor"].clone();
                subscription.dirty = value["hasMore"].as_bool().unwrap_or(false);
            } else {
                subscription.dirty = false;
            }
            json!({"_tag":"Chunk","requestId":subscription.id,"values":[value]})
        }
        Err(error) => {
            let _ = send_rpc(sender, rpc_failure(&subscription.id, &error)).await;
            return false;
        }
    };
    subscription.awaiting_ack = true;
    send_rpc(sender, response).await
}
async fn serve_socket(s: Arc<Services>, session: Session, socket: WebSocket) {
    let _auth_connection = match AuthConnectionGuard::open(s.clone(), session.id().to_owned()) {
        Ok(connection) => connection,
        Err(_) => return,
    };
    let (mut sender, mut receiver) = socket.split();
    let mut changes = s.changes.subscribe();
    let mut subscriptions = HashMap::<String, RpcSubscription>::new();
    loop {
        tokio::select! {
            received=receiver.next()=>{
                let text=match received { Some(Ok(Message::Text(text)))=>text, Some(Ok(Message::Ping(_) | Message::Pong(_)))=>continue, _=>break };
                let parsed=serde_json::from_str::<Value>(&text);
                let requests=match parsed {
                    Ok(Value::Array(batch))=>batch,
                    Ok(value)=>vec![value],
                    Err(_)=>text.lines().filter_map(|line|serde_json::from_str(line).ok()).collect(),
                };
                if !s.store.session_valid(session.id()).unwrap_or(false){return;}
                for request in requests {
                    match request["_tag"].as_str() {
                        Some("Ping")=>{if !send_rpc(&mut sender,json!({"_tag":"Pong"})).await{return;}continue;},
                        Some("Interrupt")=>{if let Some(key)=request_key(&request["requestId"]){subscriptions.remove(&key);}continue;},
                        Some("Ack")=>{
                            if let Some(key)=request_key(&request["requestId"])
                                && let Some(subscription)=subscriptions.get_mut(&key){
                                    subscription.awaiting_ack=false;
                                    if !flush_subscription(&s,&session,subscription,&mut sender).await{return;}
                                }
                            continue;
                        },
                        Some("Request")=>{},
                        _=>continue,
                    }
                    let id=&request["id"];
                    let Some(key)=request_key(id)else{continue};
                    let tag=request["tag"].as_str().unwrap_or_default();let input=&request["payload"];
                    let streaming=matches!(tag,
                        "orchestration.subscribeShell"
                        | "orchestration.subscribeArchivedShell"
                        | "orchestration.subscribeThread"
                        | "orchestration.searchThreadStream"
                        | "subscribeServerConfig"
                        | "subscribeServerLifecycle"
                        | "subscribeAuthAccess"
                        | "subscribeVcsStatus"
                        | "provider.auth.subscribe"
                        | "rust.subscribeThread"
                        | "rust.subscribeEvents"
                    );
                    let response=match rpc(&s,&session,tag,input).await {
                        Ok(value) if streaming=>{
                            let mut input=input.clone();
                            let dirty=tag=="rust.subscribeEvents" && value["hasMore"]==true;
                            if tag=="rust.subscribeEvents" {input["afterSequence"]=value["cursor"].clone();}
                            subscriptions.insert(key,RpcSubscription{id:id.clone(),tag:tag.into(),input,awaiting_ack:true,dirty});
                            json!({"_tag":"Chunk","requestId":id,"values":[value]})
                        },
                        Ok(value)=>json!({"_tag":"Exit","requestId":id,"exit":{"_tag":"Success","value":value}}),
                        Err(error)=>rpc_failure(id,&error),
                    };
                    if !send_rpc(&mut sender,response).await{return;}
                }
            }
            change=changes.recv()=>{
                let change=match change { Ok(change)=>change, Err(broadcast::error::RecvError::Lagged(_))=>json!({}), Err(_)=>break };
                if !s.store.session_valid(session.id()).unwrap_or(false){break;}
                for subscription in subscriptions.values_mut(){
                    let relevant=match subscription.tag.as_str() {
                        "subscribeAuthAccess" => change["authAccess"] == true,
                        "subscribeServerConfig" | "subscribeServerLifecycle" | "provider.auth.subscribe" => false,
                        "subscribeVcsStatus" => change["vcsCwd"] == subscription.input["cwd"],
                        "orchestration.subscribeShell" | "orchestration.subscribeArchivedShell" | "rust.subscribeEvents" => true,
                        "orchestration.searchThreadStream" => change["threadId"]==subscription.input["threadId"],
                        _ => change["threadId"].is_null() || change["threadId"]==subscription.input["threadId"],
                    };
                    if relevant {
                        subscription.dirty=true;
                        if !flush_subscription(&s,&session,subscription,&mut sender).await{return;}
                    }
                }
            }
        }
    }
}

fn notify_vcs(services: &Services, input: &Value) {
    let _ = services.changes.send(json!({"vcsCwd":input["cwd"]}));
}

async fn rpc(s: &Arc<Services>, session: &Session, tag: &str, input: &Value) -> Result<Value> {
    let scope = match tag {
        "subscribeAuthAccess" => "access:read",
        "projects.writeFile" => "filesystem:write",
        "projects.listEntries"
        | "projects.readFile"
        | "projects.searchContents"
        | "projects.searchEntries" => "filesystem:read",
        "rust.configureAgent"
        | "rust.authenticateAgent"
        | "provider.auth.subscribe"
        | "server.refreshProviders" => "providers:manage",
        "vcs.pull" | "vcs.createWorktree" | "vcs.removeWorktree" | "vcs.createRef"
        | "vcs.switchRef" | "vcs.init" => "source-control:write",
        "orchestration.dispatchCommand"
        | "orchestration.launchThread"
        | "projects.mutate"
        | "rust.cancel"
        | "rust.answerPermission" => "orchestration:operate",
        _ => "orchestration:read",
    };
    session.require(scope)?;
    match tag {
        "server.probe" => Ok(s.descriptor.clone()),
        "server.getConfig" => s.server_config(),
        "subscribeServerConfig" => {
            Ok(json!({"version":1,"type":"snapshot","config":s.server_config()?}))
        }
        "subscribeServerLifecycle" => {
            let cwd = std::env::current_dir()?;
            let project_name = cwd
                .file_name()
                .and_then(std::ffi::OsStr::to_str)
                .filter(|name| !name.is_empty())
                .unwrap_or("T3 Code");
            Ok(json!({
                "version":1,
                "sequence":0,
                "type":"welcome",
                "payload":{
                    "environment":s.descriptor,
                    "cwd":cwd.to_string_lossy(),
                    "projectName":project_name,
                    "bootstrapStatus":"complete"
                }
            }))
        }
        "subscribeAuthAccess" => Ok(json!({
            "version":1,
            "revision":crate::auth::epoch(),
            "type":"snapshot",
            "payload":s.store.auth_access(session.id(), &s.connected_session_ids()?)?
        })),
        "server.reportClientActivity" => Ok(Value::Null),
        "provider.auth.subscribe" => {
            let instance_id = required(input, "instanceId")?;
            ensure!(
                s.agents.contains_key(instance_id),
                "Unknown provider instance"
            );
            Ok(json!({
                "instanceId":instance_id,
                "phase":"idle",
                "flowId":null,
                "authorizationUrl":null,
                "expiresAt":null,
                "message":null,
                "credentialOwner":"provider"
            }))
        }
        "server.refreshProviders" => {
            if let Some(instance_id) = input["instanceId"].as_str() {
                ensure!(
                    s.agents.contains_key(instance_id),
                    "Unknown provider instance"
                );
            }
            Ok(json!({}))
        }
        "projects.list" => {
            Ok(json!({"projects":s.store.shell()?["projects"],"updatedAt":crate::store::now()}))
        }
        "projects.listEntries" => {
            crate::filesystem::list_entries(s.filesystem.clone(), input.clone()).await
        }
        "projects.readFile" => crate::filesystem::read_file(input.clone()).await,
        "projects.searchContents" => {
            crate::filesystem::search_contents(s.filesystem.clone(), input.clone()).await
        }
        "projects.searchEntries" => {
            crate::filesystem::search_entries(s.filesystem.clone(), input.clone()).await
        }
        "projects.writeFile" => {
            crate::filesystem::write_file(s.filesystem.clone(), input.clone()).await
        }
        "subscribeVcsStatus" => Ok(crate::vcs::status_stream_snapshot(
            crate::vcs::status(input).await?,
        )),
        "vcs.refreshStatus" => crate::vcs::status(input).await,
        "vcs.listRefs" => crate::vcs::list_refs(input).await,
        "vcs.createWorktree" => {
            let result = crate::vcs::create_worktree(&s.home, input).await?;
            notify_vcs(s, input);
            Ok(result)
        }
        "vcs.removeWorktree" => {
            let result = crate::vcs::remove_worktree(input).await?;
            notify_vcs(s, input);
            Ok(result)
        }
        "vcs.createRef" => {
            let result = crate::vcs::create_ref(input).await?;
            notify_vcs(s, input);
            Ok(result)
        }
        "vcs.switchRef" => {
            let result = crate::vcs::switch_ref(input).await?;
            notify_vcs(s, input);
            Ok(result)
        }
        "vcs.init" => {
            let result = crate::vcs::init(input).await?;
            notify_vcs(s, input);
            Ok(result)
        }
        "vcs.pull" => {
            let result = crate::vcs::pull(input).await?;
            notify_vcs(s, input);
            Ok(result)
        }
        "orchestration.dispatchCommand" | "projects.mutate" => s.dispatch(input.clone()).await,
        "orchestration.launchThread" => s.launch_thread(input).await,
        "orchestration.subscribeShell" => {
            Ok(json!({"kind":"snapshot","snapshot":s.store.compatible_shell()?}))
        }
        "orchestration.getArchivedShellSnapshot" => {
            let shell = s.store.compatible_shell()?;
            Ok(json!({
                "schemaVersion":shell["schemaVersion"],
                "snapshotSequence":shell["snapshotSequence"],
                "projects":shell["projects"],
                "threads":shell["archivedThreads"]
            }))
        }
        "orchestration.subscribeArchivedShell" => {
            let shell = s.store.compatible_shell()?;
            Ok(json!({"kind":"snapshot","snapshot":{
                "schemaVersion":shell["schemaVersion"],
                "snapshotSequence":shell["snapshotSequence"],
                "projects":shell["projects"],
                "threads":shell["archivedThreads"]
            }}))
        }
        "orchestration.getThreadProjection" => {
            Ok(s.store.compatible_thread(required(input, "threadId")?)?["projection"].clone())
        }
        "orchestration.searchThreads" => s.store.search_threads(input),
        "orchestration.searchThread" | "orchestration.searchThreadStream" => {
            s.store.search_thread(input)
        }
        "orchestration.subscribeThread" => {
            if input["acceptBoundedSnapshot"] == true {
                let snapshot = s.store.compatible_thread_bounded(
                    required(input, "threadId")?,
                    input["acceptCompactTurnItems"] == true,
                )?;
                let mut response = json!({"kind":"snapshot","snapshotSequence":snapshot["snapshotSequence"],"projection":snapshot["projection"],"historyCursor":snapshot["historyCursor"],"hasMoreHistory":snapshot["hasMoreHistory"],"latestLocalTurnOrdinal":snapshot["latestLocalTurnOrdinal"],"payloadBudgetExceeded":snapshot["payloadBudgetExceeded"]});
                if snapshot["turnItemsOmitLocalVisible"] == true {
                    response["turnItemsOmitLocalVisible"] = json!(true);
                }
                Ok(response)
            } else {
                let snapshot = s.store.compatible_thread(required(input, "threadId")?)?;
                Ok(
                    json!({"kind":"snapshot","snapshotSequence":snapshot["snapshotSequence"],"projection":snapshot["projection"]}),
                )
            }
        }
        "rust.getThread" | "rust.subscribeThread" => s.store.thread(required(input, "threadId")?),
        "rust.eventsAfter" | "rust.subscribeEvents" => s.store.changes_after(
            input["afterSequence"]
                .as_i64()
                .context("Invalid event cursor")?,
        ),
        "rust.cancel" => {
            s.cancel(required(input, "threadId")?).await?;
            Ok(Value::Null)
        }
        "rust.configureAgent" => {
            s.configure_agent(
                required(input, "threadId")?,
                required(input, "id")?.into(),
                input["value"].clone(),
            )
            .await?;
            Ok(Value::Null)
        }
        "rust.authenticateAgent" => {
            s.authenticate_agent(
                required(input, "threadId")?,
                required(input, "method")?.into(),
            )
            .await?;
            Ok(Value::Null)
        }
        "rust.answerPermission" => {
            s.answer_permission(
                required(input, "threadId")?,
                required(input, "id")?,
                input["optionId"].as_str().map(str::to_owned),
            )
            .await?;
            Ok(Value::Null)
        }
        _ => anyhow::bail!("RPC method is not ported yet: {tag}"),
    }
}
