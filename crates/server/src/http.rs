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
use t3_connect::EnvironmentIdentity;
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
    pub(crate) store: Store,
    pub(crate) agents: BTreeMap<String, AgentCommand>,
    pub(crate) sessions: Mutex<HashMap<String, AgentSession>>,
    pub(crate) changes: broadcast::Sender<Value>,
    pub(crate) effects: Mutex<()>,
    pub(crate) tasks: Mutex<Vec<tokio::task::JoinHandle<()>>>,
    identity: EnvironmentIdentity,
    descriptor: Value,
}

#[derive(Clone)]
pub struct Server {
    services: Arc<Services>,
}
impl Server {
    pub fn open(options: ServerOptions) -> Result<Self> {
        std::fs::create_dir_all(&options.home)?;
        let home = std::fs::canonicalize(&options.home)?;
        if let Some(user_home) =
            std::env::var_os("HOME").or_else(|| std::env::var_os("USERPROFILE"))
        {
            let live = PathBuf::from(user_home).join(".t3");
            let live = std::fs::canonicalize(&live).unwrap_or(live);
            ensure!(
                !home.starts_with(&live),
                "The Rust migration must use a separate data directory from ~/.t3"
            );
        }
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            std::fs::set_permissions(&home, std::fs::Permissions::from_mode(0o700))?;
        }
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
        let descriptor = json!({"environmentId":id,"label":options.label,"platform":{"os":os,"arch":arch},"serverVersion":env!("CARGO_PKG_VERSION"),"capabilities":{"repositoryIdentity":false},"rustMigration":true});
        let (changes, _) = broadcast::channel(256);
        Ok(Self {
            services: Arc::new(Services {
                store,
                agents: options.agents,
                sessions: Mutex::default(),
                changes,
                effects: Mutex::default(),
                tasks: Mutex::default(),
                identity,
                descriptor,
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
            .route("/api/orchestration/shell", get(shell))
            .route("/api/rust/threads/{id}", get(thread))
            .route("/api/projects", get(projects))
            .route("/api/projects/mutate", post(dispatch))
            .route("/api/rust/dispatch", post(dispatch))
            .route("/api/connect/link-proof", post(link_proof))
            .route("/api/connect/relay-config", post(relay_configuration))
            .route("/api/t3-connect/health", post(relay_health))
            .route("/api/connect/mint-credential", post(relay_mint))
            .route("/api/t3-connect/mint-credential", post(relay_mint))
            .route("/ws", get(websocket))
            .with_state(self.services.clone())
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
    pub fn thread(&self, id: &str) -> Result<Value> {
        self.services.store.thread(id)
    }
    pub async fn dispatch(&self, command: Value) -> Result<Value> {
        self.services.dispatch(command).await
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
        self.services.answer_permission(thread, id, option).await
    }
    pub async fn shutdown(&self) {
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

struct ApiError(StatusCode, String);
impl From<anyhow::Error> for ApiError {
    fn from(error: anyhow::Error) -> Self {
        Self(StatusCode::BAD_REQUEST, error.to_string())
    }
}
impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        (self.0,Json(json!({"code":if self.0==StatusCode::UNAUTHORIZED{"auth_invalid"}else{"invalid_request"},"message":self.1,"traceId":uuid::Uuid::new_v4().to_string()}))).into_response()
    }
}
type ApiResult = std::result::Result<Json<Value>, ApiError>;

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
        .map_err(|error| ApiError(StatusCode::UNAUTHORIZED, error.to_string()))?;
    if let Some(scope) = scope {
        session
            .require(scope)
            .map_err(|error| ApiError(StatusCode::FORBIDDEN, error.to_string()))?;
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
    Ok(Json(
        services
            .store
            .exchange(
                &serde_json::to_value(input).map_err(anyhow::Error::from)?,
                headers.get("dpop").and_then(|v| v.to_str().ok()),
                &request_url(&headers, &uri)?,
            )
            .map_err(|error| ApiError(StatusCode::UNAUTHORIZED, error.to_string()))?,
    ))
}
async fn session(State(s): State<Arc<Services>>, h: HeaderMap, u: Uri) -> ApiResult {
    Ok(Json(authenticate(&s, &h, &u, "GET", None)?.metadata()))
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
    let scopes: Vec<String> =
        serde_json::from_value(input["scopes"].clone()).map_err(anyhow::Error::from)?;
    Ok(Json(s.store.delegate(&session, &scopes)?))
}
async fn shell(State(s): State<Arc<Services>>, h: HeaderMap, u: Uri) -> ApiResult {
    authenticate(&s, &h, &u, "GET", Some("orchestration:read"))?;
    Ok(Json(s.store.shell()?))
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
fn issuer(value: &str) -> Result<String> {
    let parsed = url::Url::parse(value)?;
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

async fn link_proof(
    State(s): State<Arc<Services>>,
    h: HeaderMap,
    u: Uri,
    Json(input): Json<Value>,
) -> ApiResult {
    authenticate(&s, &h, &u, "POST", Some("relay:write"))?;
    reject_forwarded(&h)?;
    let relay = issuer(required(&input, "relayIssuer")?)?;
    let origin = &input["origin"];
    let port = origin["localHttpPort"]
        .as_u64()
        .filter(|v| *v <= 65535)
        .context("Invalid tunnel port")? as u16;
    t3_connect::TunnelConfiguration::validate_origin(required(origin, "localHttpHost")?, port)?;
    let host = h
        .get("host")
        .context("Missing host")?
        .to_str()
        .map_err(anyhow::Error::from)?;
    let request = url::Url::parse(&format!("http://{host}")).map_err(anyhow::Error::from)?;
    if request.port_or_known_default() != Some(port) {
        return Err(anyhow::anyhow!("Tunnel port must match this environment").into());
    }
    let timestamp = auth::epoch();
    let id = required(&s.descriptor, "environmentId")?;
    let proof=s.identity.sign(t3_connect::LINK_PROOF_TYPE,&json!({"iss":format!("t3-env:{id}"),"aud":relay,"sub":id,"jti":uuid::Uuid::new_v4().to_string(),"iat":timestamp,"exp":timestamp+300,"challenge":required(&input,"challenge")?,"descriptor":s.descriptor,"environmentId":id,"environmentPublicKey":s.identity.public_pem()?,"endpoint":input["endpoint"],"origin":origin,"scopes":["agent_activity_notifications","managed_tunnels"]}))?;
    Ok(Json(json!(proof)))
}

async fn relay_configuration(
    State(s): State<Arc<Services>>,
    h: HeaderMap,
    u: Uri,
    Json(input): Json<Value>,
) -> ApiResult {
    authenticate(&s, &h, &u, "POST", Some("relay:write"))?;
    reject_forwarded(&h)?;
    issuer(required(&input, "relayUrl")?)?;
    issuer(
        input["relayIssuer"]
            .as_str()
            .unwrap_or(required(&input, "relayUrl")?),
    )?;
    required(&input, "cloudUserId")?;
    required(&input, "cloudMintPublicKey")?;
    required(&input, "environmentCredential")?;
    if !input["endpointRuntime"].is_null() {
        let _: t3_connect::TunnelConfiguration =
            serde_json::from_value(input["endpointRuntime"].clone())
                .map_err(anyhow::Error::from)?;
    }
    s.store.set_metadata("relayConfig", &input.to_string())?;
    Ok(Json(json!({"ok":true})))
}

fn relay_claims(
    s: &Services,
    input: &Value,
    typ: &str,
    scope: &str,
) -> Result<(Value, Value, String)> {
    let configuration: Value = serde_json::from_str(
        &s.store
            .metadata("relayConfig")?
            .context("Environment is not linked")?,
    )?;
    let relay = issuer(
        configuration["relayIssuer"]
            .as_str()
            .unwrap_or(required(&configuration, "relayUrl")?),
    )?;
    let id = required(&s.descriptor, "environmentId")?;
    let claims = t3_connect::verify_relay_proof(
        required(&configuration, "cloudMintPublicKey")?,
        required(input, "proof")?,
        typ,
        &relay,
        &format!("t3-env:{id}"),
        auth::epoch(),
    )?;
    ensure!(
        claims["environmentId"] == id
            && claims["sub"] == configuration["cloudUserId"]
            && claims["scope"] == json!([scope]),
        "Relay authority mismatch"
    );
    Ok((claims, configuration, relay))
}
async fn relay_health(
    State(s): State<Arc<Services>>,
    h: HeaderMap,
    Json(input): Json<Value>,
) -> ApiResult {
    reject_forwarded(&h)?;
    let (claims, _, relay) = relay_claims(
        &s,
        &input,
        t3_connect::HEALTH_REQUEST_TYPE,
        "environment:status",
    )?;
    s.store.consume_relay_proof("health", &claims)?;
    let timestamp = auth::epoch();
    let id = required(&s.descriptor, "environmentId")?;
    let checked = auth::iso(timestamp);
    let proof=s.identity.sign(t3_connect::HEALTH_RESPONSE_TYPE,&json!({"iss":format!("t3-env:{id}"),"aud":relay,"sub":id,"jti":uuid::Uuid::new_v4().to_string(),"iat":timestamp,"exp":timestamp+300,"environmentId":id,"requestNonce":claims["nonce"],"status":"online","descriptor":s.descriptor,"checkedAt":checked}))?;
    Ok(Json(
        json!({"environmentId":id,"status":"online","descriptor":s.descriptor,"checkedAt":checked,"proof":proof}),
    ))
}
async fn relay_mint(
    State(s): State<Arc<Services>>,
    h: HeaderMap,
    Json(input): Json<Value>,
) -> ApiResult {
    reject_forwarded(&h)?;
    let (claims, _, relay) = relay_claims(
        &s,
        &input,
        t3_connect::MINT_REQUEST_TYPE,
        "environment:connect",
    )?;
    let thumbprint = required(&claims, "clientProofKeyThumbprint")?;
    if claims["cnf"]["jkt"] != thumbprint {
        return Err(anyhow::anyhow!("Client proof key mismatch").into());
    }
    s.store.consume_relay_proof("mint", &claims)?;
    let scopes = auth::STANDARD_SCOPES
        .iter()
        .map(|s| s.to_string())
        .collect::<Vec<_>>();
    let grant = s.store.grant(&scopes, Some(thumbprint), 120, false)?;
    let timestamp = auth::epoch();
    let id = required(&s.descriptor, "environmentId")?;
    let proof=s.identity.sign(t3_connect::MINT_RESPONSE_TYPE,&json!({"iss":format!("t3-env:{id}"),"aud":relay,"sub":id,"jti":uuid::Uuid::new_v4().to_string(),"iat":timestamp,"exp":timestamp+120,"environmentId":id,"requestNonce":claims["nonce"],"clientProofKeyThumbprint":thumbprint,"credential":grant["credential"]}))?;
    Ok(Json(
        json!({"credential":grant["credential"],"expiresAt":grant["expiresAt"],"proof":proof}),
    ))
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
            .map_err(|e| ApiError(StatusCode::UNAUTHORIZED, e.to_string()))?
    };
    Ok(upgrade
        .max_message_size(8 * 1024 * 1024)
        .on_upgrade(move |socket| serve_socket(s, session, socket)))
}

async fn serve_socket(s: Arc<Services>, session: Session, socket: WebSocket) {
    let (mut sender, mut receiver) = socket.split();
    let mut changes = s.changes.subscribe();
    let mut subscriptions = HashMap::<String, (String, Value)>::new();
    loop {
        tokio::select! {
            received=receiver.next()=>{
                let Some(Ok(Message::Text(text)))=received else{break};
                for line in text.lines(){
                    let Ok(request)=serde_json::from_str::<Value>(line) else{continue};
                    if request["_tag"]=="Ping"{let _=sender.send(Message::Text(json!({"_tag":"Pong"}).to_string().into())).await;continue;}
                    let id=request["id"].as_str().unwrap_or_default().to_owned();
                    if request["_tag"]=="Interrupt"{if let Some(id)=request["requestId"].as_str(){subscriptions.remove(id);}continue;}
                    if request["_tag"]!="Request"{continue;}
                    if !s.store.session_valid(session.id()).unwrap_or(false){return;}
                    let tag=request["tag"].as_str().unwrap_or_default();let input=&request["payload"];
                    let streaming=matches!(tag,"orchestration.subscribeShell"|"rust.subscribeThread");
                    let result=rpc(&s,&session,tag,input).await;
                    let response=match result{
                        Ok(value) if streaming=>{subscriptions.insert(id.clone(),(tag.into(),input.clone()));json!({"_tag":"Chunk","requestId":id,"values":[value]})},
                        Ok(value)=>json!({"_tag":"Exit","requestId":id,"exit":{"_tag":"Success","value":value}}),
                        Err(error)=>json!({"_tag":"Exit","requestId":id,"exit":{"_tag":"Failure","cause":{"_tag":"Fail","error":{"_tag":"RustMigrationError","message":error.to_string()}}}}),
                    };
                    if sender.send(Message::Text(response.to_string().into())).await.is_err(){return;}
                }
            }
            change=changes.recv()=>{
                if change.is_err(){break;}
                if !s.store.session_valid(session.id()).unwrap_or(false){break;}
                for(id,(tag,input))in &subscriptions{
                    if let Ok(value)=rpc(&s,&session,tag,input).await{if sender.send(Message::Text(json!({"_tag":"Chunk","requestId":id,"values":[value]}).to_string().into())).await.is_err(){return;}}
                }
            }
        }
    }
}
async fn rpc(s: &Arc<Services>, session: &Session, tag: &str, input: &Value) -> Result<Value> {
    let scope = match tag {
        "orchestration.dispatchCommand"
        | "projects.mutate"
        | "rust.cancel"
        | "rust.answerPermission" => "orchestration:operate",
        _ => "orchestration:read",
    };
    session.require(scope)?;
    match tag {
        "server.probe" => Ok(s.descriptor.clone()),
        "projects.list" => {
            Ok(json!({"projects":s.store.shell()?["projects"],"updatedAt":crate::store::now()}))
        }
        "orchestration.dispatchCommand" | "projects.mutate" => s.dispatch(input.clone()).await,
        "orchestration.subscribeShell" => {
            Ok(json!({"kind":"snapshot","snapshot":s.store.shell()?}))
        }
        "rust.getThread" | "rust.subscribeThread" => s.store.thread(required(input, "threadId")?),
        "rust.cancel" => {
            s.cancel(required(input, "threadId")?).await?;
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
