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
    _home_lock: std::fs::File,
    pub(crate) store: Store,
    pub(crate) agents: BTreeMap<String, AgentCommand>,
    pub(crate) sessions: Mutex<HashMap<String, AgentSession>>,
    pub(crate) changes: broadcast::Sender<Value>,
    pub(crate) effects: Mutex<()>,
    pub(crate) tasks: Mutex<Vec<tokio::task::JoinHandle<()>>>,
    pub(crate) identity: EnvironmentIdentity,
    pub(crate) descriptor: Value,
    pub(crate) cloud: Mutex<crate::cloud::CloudState>,
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
        let descriptor = json!({"environmentId":id,"label":options.label,"platform":{"os":os,"arch":arch},"serverVersion":env!("CARGO_PKG_VERSION"),"capabilities":{"repositoryIdentity":false},"rustMigration":true});
        let (changes, _) = broadcast::channel(256);
        Ok(Self {
            services: Arc::new(Services {
                _home_lock: home_lock,
                store,
                agents: options.agents,
                sessions: Mutex::default(),
                changes,
                effects: Mutex::default(),
                tasks: Mutex::default(),
                identity,
                descriptor,
                cloud: Mutex::new(crate::cloud::CloudState::new(home)),
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
            .route("/api/connect/link-state", get(link_state))
            .route("/api/connect/unlink", post(unlink))
            .route("/api/connect/preferences", post(cloud_preferences))
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
        self.services.answer_permission(thread, id, option).await
    }
    pub async fn start_connect(&self, address: std::net::SocketAddr) -> Result<()> {
        self.services.start_connect(address).await
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
    reject_forwarded(&h)?;
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
async fn relay_health(
    State(s): State<Arc<Services>>,
    h: HeaderMap,
    Json(input): Json<Value>,
) -> ApiResult {
    reject_forwarded(&h)?;
    Ok(Json(s.relay_health(&input).await?))
}
async fn relay_mint(
    State(s): State<Arc<Services>>,
    h: HeaderMap,
    Json(input): Json<Value>,
) -> ApiResult {
    reject_forwarded(&h)?;
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
            .map_err(|e| ApiError(StatusCode::UNAUTHORIZED, e.to_string()))?
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
        Ok(value) => json!({"_tag":"Chunk","requestId":subscription.id,"values":[value]}),
        Err(error) => {
            let _ = send_rpc(sender, rpc_failure(&subscription.id, &error)).await;
            return false;
        }
    };
    subscription.awaiting_ack = true;
    subscription.dirty = false;
    send_rpc(sender, response).await
}
async fn serve_socket(s: Arc<Services>, session: Session, socket: WebSocket) {
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
                    let streaming=matches!(tag,"orchestration.subscribeShell"|"rust.subscribeThread");
                    let response=match rpc(&s,&session,tag,input).await {
                        Ok(value) if streaming=>{
                            subscriptions.insert(key,RpcSubscription{id:id.clone(),tag:tag.into(),input:input.clone(),awaiting_ack:true,dirty:false});
                            json!({"_tag":"Chunk","requestId":id,"values":[value]})
                        },
                        Ok(value)=>json!({"_tag":"Exit","requestId":id,"exit":{"_tag":"Success","value":value}}),
                        Err(error)=>rpc_failure(id,&error),
                    };
                    if !send_rpc(&mut sender,response).await{return;}
                }
            }
            change=changes.recv()=>{
                let Ok(change)=change else{break};
                if !s.store.session_valid(session.id()).unwrap_or(false){break;}
                for subscription in subscriptions.values_mut(){
                    let relevant=subscription.tag=="orchestration.subscribeShell" || change["threadId"].is_null() || change["threadId"]==subscription.input["threadId"];
                    if relevant {
                        subscription.dirty=true;
                        if !flush_subscription(&s,&session,subscription,&mut sender).await{return;}
                    }
                }
            }
        }
    }
}
async fn rpc(s: &Arc<Services>, session: &Session, tag: &str, input: &Value) -> Result<Value> {
    let scope = match tag {
        "rust.configureAgent" | "rust.authenticateAgent" => "providers:manage",
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
