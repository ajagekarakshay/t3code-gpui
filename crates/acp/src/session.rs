use crate::{
    AgentCommand,
    protocol::{
        self, Agent, ConnectionTo,
        schema::{ProtocolVersion, v1::*},
    },
};
use anyhow::{Result, anyhow};
use serde_json::Value;
use std::{
    collections::{HashMap, VecDeque},
    path::PathBuf,
    sync::Arc,
};
use tokio::sync::{Mutex, mpsc, oneshot};

/// ACP updates are preserved in their native form, including tool calls and configuration.
#[derive(Debug)]
pub enum AgentEvent {
    Ready(Value),
    AuthenticationRequired(Value),
    Configuration(Value),
    Update(Value),
    Permission { id: String, request: Value },
    TurnFinished(Value),
    Failed(String),
}

pub enum SessionCommand {
    Prompt(Vec<ContentBlock>),
    Cancel,
    Mode(String),
    Config {
        id: String,
        value: SessionConfigOptionValue,
    },
    Authenticate(String),
    Shutdown,
}

type Permissions = Arc<Mutex<HashMap<String, oneshot::Sender<Option<String>>>>>;

/// One retained agent process per thread, with cancellation independent of prompt completion.
#[derive(Clone)]
pub struct Session {
    commands: mpsc::Sender<SessionCommand>,
    permissions: Permissions,
}

impl Session {
    pub fn start(
        command: AgentCommand,
        cwd: PathBuf,
        resume_id: Option<String>,
    ) -> (Self, mpsc::Receiver<AgentEvent>) {
        let (commands, receiver) = mpsc::channel(32);
        let (events, event_receiver) = mpsc::channel(256);
        let permissions: Permissions = Arc::default();
        let session = Self {
            commands,
            permissions: permissions.clone(),
        };
        tokio::spawn(async move {
            if let Err(error) = run(
                command,
                cwd,
                resume_id,
                receiver,
                events.clone(),
                permissions.clone(),
            )
            .await
            {
                let _ = events.send(AgentEvent::Failed(error.to_string())).await;
            }
            permissions.lock().await.clear();
        });
        (session, event_receiver)
    }

    pub async fn send(&self, command: SessionCommand) -> Result<()> {
        self.commands
            .send(command)
            .await
            .map_err(|_| anyhow!("Agent session closed"))
    }

    pub async fn prompt(&self, text: String) -> Result<()> {
        self.send(SessionCommand::Prompt(vec![ContentBlock::Text(
            TextContent::new(text),
        )]))
        .await
    }

    pub async fn answer_permission(&self, id: &str, option: Option<String>) -> Result<()> {
        let responder = self
            .permissions
            .lock()
            .await
            .remove(id)
            .ok_or_else(|| anyhow!("Permission request no longer pending"))?;
        responder
            .send(option)
            .map_err(|_| anyhow!("Permission request expired"))
    }
}

async fn run(
    command: AgentCommand,
    cwd: PathBuf,
    resume_id: Option<String>,
    mut commands: mpsc::Receiver<SessionCommand>,
    events: mpsc::Sender<AgentEvent>,
    permissions: Permissions,
) -> Result<()> {
    let updates = events.clone();
    let requests = events.clone();
    // IDs are local to this process; protocol requests retain their native option IDs.
    let pending_permissions = permissions.clone();
    let sequence = Arc::new(std::sync::atomic::AtomicU64::new(0));
    protocol::Client.builder()
        .on_receive_notification(async move |notification: SessionNotification, _| {
            let update = serde_json::to_value(notification).map_err(protocol::util::internal_error)?;
            updates.send(AgentEvent::Update(update)).await.map_err(protocol::util::internal_error)?;
            Ok(())
        }, protocol::on_receive_notification!())
        .on_receive_request(async move |request: RequestPermissionRequest, responder, _| {
            let id = sequence.fetch_add(1, std::sync::atomic::Ordering::Relaxed).to_string();
            let (sender, answer) = oneshot::channel();
            permissions.lock().await.insert(id.clone(), sender);
            let value = serde_json::to_value(&request).map_err(protocol::util::internal_error)?;
            if requests.send(AgentEvent::Permission { id: id.clone(), request: value }).await.is_err() {
                permissions.lock().await.remove(&id);
                return responder.respond(RequestPermissionResponse::new(RequestPermissionOutcome::Cancelled));
            }
            let selected = answer.await.ok().flatten().filter(|option| request.options.iter().any(|candidate| candidate.option_id.to_string() == *option));
            permissions.lock().await.remove(&id);
            let outcome = selected.map(|option| RequestPermissionOutcome::Selected(SelectedPermissionOutcome::new(option))).unwrap_or(RequestPermissionOutcome::Cancelled);
            responder.respond(RequestPermissionResponse::new(outcome))
        }, protocol::on_receive_request!())
        .connect_with(command.transport(), |connection: ConnectionTo<Agent>| async move {
            let initialized = connection.send_request(InitializeRequest::new(ProtocolVersion::V1)).block_task().await?;
            let authentication=serde_json::to_value(&initialized.auth_methods).map_err(protocol::util::internal_error)?;
            let mut deferred=VecDeque::new();
            let session_id=loop {
                let created=if let Some(resume)=&resume_id {
                    if !initialized.agent_capabilities.load_session {return Err(protocol::util::internal_error("Agent does not support session/load"));}
                    let id:SessionId=resume.clone().into();
                    connection.send_request(LoadSessionRequest::new(id.clone(),cwd.clone())).block_task().await
                        .and_then(|response|serde_json::to_value(response).map(|value|(id,value)).map_err(protocol::util::internal_error))
                } else {
                    connection.send_request(NewSessionRequest::new(cwd.clone())).block_task().await
                        .and_then(|response|serde_json::to_value(&response).map(|value|(response.session_id,value)).map_err(protocol::util::internal_error))
                };
                match created {
                    Ok((id,response))=>{
                        events.send(AgentEvent::Ready(serde_json::json!({"initialize":initialized,"session":response,"sessionId":id}))).await.map_err(protocol::util::internal_error)?;
                        break id;
                    }
                    Err(error) if error.code==protocol::schema::v1::ErrorCode::AuthRequired=>{
                        events.send(AgentEvent::AuthenticationRequired(authentication.clone())).await.map_err(protocol::util::internal_error)?;
                        loop {
                            match commands.recv().await {
                                Some(SessionCommand::Authenticate(method))=>{
                                    if !authentication.as_array().is_some_and(|methods|methods.iter().any(|candidate|candidate["id"]==method && candidate["type"]!="terminal")) {
                                        let _=events.send(AgentEvent::Failed("Unsupported authentication method".into())).await;continue;
                                    }
                                    match connection.send_request(AuthenticateRequest::new(method)).block_task().await {
                                        Ok(_)=>break,
                                        Err(error)=>{deferred.clear();let _=events.send(AgentEvent::Failed(error.to_string())).await;},
                                    }
                                }
                                Some(SessionCommand::Prompt(content)) if deferred.is_empty()=>deferred.push_back(SessionCommand::Prompt(content)),
                                Some(SessionCommand::Cancel)=>{deferred.clear();let _=events.send(AgentEvent::TurnFinished(serde_json::json!({"stopReason":"cancelled"}))).await;},
                                Some(SessionCommand::Shutdown)|None=>return Ok(()),
                                _=>{let _=events.send(AgentEvent::Failed("Authenticate before continuing".into())).await;},
                            }
                        }
                    }
                    Err(error)=>return Err(error),
                }
            };
            loop {
                let command=if let Some(command)=deferred.pop_front(){command}else{let Some(command)=commands.recv().await else{break};command};
                match command {
                    SessionCommand::Prompt(content) => {
                        let prompt = connection.send_request(PromptRequest::new(session_id.clone(), content)).block_task();
                        tokio::pin!(prompt);
                        loop {
                            tokio::select! {
                                result = &mut prompt => {
                                    pending_permissions.lock().await.clear();
                                    match result {
                                        Ok(response) => { let _ = events.send(AgentEvent::TurnFinished(serde_json::to_value(response).map_err(protocol::util::internal_error)?)).await; }
                                        Err(error) => { let _ = events.send(AgentEvent::Failed(error.to_string())).await; }
                                    }
                                    break;
                                }
                                command = commands.recv() => match command {
                                    Some(SessionCommand::Cancel) => { pending_permissions.lock().await.clear(); connection.send_notification(CancelNotification::new(session_id.clone()))?; }
                                    Some(SessionCommand::Shutdown) | None => { return Ok(()); }
                                    _ => { let _ = events.send(AgentEvent::Failed("A turn is already running".into())).await; }
                                }
                            }
                        }
                    }
                    SessionCommand::Cancel => { pending_permissions.lock().await.clear(); connection.send_notification(CancelNotification::new(session_id.clone()))?; }
                    SessionCommand::Mode(mode) => {
                        let response=connection.send_request(SetSessionModeRequest::new(session_id.clone(), mode.clone())).block_task().await?;
                        let _=events.send(AgentEvent::Configuration(serde_json::json!({"currentModeId":mode,"response":response}))).await;
                    }
                    SessionCommand::Config{id,value} => {
                        let response=connection.send_request(SetSessionConfigOptionRequest::new(session_id.clone(), id, value)).block_task().await?;
                        let _=events.send(AgentEvent::Configuration(serde_json::to_value(response).map_err(protocol::util::internal_error)?)).await;
                    }
                    SessionCommand::Authenticate(method) => { connection.send_request(AuthenticateRequest::new(method)).block_task().await?; }
                    SessionCommand::Shutdown => break,
                }
            }
            Ok(())
        }).await.map_err(|error| anyhow!(error.to_string()))
}
