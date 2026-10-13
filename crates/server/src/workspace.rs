use crate::{activity::ActivityKind, http::Services, store::required};
use anyhow::{Context, Result, ensure};
use serde_json::{Value, json};
use std::sync::Arc;
use t3_acp::{AgentEvent, Session, SessionCommand};

impl Services {
    pub(crate) async fn dispatch(self: &Arc<Self>, mut command: Value) -> Result<Value> {
        let kind = required(&command, "type")?.to_owned();
        if matches!(kind.as_str(), "project.create" | "project.update")
            && let Some(root) = command["workspaceRoot"].as_str()
        {
            let root = tokio::fs::canonicalize(root)
                .await
                .context("Project directory does not exist")?;
            ensure!(root.is_dir(), "Project root must be a directory");
            command["workspaceRoot"] = json!(root);
        }
        let result = self.store.dispatch(&command)?;
        let _ = self.changes.send(
            json!({"type":"changed","threadId":command["threadId"],"sequence":result["sequence"]}),
        );
        if let Some(thread_id) = command["threadId"].as_str() {
            let activity = match kind.as_str() {
                "thread.archive" | "thread.delete" => Some(ActivityKind::Tombstone),
                "message.dispatch" | "thread.unarchive" | "thread.metadata.update" => {
                    Some(ActivityKind::Normal)
                }
                _ => None,
            };
            if let Some(activity) = activity {
                self.queue_activity(thread_id, activity).await;
            }
        }
        let services = self.clone();
        let task = tokio::spawn(async move {
            if let Err(error) = services.execute_pending().await {
                tracing::error!(%error,"Effect worker failed");
            }
        });
        {
            let mut tasks = self.tasks.lock().await;
            tasks.retain(|task| !task.is_finished());
            tasks.push(task);
        }
        Ok(result)
    }

    async fn execute_pending(self: &Arc<Self>) -> Result<()> {
        let _serialized = self.effects.lock().await;
        while let Some((id, thread_id, command)) = self.store.claim_effect()? {
            if let Err(error) = self.deliver_prompt(&thread_id, &command).await {
                let change = self.store.agent_event(
                    &thread_id,
                    &json!({"type":"acp.failure","message":error.to_string()}),
                    None,
                    true,
                    Some(&error.to_string()),
                )?;
                let _ = self.changes.send(change);
                self.queue_activity(&thread_id, ActivityKind::Terminal)
                    .await;
            }
            self.store.complete_effect(id)?;
        }
        Ok(())
    }

    async fn deliver_prompt(self: &Arc<Self>, thread_id: &str, command: &Value) -> Result<()> {
        let snapshot = self.store.thread(thread_id)?;
        let thread = &snapshot["thread"];
        let provider = required(thread, "providerInstanceId")?;
        let mut sessions = self.sessions.lock().await;
        let session = if let Some(session) = sessions.get(thread_id) {
            session.clone()
        } else {
            let shell = self.store.shell()?;
            let project = shell["projects"]
                .as_array()
                .context("Missing projects")?
                .iter()
                .find(|project| project["id"] == thread["projectId"])
                .context("Project unavailable")?;
            let cwd = thread["worktreePath"]
                .as_str()
                .unwrap_or(required(project, "workspaceRoot")?);
            ensure!(
                std::path::Path::new(cwd).is_dir(),
                "Workspace directory is unavailable"
            );
            let agent = self
                .agents
                .get(provider)
                .context("Unsupported ACP agent")?
                .clone();
            let (session, mut events) = Session::start(
                agent,
                cwd.into(),
                thread["nativeSessionId"].as_str().map(str::to_owned),
            );
            sessions.insert(thread_id.to_owned(), session.clone());
            let services = self.clone();
            let thread_id = thread_id.to_owned();
            let task = tokio::spawn(async move {
                while let Some(event) = events.recv().await {
                    if let Err(error) = services.record_agent_event(&thread_id, event).await {
                        tracing::error!(%error,"Could not persist agent event");
                        break;
                    }
                }
                services.sessions.lock().await.remove(&thread_id);
            });
            {
                let mut tasks = self.tasks.lock().await;
                tasks.retain(|task| !task.is_finished());
                tasks.push(task);
            }
            session
        };
        drop(sessions);
        session.prompt(required(command, "text")?.into()).await
    }

    async fn record_agent_event(
        self: &Arc<Self>,
        thread_id: &str,
        event: AgentEvent,
    ) -> Result<()> {
        let (event, text, finished, failure) = match event {
            AgentEvent::AuthenticationRequired(methods) => (
                json!({"type":"acp.authentication.required","methods":methods}),
                None,
                false,
                None,
            ),
            AgentEvent::Configuration(data) => (
                json!({"type":"acp.configuration","data":data}),
                None,
                false,
                None,
            ),
            AgentEvent::Ready(data) => (json!({"type":"acp.ready","data":data}), None, false, None),
            AgentEvent::Update(data) => {
                let text = if data["update"]["sessionUpdate"] == "agent_message_chunk" {
                    data["update"]["content"]["text"]
                        .as_str()
                        .map(str::to_owned)
                } else {
                    None
                };
                (json!({"type":"acp.update","data":data}), text, false, None)
            }
            AgentEvent::Permission { id, request } => (
                json!({"type":"acp.permission","id":id,"request":request}),
                None,
                false,
                None,
            ),
            AgentEvent::TurnFinished(data) => (
                json!({"type":"acp.turn.finished","data":data}),
                None,
                true,
                None,
            ),
            AgentEvent::Failed(error) => (
                json!({"type":"acp.failure","message":error}),
                None,
                true,
                Some(error),
            ),
        };
        let change = self.store.agent_event(
            thread_id,
            &event,
            text.as_deref(),
            finished,
            failure.as_deref(),
        )?;
        let _ = self.changes.send(change);
        let event_type = event["type"].as_str();
        if finished || failure.is_some() {
            self.queue_activity(thread_id, ActivityKind::Terminal).await;
        } else if matches!(
            event_type,
            Some(
                "acp.ready"
                    | "acp.permission"
                    | "acp.authentication.required"
                    | "acp.permission.answered"
            )
        ) {
            self.queue_activity(thread_id, ActivityKind::Normal).await;
        }
        Ok(())
    }

    pub(crate) async fn cancel(&self, thread_id: &str) -> Result<()> {
        let session = self
            .sessions
            .lock()
            .await
            .get(thread_id)
            .cloned()
            .context("Agent session is not running")?;
        session.send(SessionCommand::Cancel).await
    }

    pub(crate) async fn configure_agent(
        &self,
        thread_id: &str,
        id: String,
        value: serde_json::Value,
    ) -> Result<()> {
        let thread = self.store.thread(thread_id)?;
        ensure!(
            thread["thread"]["activeRunId"].is_null(),
            "Wait for this turn to finish before changing agent configuration"
        );
        let value = if let Some(value) = value.as_bool() {
            t3_acp::protocol::schema::v1::SessionConfigOptionValue::boolean(value)
        } else {
            t3_acp::protocol::schema::v1::SessionConfigOptionValue::value_id(
                value
                    .as_str()
                    .context("Invalid configuration value")?
                    .to_owned(),
            )
        };
        let session = self
            .sessions
            .lock()
            .await
            .get(thread_id)
            .cloned()
            .context("Agent session is not running")?;
        session.send(SessionCommand::Config { id, value }).await
    }
    pub(crate) async fn authenticate_agent(&self, thread_id: &str, method: String) -> Result<()> {
        let session = self
            .sessions
            .lock()
            .await
            .get(thread_id)
            .cloned()
            .context("Agent session is not running")?;
        session.send(SessionCommand::Authenticate(method)).await
    }

    pub(crate) async fn answer_permission(
        self: &Arc<Self>,
        thread_id: &str,
        id: &str,
        option: Option<String>,
    ) -> Result<()> {
        let session = self
            .sessions
            .lock()
            .await
            .get(thread_id)
            .cloned()
            .context("Agent session is not running")?;
        session.answer_permission(id, option).await?;
        let change = self.store.agent_event(
            thread_id,
            &json!({"type":"acp.permission.answered","id":id}),
            None,
            false,
            None,
        )?;
        let _ = self.changes.send(change);
        self.queue_activity(thread_id, ActivityKind::Normal).await;
        Ok(())
    }
}
