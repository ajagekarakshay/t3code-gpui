use crate::{
    http::Services,
    store::{now, required},
};
use anyhow::{Context, Result, ensure};
use serde_json::{Value, json};
use std::sync::Arc;
use t3_acp::{AgentEvent, Session, SessionCommand};

impl Services {
    pub(crate) async fn dispatch(self: &Arc<Self>, mut command: Value) -> Result<Value> {
        let kind = required(&command, "type")?.to_owned();
        if matches!(kind.as_str(), "project.create" | "project.update") {
            if let Some(root) = command["workspaceRoot"].as_str() {
                let root = tokio::fs::canonicalize(root)
                    .await
                    .context("Project directory does not exist")?;
                ensure!(root.is_dir(), "Project root must be a directory");
                command["workspaceRoot"] = json!(root);
            }
        }
        let result = self.store.dispatch(&command)?;
        let _ = self.changes.send(
            json!({"type":"changed","threadId":command["threadId"],"sequence":result["sequence"]}),
        );
        let services = self.clone();
        let task = tokio::spawn(async move {
            if let Err(error) = services.execute_pending().await {
                tracing::error!(%error,"Effect worker failed");
            }
        });
        self.tasks.lock().await.push(task);
        Ok(result)
    }

    async fn execute_pending(self: &Arc<Self>) -> Result<()> {
        let _serialized = self.effects.lock().await;
        while let Some((id, thread_id, command)) = self.store.claim_effect()? {
            if let Err(error) = self.deliver_prompt(&thread_id, &command).await {
                self.store.agent_event(
                    &thread_id,
                    &json!({"type":"acp.failure","message":error.to_string()}),
                    None,
                    true,
                    Some(&error.to_string()),
                )?;
                let _ = self
                    .changes
                    .send(json!({"type":"changed","threadId":thread_id}));
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
            self.tasks.lock().await.push(task);
            session
        };
        drop(sessions);
        session.prompt(required(command, "text")?.into()).await
    }

    async fn record_agent_event(&self, thread_id: &str, event: AgentEvent) -> Result<()> {
        let (event, text, finished, failure) = match event {
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
        self.store.agent_event(
            thread_id,
            &event,
            text.as_deref(),
            finished,
            failure.as_deref(),
        )?;
        let _ = self.changes.send(
            json!({"type":"changed","threadId":thread_id,"agentEvent":event,"updatedAt":now()}),
        );
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

    pub(crate) async fn answer_permission(
        &self,
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
        session.answer_permission(id, option).await
    }
}
