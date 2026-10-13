use crate::{activity::ActivityKind, http::Services, store::required};
use anyhow::{Context, Result, ensure};
use serde_json::{Value, json};
use std::{future::Future, pin::Pin, sync::Arc};
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
        let mut claimed_pending = Vec::new();
        if kind == "message.dispatch" {
            let thread_id = required(&command, "threadId")?.to_owned();
            if command.get("attachments").is_none() {
                command["attachments"] = json!([]);
            }
            claimed_pending = command["attachments"]
                .as_array()
                .into_iter()
                .flatten()
                .filter_map(|attachment| attachment["id"].as_str())
                .filter(|id| id.starts_with("pending-"))
                .map(str::to_owned)
                .collect();
            self.attachments
                .claim(&thread_id, &mut command["attachments"])
                .await?;
        }
        let result = self.store.dispatch(&command)?;
        for attachment_id in claimed_pending {
            let _ = self.attachments.delete_pending(&attachment_id).await;
        }
        if kind == "provider.switch"
            && let Some(thread_id) = command["threadId"].as_str()
            && let Some(session) = self.sessions.lock().await.remove(thread_id)
        {
            let _ = session.send(SessionCommand::Shutdown).await;
        }
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
        self.schedule_effects().await;
        Ok(result)
    }

    pub(crate) async fn launch_thread(self: &Arc<Self>, input: &Value) -> Result<Value> {
        let command_id = required(input, "commandId")?;
        let thread_id = input["threadId"]
            .as_str()
            .map(str::to_owned)
            .unwrap_or_else(|| uuid::Uuid::new_v4().to_string());
        let reuse = input["reuseExistingThread"] == true;
        let workspace = &input["workspaceStrategy"];
        let mut created_worktree = None::<(String, String)>;
        let (branch, worktree_path) = if reuse {
            let existing = self.store.thread(&thread_id)?;
            (
                existing["thread"]["branch"].clone(),
                existing["thread"]["worktreePath"].clone(),
            )
        } else {
            match required(workspace, "type")? {
                "root" => (workspace["branch"].clone(), Value::Null),
                "existing_worktree" => {
                    let path = tokio::fs::canonicalize(required(workspace, "worktreePath")?)
                        .await
                        .context("Existing worktree is unavailable")?;
                    ensure!(path.is_dir(), "Existing worktree is not a directory");
                    (workspace["branch"].clone(), json!(path))
                }
                "worktree" => {
                    let shell = self.store.shell()?;
                    let project_id = required(input, "projectId")?;
                    let project = shell["projects"]
                        .as_array()
                        .context("Missing projects")?
                        .iter()
                        .find(|project| project["id"] == project_id)
                        .context("Project unavailable")?;
                    let root = required(project, "workspaceRoot")?;
                    let base = required(workspace, "baseRef")?;
                    let base = if workspace["startFromOrigin"] == true {
                        crate::vcs::fetch_ref(root, "origin", base).await?;
                        format!("origin/{base}")
                    } else {
                        base.to_owned()
                    };
                    let short_id = thread_id.chars().take(8).collect::<String>();
                    let branch = workspace["branch"]
                        .as_str()
                        .map(str::to_owned)
                        .unwrap_or_else(|| format!("t3code/{short_id}"));
                    let created = crate::vcs::create_worktree(
                        &self.home,
                        &json!({
                            "cwd":root,
                            "refName":base,
                            "newRefName":branch,
                            "baseRefName":base,
                            "path":null
                        }),
                    )
                    .await?;
                    let path = created["worktree"]["path"].clone();
                    created_worktree = path.as_str().map(|path| (root.to_owned(), path.to_owned()));
                    (created["worktree"]["refName"].clone(), path)
                }
                _ => anyhow::bail!("Unsupported workspace strategy"),
            }
        };
        if let Some(message) = input.get("initialMessage") {
            required(message, "text")?;
            ensure!(
                message["attachments"].as_array().is_some(),
                "Attachments must be an array"
            );
        }
        if reuse {
            // The lookup above also preserves the existing workspace binding.
        } else {
            if let Err(error) = self
                .dispatch(json!({
                    "type":"thread.create",
                    "commandId":format!("{command_id}:thread"),
                    "createdBy":"user",
                    "creationSource":input.get("creationSource").cloned().unwrap_or(json!("web")),
                    "threadId":thread_id,
                    "projectId":input["projectId"],
                    "title":input["title"],
                    "modelSelection":input["modelSelection"],
                    "runtimeMode":input["runtimeMode"],
                    "interactionMode":input["interactionMode"],
                    "branch":branch,
                    "worktreePath":worktree_path
                }))
                .await
            {
                if let Some((root, path)) = created_worktree.take() {
                    let _ =
                        crate::vcs::remove_worktree(&json!({"cwd":root,"path":path,"force":true}))
                            .await;
                }
                return Err(error);
            }
        }
        if let Some(message) = input.get("initialMessage")
            && let Err(error) = self
                .dispatch(json!({
                    "type":"message.dispatch",
                    "commandId":command_id,
                    "createdBy":"user",
                    "creationSource":input.get("creationSource").cloned().unwrap_or(json!("web")),
                    "threadId":thread_id,
                    "messageId":message.get("messageId").cloned().unwrap_or(json!(uuid::Uuid::new_v4().to_string())),
                    "text":message["text"],
                    "attachments":message["attachments"],
                    "dispatchMode":{"type":"start_immediately"}
                }))
                .await
        {
            if !reuse {
                let _ = self
                    .dispatch(json!({"type":"thread.delete","commandId":format!("{command_id}:rollback"),"threadId":thread_id}))
                    .await;
                if let Some((root, path)) = created_worktree {
                    let _ = crate::vcs::remove_worktree(
                        &json!({"cwd":root,"path":path,"force":true}),
                    )
                    .await;
                }
            }
            return Err(error);
        }
        Ok(json!({
            "threadId":thread_id,
            "projection":self.store.compatible_thread(&thread_id)?["projection"],
            "resumed":reuse
        }))
    }

    async fn execute_pending(self: &Arc<Self>) -> Result<()> {
        let _serialized = self.effects.lock().await;
        while let Some((id, thread_id, command)) = self.store.claim_effect()? {
            let effect = match command["type"].as_str() {
                Some("message.dispatch") => self.deliver_prompt(&thread_id, &command).await,
                Some("runtime-request.respond") => {
                    self.deliver_permission_response(&thread_id, &command).await
                }
                Some("run.interrupt") => self.cancel(&thread_id).await,
                _ => anyhow::bail!("Unsupported durable effect"),
            };
            if let Err(error) = effect {
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

    async fn schedule_effects(self: &Arc<Self>) {
        let effect = self.clone().execute_pending_boxed();
        let task = tokio::spawn(async move {
            if let Err(error) = effect.await {
                tracing::error!(%error, "Effect worker failed");
            }
        });
        let mut tasks = self.tasks.lock().await;
        tasks.retain(|task| !task.is_finished());
        tasks.push(task);
    }

    fn execute_pending_boxed(
        self: Arc<Self>,
    ) -> Pin<Box<dyn Future<Output = Result<()>> + Send + 'static>> {
        Box::pin(async move { self.execute_pending().await })
    }

    async fn deliver_permission_response(
        self: &Arc<Self>,
        thread_id: &str,
        command: &Value,
    ) -> Result<()> {
        let request_id = required(command, "requestId")?;
        let snapshot = self.store.thread(thread_id)?;
        let permission = &snapshot["thread"]["pendingAcpPermission"];
        ensure!(
            permission["id"] == request_id,
            "Permission request is no longer pending"
        );
        let desired_kind = match command["decision"].as_str() {
            Some("accept") => Some("allow_once"),
            Some("acceptForSession" | "acceptAlways") => Some("allow_always"),
            Some("decline") => Some("reject_once"),
            Some("cancel") => None,
            _ => anyhow::bail!("Unsupported approval decision"),
        };
        let option = desired_kind
            .and_then(|kind| {
                permission["request"]["options"]
                    .as_array()?
                    .iter()
                    .find(|option| option["kind"] == kind)
            })
            .and_then(|option| option["optionId"].as_str())
            .map(str::to_owned);
        ensure!(
            desired_kind.is_none() || option.is_some(),
            "The provider did not offer that approval choice"
        );
        self.answer_permission(thread_id, request_id, option)
            .await?;
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
        let empty_attachments = Value::Array(Vec::new());
        let content = self
            .attachments
            .prompt(
                required(command, "text")?.into(),
                command.get("attachments").unwrap_or(&empty_attachments),
            )
            .await?;
        session.send(SessionCommand::Prompt(content)).await
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
        let continued = finished || failure.is_some();
        let queued_run_started = continued && change["thread"]["activeRunId"].is_string();
        let _ = self.changes.send(change);
        let event_type = event["type"].as_str();
        if queued_run_started {
            self.queue_activity(thread_id, ActivityKind::Normal).await;
            self.schedule_effects().await;
        } else if continued {
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
    ) -> Result<Value> {
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
        let sequence = change["sequence"].clone();
        let _ = self.changes.send(change);
        self.queue_activity(thread_id, ActivityKind::Normal).await;
        Ok(json!({"sequence":sequence}))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{Server, ServerOptions};
    use tokio::process::Command;

    async fn git(directory: &std::path::Path, args: &[&str]) -> Result<()> {
        let status = Command::new("git")
            .args(args)
            .current_dir(directory)
            .status()
            .await?;
        ensure!(status.success(), "Test Git command failed");
        Ok(())
    }

    #[tokio::test]
    async fn launches_threads_in_new_git_worktrees() -> Result<()> {
        let repository = tempfile::tempdir()?;
        git(repository.path(), &["init", "-b", "main"]).await?;
        tokio::fs::write(repository.path().join("README.md"), "hello\n").await?;
        git(repository.path(), &["add", "."]).await?;
        git(
            repository.path(),
            &[
                "-c",
                "user.name=T3 Test",
                "-c",
                "user.email=test@t3.invalid",
                "commit",
                "-m",
                "initial",
            ],
        )
        .await?;

        let home = tempfile::tempdir()?;
        let server = Server::open(ServerOptions::new(home.path()))?;
        server
            .dispatch(json!({
                "type":"project.create",
                "commandId":"project-command",
                "projectId":"project",
                "title":"Repository",
                "workspaceRoot":repository.path(),
                "defaultModelSelection":{"instanceId":"codex","model":"default"}
            }))
            .await?;
        let launched = server
            .services
            .launch_thread(&json!({
                "commandId":"launch-command",
                "threadId":"thread",
                "projectId":"project",
                "title":"Worktree thread",
                "modelSelection":{"instanceId":"codex","model":"default"},
                "runtimeMode":"full-access",
                "interactionMode":"default",
                "workspaceStrategy":{"type":"worktree","baseRef":"main","branch":"feature/native"}
            }))
            .await?;
        let path = launched["projection"]["thread"]["worktreePath"]
            .as_str()
            .context("Missing worktree path")?;
        assert!(std::path::Path::new(path).is_dir());
        assert_eq!(launched["projection"]["thread"]["branch"], "feature/native");
        server.shutdown().await;
        Ok(())
    }
}
