use anyhow::{Context, Result, ensure};
use rusqlite::{Connection, OptionalExtension, Transaction, params};
use serde_json::{Value, json};
use std::{
    path::Path,
    sync::{Mutex, MutexGuard},
};

pub(crate) struct Store {
    connection: Mutex<Connection>,
}

pub(crate) fn now() -> String {
    chrono::Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Millis, true)
}
pub(crate) fn required<'a>(value: &'a Value, key: &str) -> Result<&'a str> {
    value[key]
        .as_str()
        .filter(|s| !s.trim().is_empty())
        .with_context(|| format!("Missing {key}"))
}

impl Store {
    pub(crate) fn open(path: &Path) -> Result<Self> {
        // Create private before SQLite opens it; its WAL and journal inherit this mode.
        let mut file = std::fs::OpenOptions::new();
        file.write(true).create(true).truncate(false);
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt;
            file.mode(0o600);
        }
        file.open(path)?;
        let mut connection = Connection::open(path)?;
        connection.busy_timeout(std::time::Duration::from_secs(5))?;
        connection.execute_batch("PRAGMA journal_mode=WAL; PRAGMA foreign_keys=ON;
            CREATE TABLE IF NOT EXISTS metadata (key TEXT PRIMARY KEY, value TEXT NOT NULL);
            CREATE TABLE IF NOT EXISTS projects (id TEXT PRIMARY KEY, value TEXT NOT NULL);
            CREATE TABLE IF NOT EXISTS threads (id TEXT PRIMARY KEY, value TEXT NOT NULL);
            CREATE TABLE IF NOT EXISTS messages (id TEXT PRIMARY KEY, thread_id TEXT NOT NULL REFERENCES threads(id), value TEXT NOT NULL);
            CREATE INDEX IF NOT EXISTS messages_thread ON messages(thread_id);
            CREATE TABLE IF NOT EXISTS events (sequence INTEGER PRIMARY KEY AUTOINCREMENT, thread_id TEXT, command_id TEXT, value TEXT NOT NULL);
            CREATE INDEX IF NOT EXISTS events_thread ON events(thread_id, sequence);
            CREATE TABLE IF NOT EXISTS receipts (id TEXT PRIMARY KEY, command TEXT NOT NULL, result TEXT NOT NULL);
            CREATE TABLE IF NOT EXISTS outbox (id INTEGER PRIMARY KEY AUTOINCREMENT, thread_id TEXT NOT NULL, value TEXT NOT NULL, status TEXT NOT NULL DEFAULT 'pending');
            CREATE TABLE IF NOT EXISTS grants (hash TEXT PRIMARY KEY, scopes TEXT NOT NULL, proof_key TEXT, expires INTEGER NOT NULL, reusable INTEGER NOT NULL DEFAULT 0);
            CREATE TABLE IF NOT EXISTS sessions (id TEXT PRIMARY KEY, hash TEXT UNIQUE NOT NULL, scopes TEXT NOT NULL, proof_key TEXT, expires INTEGER NOT NULL);
            CREATE TABLE IF NOT EXISTS tickets (hash TEXT PRIMARY KEY, session_id TEXT NOT NULL REFERENCES sessions(id) ON DELETE CASCADE, expires INTEGER NOT NULL);
            CREATE TABLE IF NOT EXISTS replay_guards (key TEXT PRIMARY KEY, expires INTEGER NOT NULL);")?;
        // A lost process cannot replay an accepted prompt safely. Retire it and expose failure.
        let tx = connection.transaction()?;
        let interrupted: Vec<(String, String)> = {
            let mut query = tx.prepare("SELECT id, value FROM threads WHERE json_extract(value, '$.status') IN ('running', 'starting', 'waiting')")?;
            query
                .query_map([], |row| Ok((row.get(0)?, row.get(1)?)))?
                .collect::<rusqlite::Result<_>>()?
        };
        for (id, value) in interrupted {
            let mut thread: Value = serde_json::from_str(&value)?;
            thread["status"] = json!("error");
            thread["lastError"] =
                json!("The server stopped during this turn. Send a new message to continue.");
            thread["activeRunId"] = Value::Null;
            thread["pendingAcpPermission"] = Value::Null;
            thread["pendingRuntimeRequest"] = Value::Null;
            thread["authenticationMethods"] = Value::Null;
            thread["updatedAt"] = json!(now());
            write_record(&tx, "threads", &id, &thread)?;
            append_event(
                &tx,
                Some(&id),
                None,
                &json!({"type":"rust.run.interrupted","thread":thread}),
            )?;
        }
        tx.execute(
            "UPDATE outbox SET status='retired' WHERE status IN ('running','pending')",
            [],
        )?;
        tx.commit()?;
        Ok(Self {
            connection: Mutex::new(connection),
        })
    }

    pub(crate) fn lock(&self) -> Result<MutexGuard<'_, Connection>> {
        self.connection
            .lock()
            .map_err(|_| anyhow::anyhow!("Database lock poisoned"))
    }

    pub(crate) fn metadata(&self, key: &str) -> Result<Option<String>> {
        Ok(self
            .lock()?
            .query_row("SELECT value FROM metadata WHERE key=?", [key], |row| {
                row.get(0)
            })
            .optional()?)
    }

    pub(crate) fn set_metadata(&self, key: &str, value: &str) -> Result<()> {
        self.lock()?.execute("INSERT INTO metadata(key,value) VALUES (?,?) ON CONFLICT(key) DO UPDATE SET value=excluded.value", params![key,value])?;
        Ok(())
    }

    pub(crate) fn set_metadata_batch(&self, values: &[(&str, String)]) -> Result<()> {
        let mut connection = self.lock()?;
        let tx = connection.transaction()?;
        for (key, value) in values {
            tx.execute("INSERT INTO metadata(key,value) VALUES (?,?) ON CONFLICT(key) DO UPDATE SET value=excluded.value",params![key,value])?;
        }
        tx.commit()?;
        Ok(())
    }

    pub(crate) fn remove_metadata(&self, keys: &[&str]) -> Result<()> {
        let mut connection = self.lock()?;
        let tx = connection.transaction()?;
        for key in keys {
            tx.execute("DELETE FROM metadata WHERE key=?", [key])?;
        }
        tx.commit()?;
        Ok(())
    }

    pub(crate) fn shell(&self) -> Result<Value> {
        let connection = self.lock()?;
        let projects = records(
            &connection,
            "SELECT value FROM projects WHERE json_extract(value,'$.deletedAt') IS NULL",
        )?;
        let all_threads = records(
            &connection,
            "SELECT value FROM threads WHERE json_extract(value,'$.deletedAt') IS NULL ORDER BY json_extract(value,'$.updatedAt') DESC",
        )?;
        let (threads, archived): (Vec<_>, Vec<_>) = all_threads
            .into_iter()
            .partition(|thread| thread["archivedAt"].is_null());
        let sequence: i64 =
            connection.query_row("SELECT COALESCE(MAX(sequence),0) FROM events", [], |row| {
                row.get(0)
            })?;
        Ok(
            json!({"schemaVersion":2,"snapshotSequence":sequence,"projects":projects,"threads":threads,"archivedThreads":archived}),
        )
    }

    pub(crate) fn compatible_shell(&self) -> Result<Value> {
        let mut shell = self.shell()?;
        if let Some(projects) = shell["projects"].as_array_mut() {
            for project in projects {
                project
                    .as_object_mut()
                    .map(|project| project.remove("deletedAt"));
            }
        }
        for key in ["threads", "archivedThreads"] {
            if let Some(threads) = shell[key].as_array_mut() {
                for thread in threads {
                    normalize_thread_status(thread);
                }
            }
        }
        Ok(shell)
    }

    pub(crate) fn compatible_thread(&self, id: &str) -> Result<Value> {
        let snapshot = self.thread(id)?;
        let mut thread = snapshot["thread"].clone();
        normalize_thread_status(&mut thread);
        let messages = snapshot["messages"]
            .as_array()
            .context("Missing messages")?;
        let mut conversation = Vec::new();
        let mut turn_items = Vec::new();
        let mut visible = Vec::new();
        let mut runtime_requests = Vec::new();
        for (ordinal, message) in messages.iter().enumerate() {
            let Some(role) = message["role"].as_str() else {
                continue;
            };
            let status = if role == "tool" {
                match message["toolCall"]["status"].as_str() {
                    Some("completed") => "completed",
                    Some("failed") => "failed",
                    Some("cancelled") => "cancelled",
                    Some("in_progress" | "running") => "running",
                    _ => "pending",
                }
            } else if message["streaming"] == true {
                "running"
            } else {
                "completed"
            };
            let mut base = json!({
                "id":required(message,"id")?,
                "threadId":id,
                "runId":message["runId"],
                "nodeId":message["nodeId"],
                "providerThreadId":null,
                "providerTurnId":null,
                "nativeItemRef":null,
                "parentItemId":null,
                "ordinal":ordinal,
                "status":status,
                "title":if role=="tool" {message["toolCall"]["title"].clone()} else {Value::Null},
                "startedAt":message["createdAt"],
                "completedAt":if status=="completed" {message["updatedAt"].clone()} else {Value::Null},
                "updatedAt":message["updatedAt"]
            });
            match role {
                "user" => {
                    base["type"] = json!("user_message");
                    base["messageId"] = message["id"].clone();
                    base["inputIntent"] = json!("turn_start");
                    base["text"] = message["text"].clone();
                    base["attachments"] = message["attachments"].clone();
                    base["createdBy"] = message["createdBy"].clone();
                    base["creationSource"] = message["creationSource"].clone();
                }
                "assistant" => {
                    base["type"] = json!("assistant_message");
                    base["messageId"] = message["id"].clone();
                    base["text"] = message["text"].clone();
                    base["attachments"] = message["attachments"].clone();
                    base["streaming"] = message["streaming"].clone();
                }
                "system" => {
                    base["type"] = json!("system_notice");
                    base["message"] = message["text"].clone();
                }
                "tool" => {
                    base["type"] = json!("dynamic_tool");
                    base["toolName"] = message["toolCall"]
                        .get("kind")
                        .cloned()
                        .unwrap_or(Value::Null);
                    base["input"] = message["toolCall"]
                        .get("rawInput")
                        .cloned()
                        .unwrap_or_else(|| json!({}));
                    if let Some(output) = message["toolCall"].get("rawOutput") {
                        base["output"] = output.clone();
                    }
                }
                _ => continue,
            }
            if matches!(role, "user" | "assistant" | "system") {
                let mut row = message.clone();
                if row["nodeId"].is_null() {
                    row["nodeId"] = Value::Null;
                }
                conversation.push(row);
            }
            visible.push(json!({
                "position":ordinal,
                "visibility":"local",
                "sourceThreadId":id,
                "sourceItemId":base["id"],
                "item":base
            }));
            turn_items.push(base);
        }
        if let Some(permission) = thread
            .get("pendingAcpPermission")
            .filter(|permission| !permission.is_null())
        {
            let request_id = required(permission, "id")?;
            let request = &permission["request"];
            let request_kind = permission_request_kind(request);
            let node_id = format!("node:{}", thread["latestRunId"].as_str().unwrap_or(id));
            let provider_session_id = thread["nativeSessionId"]
                .as_str()
                .map(str::to_owned)
                .unwrap_or_else(|| format!("session:{id}"));
            runtime_requests.push(json!({
                "id":request_id,
                "nodeId":node_id,
                "providerTurnId":null,
                "nativeRequestRef":null,
                "kind":request_kind,
                "status":"pending",
                "responseCapability":{"type":"live","providerSessionId":provider_session_id},
                "createdAt":thread["updatedAt"],
                "resolvedAt":null
            }));
            let options = request["options"]
                .as_array()
                .into_iter()
                .flatten()
                .filter_map(|option| {
                    let decision = match option["kind"].as_str()? {
                        "allow_once" => "accept",
                        "allow_always" => "acceptForSession",
                        "reject_once" | "reject_always" => "decline",
                        _ => return None,
                    };
                    Some(json!({
                        "decision":decision,
                        "label":option["name"].as_str().unwrap_or(decision)
                    }))
                })
                .collect::<Vec<_>>();
            let ordinal = turn_items.len();
            let mut item = json!({
                "id":format!("approval:{request_id}"),
                "threadId":id,
                "runId":thread["latestRunId"],
                "nodeId":node_id,
                "providerThreadId":null,
                "providerTurnId":null,
                "nativeItemRef":null,
                "parentItemId":null,
                "ordinal":ordinal,
                "status":"waiting",
                "title":request["toolCall"]["title"],
                "startedAt":thread["updatedAt"],
                "completedAt":null,
                "updatedAt":thread["updatedAt"],
                "type":"approval_request",
                "requestId":request_id,
                "requestKind":request_kind,
                "options":options
            });
            if let Some(prompt) = request["toolCall"]["title"].as_str() {
                item["prompt"] = json!(prompt);
            }
            visible.push(json!({
                "position":ordinal,
                "visibility":"local",
                "sourceThreadId":id,
                "sourceItemId":item["id"],
                "item":item
            }));
            turn_items.push(item);
        }
        let projection = json!({
            "thread":thread,
            "runs":[],
            "attempts":[],
            "nodes":[],
            "subagents":[],
            "providerSessions":[],
            "providerThreads":[],
            "providerTurns":[],
            "runtimeRequests":runtime_requests,
            "messages":conversation,
            "plans":[],
            "turnItems":turn_items,
            "checkpointScopes":[],
            "checkpoints":[],
            "contextHandoffs":[],
            "contextTransfers":[],
            "visibleTurnItems":visible,
            "updatedAt":snapshot["thread"]["updatedAt"]
        });
        Ok(json!({"snapshotSequence":snapshot["snapshotSequence"],"projection":projection}))
    }

    pub(crate) fn thread(&self, id: &str) -> Result<Value> {
        let connection = self.lock()?;
        let thread = read_record(&connection, "threads", id)?;
        let mut query =
            connection.prepare("SELECT value FROM messages WHERE thread_id=? ORDER BY rowid")?;
        let messages: Vec<String> = query
            .query_map([id], |row| row.get(0))?
            .collect::<rusqlite::Result<_>>()?;
        let messages: Vec<Value> = messages
            .iter()
            .map(|v| serde_json::from_str(v))
            .collect::<serde_json::Result<_>>()?;
        let sequence: i64 =
            connection.query_row("SELECT COALESCE(MAX(sequence),0) FROM events", [], |row| {
                row.get(0)
            })?;
        Ok(json!({"snapshotSequence":sequence,"thread":thread,"messages":messages}))
    }

    /// Replay committed changes in bounded pages; the cursor advances only after delivery.
    pub(crate) fn changes_after(&self, after: i64) -> Result<Value> {
        ensure!(after >= 0, "Invalid event cursor");
        let connection = self.lock()?;
        let latest: i64 =
            connection.query_row("SELECT COALESCE(MAX(sequence),0) FROM events", [], |row| {
                row.get(0)
            })?;
        ensure!(after <= latest, "Event cursor is ahead of this environment");
        let mut query = connection.prepare("SELECT sequence,thread_id,value FROM events WHERE sequence>? ORDER BY sequence LIMIT 256")?;
        let rows = query.query_map([after], |row| {
            Ok((
                row.get::<_, i64>(0)?,
                row.get::<_, Option<String>>(1)?,
                row.get::<_, String>(2)?,
            ))
        })?;
        let mut changes = Vec::new();
        let mut bytes = 0;
        let mut cursor = after;
        for row in rows {
            let (sequence, thread, value) = row?;
            if !changes.is_empty() && bytes + value.len() > 512 * 1024 {
                break;
            }
            bytes += value.len();
            let event: Value = serde_json::from_str(&value)?;
            let mut change = if event["type"] == "changed" {
                event
            } else {
                // Older ACP rows predate replayable projections. Consumers refresh snapshots.
                json!({"type":"changed","event":event})
            };
            change["sequence"] = json!(sequence);
            change["threadId"] = json!(thread);
            changes.push(change);
            cursor = sequence;
        }
        Ok(json!({"changes":changes,"cursor":cursor,"hasMore":cursor < latest}))
    }

    pub(crate) fn dispatch(&self, command: &Value) -> Result<Value> {
        let command_id = required(command, "commandId")?;
        let kind = required(command, "type")?;
        let mut connection = self.lock()?;
        let tx = connection.transaction()?;
        let existing: Option<(String, String)> = tx
            .query_row(
                "SELECT command,result FROM receipts WHERE id=?",
                [command_id],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .optional()?;
        if let Some((original, result)) = existing {
            ensure!(
                serde_json::from_str::<Value>(&original)? == *command,
                "Command ID reused with different input"
            );
            return Ok(serde_json::from_str(&result)?);
        }
        let timestamp = now();
        let event = match kind {
            "project.create" => {
                let id = required(command, "projectId")?;
                ensure!(!exists(&tx, "projects", id)?, "Project already exists");
                let project = json!({"id":id,"title":required(command,"title")?,"workspaceRoot":required(command,"workspaceRoot")?,"defaultModelSelection":command["defaultModelSelection"],"scripts":command.get("scripts").cloned().unwrap_or(json!([])),"createdAt":timestamp,"updatedAt":timestamp,"deletedAt":null});
                write_record(&tx, "projects", id, &project)?;
                json!({"type":"project.upserted","project":project})
            }
            "project.update" | "project.delete" => {
                let id = required(command, "projectId")?;
                let mut project = read_record(&tx, "projects", id)?;
                if kind == "project.delete" {
                    let active: i64 = tx.query_row("SELECT COUNT(*) FROM threads WHERE json_extract(value,'$.projectId')=? AND json_extract(value,'$.deletedAt') IS NULL", [id], |r|r.get(0))?;
                    ensure!(active == 0, "Remove the project's threads first");
                    project["deletedAt"] = json!(timestamp);
                } else {
                    for key in [
                        "title",
                        "workspaceRoot",
                        "defaultModelSelection",
                        "scripts",
                        "autoPull",
                        "defaultThreadEnvMode",
                    ] {
                        if let Some(value) = command.get(key) {
                            project[key] = value.clone();
                        }
                    }
                }
                project["updatedAt"] = json!(timestamp);
                write_record(&tx, "projects", id, &project)?;
                json!({"type":"project.upserted","project":project})
            }
            "thread.create" => {
                let id = required(command, "threadId")?;
                let project_id = required(command, "projectId")?;
                let project = read_record(&tx, "projects", project_id)?;
                ensure!(project["deletedAt"].is_null(), "Project was deleted");
                ensure!(!exists(&tx, "threads", id)?, "Thread already exists");
                let provider = required(&command["modelSelection"], "instanceId")?;
                ensure!(
                    matches!(provider, "codex" | "claude-code"),
                    "Unsupported provider"
                );
                required(&command["modelSelection"], "model")?;
                let runtime_mode = command["runtimeMode"].as_str().unwrap_or("full-access");
                ensure!(
                    matches!(
                        runtime_mode,
                        "approval-required" | "auto-accept-edits" | "auto" | "full-access"
                    ),
                    "Unsupported runtime mode"
                );
                let interaction_mode = command["interactionMode"].as_str().unwrap_or("default");
                ensure!(
                    matches!(interaction_mode, "default" | "plan"),
                    "Unsupported interaction mode"
                );
                let thread = json!({"id":id,"projectId":project_id,"title":required(command,"title")?,"modelSelection":command["modelSelection"],"providerInstanceId":provider,"runtimeMode":command.get("runtimeMode").cloned().unwrap_or(json!("full-access")),"interactionMode":command.get("interactionMode").cloned().unwrap_or(json!("default")),"branch":command["branch"],"worktreePath":command["worktreePath"],"createdBy":command.get("createdBy").cloned().unwrap_or(json!("user")),"creationSource":command.get("creationSource").cloned().unwrap_or(json!("server")),"lineage":{"parentThreadId":null,"relationshipToParent":null,"rootThreadId":id},"forkedFrom":null,"activeProviderThreadId":null,"latestRunId":null,"activeRunId":null,"status":"idle","pendingRuntimeRequest":null,"latestVisibleMessage":null,"latestUserMessageAt":null,"hasActionableProposedPlan":false,"itemCount":0,"visibleItemCount":0,"createdAt":timestamp,"updatedAt":timestamp,"archivedAt":null,"settledOverride":null,"settledAt":null,"deletedAt":null});
                write_record(&tx, "threads", id, &thread)?;
                json!({"type":"thread.updated","thread":thread})
            }
            "message.dispatch" => {
                let id = required(command, "threadId")?;
                let mut thread = read_record(&tx, "threads", id)?;
                ensure!(
                    thread["deletedAt"].is_null() && thread["archivedAt"].is_null(),
                    "Thread is unavailable"
                );
                ensure!(
                    !matches!(
                        thread["status"].as_str(),
                        Some("starting" | "running" | "waiting")
                    ),
                    "A turn is already running"
                );
                ensure!(
                    command
                        .get("attachments")
                        .and_then(Value::as_array)
                        .is_none_or(Vec::is_empty),
                    "Attachments are not ported yet"
                );
                let text = required(command, "text")?;
                let message_id = required(command, "messageId")?;
                let run_id = uuid::Uuid::new_v4().to_string();
                let message = json!({"id":message_id,"threadId":id,"runId":run_id,"nodeId":null,"role":"user","text":text,"attachments":[],"streaming":false,"createdAt":timestamp,"updatedAt":timestamp,"createdBy":thread["createdBy"],"creationSource":thread["creationSource"]});
                ensure!(
                    !exists(&tx, "messages", message_id)?,
                    "Message ID already exists"
                );
                tx.execute(
                    "INSERT INTO messages(id,thread_id,value) VALUES (?,?,?)",
                    params![message_id, id, message.to_string()],
                )?;
                thread["activeRunId"] = json!(run_id);
                thread["latestRunId"] = json!(run_id);
                thread["status"] = json!("starting");
                thread["lastError"] = Value::Null;
                thread["updatedAt"] = json!(timestamp);
                thread["latestUserMessageAt"] = json!(timestamp);
                thread["latestVisibleMessage"] =
                    json!({"id":message_id,"role":"user","text":text,"updatedAt":timestamp});
                thread["itemCount"] = json!(thread["itemCount"].as_u64().unwrap_or(0) + 1);
                thread["visibleItemCount"] =
                    json!(thread["visibleItemCount"].as_u64().unwrap_or(0) + 1);
                write_record(&tx, "threads", id, &thread)?;
                tx.execute(
                    "INSERT INTO outbox(thread_id,value) VALUES (?,?)",
                    params![id, command.to_string()],
                )?;
                json!({"type":"rust.message.dispatched","message":message,"thread":thread})
            }
            "runtime-request.respond" => {
                let id = required(command, "threadId")?;
                let thread = read_record(&tx, "threads", id)?;
                let permission = &thread["pendingAcpPermission"];
                ensure!(
                    permission["id"] == required(command, "requestId")?,
                    "Permission request is no longer pending"
                );
                ensure!(
                    command["answers"].is_null(),
                    "User input answers are not ported yet"
                );
                ensure!(
                    command["attachmentsByQuestionId"].is_null(),
                    "User input attachments are not ported yet"
                );
                let desired_kind = match required(command, "decision")? {
                    "accept" => Some("allow_once"),
                    "acceptForSession" | "acceptAlways" => Some("allow_always"),
                    "decline" => Some("reject_once"),
                    "cancel" => None,
                    _ => anyhow::bail!("Unsupported approval decision"),
                };
                ensure!(
                    desired_kind.is_none()
                        || permission["request"]["options"]
                            .as_array()
                            .is_some_and(|options| options
                                .iter()
                                .any(|option| { option["kind"].as_str() == desired_kind })),
                    "The provider did not offer that approval choice"
                );
                tx.execute(
                    "INSERT INTO outbox(thread_id,value) VALUES (?,?)",
                    params![id, command.to_string()],
                )?;
                json!({"type":"rust.runtime-request.response-requested","threadId":id,"requestId":permission["id"]})
            }
            "run.interrupt" => {
                let id = required(command, "threadId")?;
                let thread = read_record(&tx, "threads", id)?;
                ensure!(
                    thread["activeRunId"] == required(command, "runId")?,
                    "Run is no longer active"
                );
                ensure!(
                    matches!(
                        thread["status"].as_str(),
                        Some("starting" | "running" | "waiting")
                    ),
                    "Run is not interruptible"
                );
                tx.execute(
                    "INSERT INTO outbox(thread_id,value) VALUES (?,?)",
                    params![id, command.to_string()],
                )?;
                json!({"type":"rust.run.interrupt-requested","threadId":id,"runId":thread["activeRunId"],"reason":command["reason"]})
            }
            "thread.archive"
            | "thread.unarchive"
            | "thread.delete"
            | "thread.settle"
            | "thread.unsettle"
            | "thread.pin"
            | "thread.unpin"
            | "thread.visit"
            | "thread.metadata.update"
            | "thread.runtime-mode.set"
            | "thread.interaction-mode.set"
            | "thread.model-selection.set"
            | "provider.switch"
            | "thread.snooze"
            | "thread.unsnooze" => {
                let id = required(command, "threadId")?;
                let mut thread = read_record(&tx, "threads", id)?;
                ensure!(thread["deletedAt"].is_null(), "Thread was deleted");
                if matches!(kind, "thread.archive" | "thread.delete" | "thread.settle") {
                    ensure!(
                        !matches!(
                            thread["status"].as_str(),
                            Some("starting" | "running" | "waiting")
                        ),
                        "Stop the running turn first"
                    );
                }
                match kind {
                    "thread.archive" => thread["archivedAt"] = json!(timestamp),
                    "thread.unarchive" => thread["archivedAt"] = Value::Null,
                    "thread.delete" => thread["deletedAt"] = json!(timestamp),
                    "thread.settle" => {
                        thread["settledAt"] = json!(timestamp);
                        thread["settledOverride"] = json!("settled");
                    }
                    "thread.unsettle" => {
                        thread["settledAt"] = Value::Null;
                        thread["settledOverride"] = json!("active");
                    }
                    "thread.pin" => thread["pinnedAt"] = json!(timestamp),
                    "thread.unpin" => thread["pinnedAt"] = Value::Null,
                    "thread.visit" => thread["lastVisitedAt"] = json!(timestamp),
                    "thread.snooze" => thread["snoozedUntil"] = command["until"].clone(),
                    "thread.unsnooze" => thread["snoozedUntil"] = Value::Null,
                    "thread.metadata.update" => {
                        for key in ["title", "branch", "worktreePath", "linkedPullRequest"] {
                            if let Some(value) = command.get(key) {
                                thread[key] = value.clone();
                            }
                        }
                    }
                    "thread.runtime-mode.set" => {
                        ensure!(
                            matches!(
                                required(command, "runtimeMode")?,
                                "approval-required" | "auto-accept-edits" | "auto" | "full-access"
                            ),
                            "Unsupported runtime mode"
                        );
                        thread["runtimeMode"] = command["runtimeMode"].clone()
                    }
                    "thread.interaction-mode.set" => {
                        ensure!(
                            matches!(required(command, "interactionMode")?, "default" | "plan"),
                            "Unsupported interaction mode"
                        );
                        thread["interactionMode"] = command["interactionMode"].clone()
                    }
                    "thread.model-selection.set" | "provider.switch" => {
                        ensure!(
                            !matches!(
                                thread["status"].as_str(),
                                Some("starting" | "running" | "waiting")
                            ),
                            "Stop the running turn before changing providers"
                        );
                        let provider = required(&command["modelSelection"], "instanceId")?;
                        ensure!(
                            matches!(provider, "codex" | "claude-code"),
                            "Unsupported provider"
                        );
                        thread["modelSelection"] = command["modelSelection"].clone();
                        thread["providerInstanceId"] = json!(provider);
                    }
                    _ => unreachable!(),
                }
                thread["updatedAt"] = json!(timestamp);
                write_record(&tx, "threads", id, &thread)?;
                json!({"type":"thread.updated","thread":thread})
            }
            _ => anyhow::bail!("Command is not ported yet: {kind}"),
        };
        let sequence = append_event(&tx, command["threadId"].as_str(), Some(command_id), &event)?;
        let result = json!({"commandId":command_id,"acceptedAt":timestamp,"sequence":sequence});
        tx.execute(
            "INSERT INTO receipts(id,command,result) VALUES (?,?,?)",
            params![command_id, command.to_string(), result.to_string()],
        )?;
        tx.commit()?;
        Ok(result)
    }

    pub(crate) fn claim_effect(&self) -> Result<Option<(i64, String, Value)>> {
        let mut connection = self.lock()?;
        let tx = connection.transaction()?;
        let effect: Option<(i64, String, String)> = tx
            .query_row(
                "SELECT id,thread_id,value FROM outbox WHERE status='pending' ORDER BY id LIMIT 1",
                [],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
            )
            .optional()?;
        if let Some((id, thread, command)) = effect {
            tx.execute("UPDATE outbox SET status='running' WHERE id=?", [id])?;
            tx.commit()?;
            Ok(Some((id, thread, serde_json::from_str(&command)?)))
        } else {
            Ok(None)
        }
    }

    pub(crate) fn complete_effect(&self, id: i64) -> Result<()> {
        self.lock()?
            .execute("UPDATE outbox SET status='complete' WHERE id=?", [id])?;
        Ok(())
    }

    pub(crate) fn agent_event(
        &self,
        thread_id: &str,
        event: &Value,
        text: Option<&str>,
        finished: bool,
        failure: Option<&str>,
    ) -> Result<Value> {
        let mut connection = self.lock()?;
        let tx = connection.transaction()?;
        let mut thread = read_record(&tx, "threads", thread_id)?;
        let timestamp = now();
        let mut inserted_item = false;
        if let Some(text) = text {
            let message_id = format!("assistant:{}", required(&thread, "latestRunId")?);
            let existing: Option<String> = tx
                .query_row(
                    "SELECT value FROM messages WHERE id=?",
                    [&message_id],
                    |row| row.get(0),
                )
                .optional()?;
            inserted_item |= existing.is_none();
            let mut message = if let Some(value) = existing {
                serde_json::from_str::<Value>(&value)?
            } else {
                json!({"id":message_id,"threadId":thread_id,"runId":thread["latestRunId"],"nodeId":null,"role":"assistant","text":"","attachments":[],"streaming":true,"createdAt":timestamp,"updatedAt":timestamp,"createdBy":"agent","creationSource":"provider"})
            };
            message["text"] = json!(format!(
                "{}{text}",
                message["text"].as_str().unwrap_or_default()
            ));
            message["updatedAt"] = json!(timestamp);
            thread["latestVisibleMessage"] = json!({"id":message_id,"role":"assistant","text":message["text"].as_str().unwrap_or_default().chars().take(400).collect::<String>(),"updatedAt":timestamp});
            tx.execute("INSERT INTO messages(id,thread_id,value) VALUES (?,?,?) ON CONFLICT(id) DO UPDATE SET value=excluded.value",params![message_id,thread_id,message.to_string()])?;
        }
        let update = &event["data"]["update"];
        if matches!(
            update["sessionUpdate"].as_str(),
            Some("tool_call" | "tool_call_update")
        ) {
            let tool_id = required(update, "toolCallId")?;
            let message_id = format!("tool:{}:{tool_id}", required(&thread, "latestRunId")?);
            let existing: Option<String> = tx
                .query_row(
                    "SELECT value FROM messages WHERE id=?",
                    [&message_id],
                    |row| row.get(0),
                )
                .optional()?;
            inserted_item |= existing.is_none();
            let mut message = existing.map(|value|serde_json::from_str::<Value>(&value)).transpose()?.unwrap_or(json!({"id":message_id,"threadId":thread_id,"runId":thread["latestRunId"],"role":"tool","text":"","toolCall":{},"createdAt":timestamp,"updatedAt":timestamp}));
            if let Some(fields) = update.as_object() {
                for (key, value) in fields {
                    if key != "sessionUpdate" {
                        message["toolCall"][key] = value.clone();
                    }
                }
            }
            message["text"] = message["toolCall"]["title"].clone();
            message["updatedAt"] = json!(timestamp);
            tx.execute("INSERT INTO messages(id,thread_id,value) VALUES (?,?,?) ON CONFLICT(id) DO UPDATE SET value=excluded.value",params![message_id,thread_id,message.to_string()])?;
        }
        thread["status"] = json!(if failure.is_some() {
            "error"
        } else if finished {
            "idle"
        } else if matches!(
            event["type"].as_str(),
            Some("acp.permission" | "acp.authentication.required")
        ) {
            "waiting"
        } else if thread["activeRunId"].is_null() {
            thread["status"].as_str().unwrap_or("idle")
        } else {
            "running"
        });
        if finished || failure.is_some() {
            thread["activeRunId"] = Value::Null;
            tx.execute("UPDATE messages SET value=json_set(value,'$.streaming',json('false')) WHERE thread_id=?",[thread_id])?;
        }
        if let Some(failure) = failure {
            thread["lastError"] = json!(failure);
        }
        if event["type"] == "acp.ready" {
            thread["nativeSessionId"] = event["data"]["sessionId"].clone();
            thread["agentConfiguration"] = event["data"].clone();
        }
        if event["type"] == "acp.authentication.required" {
            thread["authenticationMethods"] = event["methods"].clone();
        }
        if event["type"] == "acp.ready" {
            thread["authenticationMethods"] = Value::Null;
        }
        if event["type"] == "acp.configuration" {
            if let Some(options) = event["data"].get("configOptions") {
                thread["agentConfiguration"]["session"]["configOptions"] = options.clone();
            }
            if let Some(mode) = event["data"].get("currentModeId") {
                thread["agentConfiguration"]["session"]["modes"]["currentModeId"] = mode.clone();
            }
        }
        if event["type"] == "acp.permission" {
            thread["pendingAcpPermission"] = event.clone();
        }
        if finished || event["type"] == "acp.permission.answered" {
            thread["pendingAcpPermission"] = Value::Null;
        }
        if update["sessionUpdate"] == "config_option_update" {
            thread["agentConfiguration"]["session"]["configOptions"] =
                update["configOptions"].clone();
        }
        if update["sessionUpdate"] == "current_mode_update" {
            thread["agentConfiguration"]["session"]["modes"]["currentModeId"] =
                update["currentModeId"].clone();
        }
        if inserted_item {
            thread["itemCount"] = json!(thread["itemCount"].as_u64().unwrap_or(0) + 1);
            thread["visibleItemCount"] =
                json!(thread["visibleItemCount"].as_u64().unwrap_or(0) + 1);
        }
        thread["updatedAt"] = json!(timestamp);
        write_record(&tx, "threads", thread_id, &thread)?;
        let mut change =
            json!({"type":"changed","threadId":thread_id,"thread":thread,"agentEvent":event});
        let sequence = append_event(&tx, Some(thread_id), None, &change)?;
        tx.commit()?;
        change["sequence"] = json!(sequence);
        Ok(change)
    }
}

fn normalize_thread_status(thread: &mut Value) {
    if thread["status"] == "error" {
        thread["status"] = json!("failed");
    }
    if let Some(permission) = thread
        .get("pendingAcpPermission")
        .filter(|permission| !permission.is_null())
    {
        thread["pendingRuntimeRequest"] = json!({
            "id":permission["id"],
            "kind":permission_request_kind(&permission["request"]),
            "createdAt":thread["updatedAt"]
        });
        thread["itemCount"] = json!(thread["itemCount"].as_u64().unwrap_or(0) + 1);
        thread["visibleItemCount"] = json!(thread["visibleItemCount"].as_u64().unwrap_or(0) + 1);
    }
}

fn permission_request_kind(request: &Value) -> &'static str {
    match request["toolCall"]["kind"].as_str() {
        Some("execute") => "command",
        Some("edit") => "file-change",
        Some("read") => "file-read",
        _ => "permission",
    }
}

fn exists(connection: &Connection, table: &str, id: &str) -> Result<bool> {
    Ok(connection.query_row(
        &format!("SELECT EXISTS(SELECT 1 FROM {table} WHERE id=?)"),
        [id],
        |row| row.get(0),
    )?)
}
fn read_record(connection: &Connection, table: &str, id: &str) -> Result<Value> {
    let value: String = connection
        .query_row(
            &format!("SELECT value FROM {table} WHERE id=?"),
            [id],
            |row| row.get(0),
        )
        .with_context(|| format!("Unknown {table} ID"))?;
    Ok(serde_json::from_str(&value)?)
}
fn write_record(tx: &Transaction<'_>, table: &str, id: &str, value: &Value) -> Result<()> {
    tx.execute(&format!("INSERT INTO {table}(id,value) VALUES (?,?) ON CONFLICT(id) DO UPDATE SET value=excluded.value"),params![id,value.to_string()])?;
    Ok(())
}
fn append_event(
    tx: &Transaction<'_>,
    thread: Option<&str>,
    command: Option<&str>,
    event: &Value,
) -> Result<i64> {
    tx.execute(
        "INSERT INTO events(thread_id,command_id,value) VALUES (?,?,?)",
        params![thread, command, event.to_string()],
    )?;
    Ok(tx.last_insert_rowid())
}
fn records(connection: &Connection, query: &str) -> Result<Vec<Value>> {
    let mut query = connection.prepare(query)?;
    let rows: Vec<String> = query
        .query_map([], |row| row.get(0))?
        .collect::<rusqlite::Result<_>>()?;
    Ok(rows
        .iter()
        .map(|row| serde_json::from_str(row))
        .collect::<serde_json::Result<_>>()?)
}

#[cfg(test)]
mod tests {
    use super::*;
    fn setup() -> Result<(tempfile::TempDir, Store)> {
        let directory = tempfile::tempdir()?;
        let store = Store::open(&directory.path().join("state.sqlite"))?;
        store.dispatch(&json!({"type":"project.create","commandId":"project-command","projectId":"project","title":"Test","workspaceRoot":"/test"}))?;
        store.dispatch(&json!({"type":"thread.create","commandId":"thread-command","threadId":"thread","projectId":"project","title":"Work","modelSelection":{"instanceId":"codex","model":"default"},"createdBy":"user","creationSource":"server"}))?;
        Ok((directory, store))
    }
    fn message() -> Value {
        json!({"type":"message.dispatch","commandId":"send","threadId":"thread","messageId":"message","text":"hello","attachments":[]})
    }

    #[test]
    fn duplicate_command_commits_one_message_and_one_effect() -> Result<()> {
        let (_directory, store) = setup()?;
        let first = store.dispatch(&message())?;
        assert_eq!(store.dispatch(&message())?, first);
        assert_eq!(
            store.thread("thread")?["messages"]
                .as_array()
                .unwrap()
                .len(),
            1
        );
        assert!(store.claim_effect()?.is_some());
        assert!(store.claim_effect()?.is_none());
        let mut changed = message();
        changed["text"] = json!("different");
        assert!(store.dispatch(&changed).is_err());
        Ok(())
    }

    #[test]
    fn failed_command_rolls_back_receipt_projection_event_and_effect() -> Result<()> {
        let (_directory, store) = setup()?;
        let before = store.shell()?;
        let mut invalid = message();
        invalid["attachments"] = json!([{"type":"image"}]);
        assert!(store.dispatch(&invalid).is_err());
        assert_eq!(store.shell()?, before);
        assert!(
            store.thread("thread")?["messages"]
                .as_array()
                .unwrap()
                .is_empty()
        );
        assert!(store.claim_effect()?.is_none());
        assert!(store.dispatch(&message()).is_ok());
        Ok(())
    }

    #[test]
    fn restart_retires_uncertain_work_instead_of_running_an_agent_twice() -> Result<()> {
        let (directory, store) = setup()?;
        store.dispatch(&message())?;
        drop(store);
        let reopened = Store::open(&directory.path().join("state.sqlite"))?;
        assert_eq!(reopened.thread("thread")?["thread"]["status"], "error");
        assert_eq!(
            reopened.compatible_shell()?["threads"][0]["status"],
            "failed"
        );
        assert!(reopened.claim_effect()?.is_none());
        assert_eq!(reopened.thread("thread")?["messages"][0]["text"], "hello");
        assert!(reopened.dispatch(&message()).is_ok());
        assert!(reopened.claim_effect()?.is_none());
        Ok(())
    }

    #[test]
    fn archive_and_unarchive_preserve_history_and_project_ownership() -> Result<()> {
        let (_directory, store) = setup()?;
        store.dispatch(
            &json!({"type":"thread.archive","commandId":"archive","threadId":"thread"}),
        )?;
        assert_eq!(
            store.shell()?["archivedThreads"].as_array().unwrap().len(),
            1
        );
        assert!(store.shell()?["threads"].as_array().unwrap().is_empty());
        store.dispatch(
            &json!({"type":"thread.unarchive","commandId":"restore","threadId":"thread"}),
        )?;
        assert_eq!(store.shell()?["threads"][0]["projectId"], "project");
        Ok(())
    }

    #[test]
    fn standard_thread_configuration_commands_update_the_compatible_projection() -> Result<()> {
        let (_directory, store) = setup()?;
        store.dispatch(&json!({"type":"thread.runtime-mode.set","commandId":"runtime","threadId":"thread","runtimeMode":"approval-required"}))?;
        store.dispatch(&json!({"type":"thread.interaction-mode.set","commandId":"interaction","threadId":"thread","interactionMode":"plan"}))?;
        store.dispatch(&json!({"type":"provider.switch","commandId":"provider","threadId":"thread","modelSelection":{"instanceId":"claude-code","model":"default"}}))?;
        let thread = &store.compatible_thread("thread")?["projection"]["thread"];
        assert_eq!(thread["runtimeMode"], "approval-required");
        assert_eq!(thread["interactionMode"], "plan");
        assert_eq!(thread["providerInstanceId"], "claude-code");
        Ok(())
    }
    #[test]
    fn native_tool_updates_and_pending_permissions_survive_snapshot_reads() -> Result<()> {
        let (_directory, store) = setup()?;
        store.dispatch(&message())?;
        store.agent_event("thread",&json!({"type":"acp.update","data":{"update":{"sessionUpdate":"tool_call","toolCallId":"write","title":"Edit main.rs","kind":"edit","status":"pending","rawInput":{"file":"main.rs"}}}}),None,false,None)?;
        store.agent_event("thread",&json!({"type":"acp.permission","id":"permission","request":{"toolCall":{"toolCallId":"write","title":"Write file","kind":"edit","status":"pending"},"options":[{"optionId":"allow","name":"Allow once","kind":"allow_once"},{"optionId":"always","name":"Allow for session","kind":"allow_always"},{"optionId":"deny","name":"Reject","kind":"reject_once"}]}}),None,false,None)?;
        let waiting = store.thread("thread")?;
        assert_eq!(waiting["thread"]["status"], "waiting");
        assert_eq!(
            waiting["thread"]["pendingAcpPermission"]["id"],
            "permission"
        );
        let compatible_waiting = store.compatible_thread("thread")?;
        assert_eq!(
            compatible_waiting["projection"]["thread"]["pendingRuntimeRequest"]["kind"],
            "file-change"
        );
        assert_eq!(
            compatible_waiting["projection"]["runtimeRequests"][0]["responseCapability"]["type"],
            "live"
        );
        assert_eq!(
            compatible_waiting["projection"]["turnItems"][2]["type"],
            "approval_request"
        );
        assert_eq!(
            compatible_waiting["projection"]["turnItems"][2]["options"][1]["decision"],
            "acceptForSession"
        );
        store.agent_event("thread",&json!({"type":"acp.update","data":{"update":{"sessionUpdate":"tool_call_update","toolCallId":"write","status":"completed","rawOutput":"saved"}}}),None,false,None)?;
        store.agent_event(
            "thread",
            &json!({"type":"acp.turn.finished","data":{"stopReason":"end_turn"}}),
            None,
            true,
            None,
        )?;
        let done = store.thread("thread")?;
        let tool = &done["messages"][1]["toolCall"];
        assert_eq!(tool["title"], "Edit main.rs");
        assert_eq!(tool["kind"], "edit");
        assert_eq!(tool["status"], "completed");
        assert_eq!(tool["rawInput"]["file"], "main.rs");
        assert_eq!(tool["rawOutput"], "saved");
        assert_eq!(done["messages"].as_array().unwrap().len(), 2);
        assert!(done["thread"]["pendingAcpPermission"].is_null());
        assert_eq!(done["thread"]["status"], "idle");
        let compatible = store.compatible_thread("thread")?;
        assert_eq!(compatible["snapshotSequence"], done["snapshotSequence"]);
        assert_eq!(
            compatible["projection"]["messages"]
                .as_array()
                .unwrap()
                .len(),
            1
        );
        assert_eq!(
            compatible["projection"]["turnItems"]
                .as_array()
                .unwrap()
                .len(),
            2
        );
        assert_eq!(
            compatible["projection"]["visibleTurnItems"][0]["item"]["type"],
            "user_message"
        );
        assert_eq!(
            compatible["projection"]["visibleTurnItems"][1]["item"]["type"],
            "dynamic_tool"
        );
        assert_eq!(
            compatible["projection"]["visibleTurnItems"][1]["item"]["output"],
            "saved"
        );
        Ok(())
    }
}
