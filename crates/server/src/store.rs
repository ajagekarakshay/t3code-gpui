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
                let thread = json!({"id":id,"projectId":project_id,"title":required(command,"title")?,"modelSelection":command["modelSelection"],"providerInstanceId":provider,"runtimeMode":command.get("runtimeMode").cloned().unwrap_or(json!("supervised")),"interactionMode":command.get("interactionMode").cloned().unwrap_or(json!("default")),"branch":command["branch"],"worktreePath":command["worktreePath"],"createdBy":command.get("createdBy").cloned().unwrap_or(json!({"type":"user"})),"creationSource":command.get("creationSource").cloned().unwrap_or(json!("user")),"lineage":{"parentThreadId":null,"relationshipToParent":null,"rootThreadId":id},"forkedFrom":null,"activeProviderThreadId":null,"latestRunId":null,"activeRunId":null,"status":"idle","pendingRuntimeRequest":null,"latestVisibleMessage":null,"latestUserMessageAt":null,"hasActionableProposedPlan":false,"itemCount":0,"visibleItemCount":0,"createdAt":timestamp,"updatedAt":timestamp,"archivedAt":null,"settledOverride":null,"settledAt":null,"deletedAt":null});
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
                write_record(&tx, "threads", id, &thread)?;
                tx.execute(
                    "INSERT INTO outbox(thread_id,value) VALUES (?,?)",
                    params![id, command.to_string()],
                )?;
                json!({"type":"rust.message.dispatched","message":message,"thread":thread})
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
                        if let Some(title) = command.get("title") {
                            thread["title"] = title.clone();
                        }
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
    ) -> Result<()> {
        let mut connection = self.lock()?;
        let tx = connection.transaction()?;
        let mut thread = read_record(&tx, "threads", thread_id)?;
        let timestamp = now();
        if let Some(text) = text {
            let message_id = format!("assistant:{}", required(&thread, "latestRunId")?);
            let existing: Option<String> = tx
                .query_row(
                    "SELECT value FROM messages WHERE id=?",
                    [&message_id],
                    |row| row.get(0),
                )
                .optional()?;
            let mut message = if let Some(value) = existing {
                serde_json::from_str::<Value>(&value)?
            } else {
                json!({"id":message_id,"threadId":thread_id,"runId":thread["latestRunId"],"nodeId":null,"role":"assistant","text":"","attachments":[],"streaming":true,"createdAt":timestamp,"updatedAt":timestamp,"createdBy":{"type":"agent"},"creationSource":"agent"})
            };
            message["text"] = json!(format!(
                "{}{text}",
                message["text"].as_str().unwrap_or_default()
            ));
            message["updatedAt"] = json!(timestamp);
            thread["latestVisibleMessage"] = json!({"id":message_id,"role":"assistant","text":message["text"],"updatedAt":timestamp});
            tx.execute("INSERT INTO messages(id,thread_id,value) VALUES (?,?,?) ON CONFLICT(id) DO UPDATE SET value=excluded.value",params![message_id,thread_id,message.to_string()])?;
        }
        thread["status"] = json!(if failure.is_some() {
            "error"
        } else if finished {
            "idle"
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
        thread["updatedAt"] = json!(timestamp);
        write_record(&tx, "threads", thread_id, &thread)?;
        append_event(&tx, Some(thread_id), None, event)?;
        tx.commit()?;
        Ok(())
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
}
