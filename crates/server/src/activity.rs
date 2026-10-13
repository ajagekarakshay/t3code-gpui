use crate::{http::Services, store::required};
use anyhow::{Context, Result};
use serde_json::{Value, json};
use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
    time::Duration,
};
use tokio::sync::{Mutex, Notify};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum ActivityKind {
    Normal,
    Terminal,
    Tombstone,
}

impl ActivityKind {
    fn merge(self, next: Self) -> Self {
        match (self, next) {
            (_, Self::Tombstone) => Self::Tombstone,
            (Self::Tombstone, Self::Normal) => Self::Normal,
            (Self::Tombstone, Self::Terminal) => Self::Tombstone,
            (Self::Terminal, _) | (_, Self::Terminal) => Self::Terminal,
            _ => Self::Normal,
        }
    }
}

#[derive(Default)]
struct QueueState {
    pending: HashMap<String, ActivityKind>,
    running: HashSet<String>,
    published: HashMap<String, String>,
    generation: u64,
}

#[derive(Default)]
pub(crate) struct ActivityQueue {
    state: Mutex<QueueState>,
    changed: Notify,
}

impl Services {
    pub(crate) async fn reset_activity_publications(&self) {
        let mut state = self.activity.state.lock().await;
        state.generation += 1;
        state.published.clear();
        self.activity.changed.notify_waiters();
    }

    pub(crate) async fn queue_activity_catchup(self: &Arc<Self>) -> Result<()> {
        let preferences: Value = self
            .store
            .metadata("relayPreferences")?
            .map(|value| serde_json::from_str(&value))
            .transpose()?
            .unwrap_or(json!({}));
        if preferences["publishAgentActivity"] != true {
            return Ok(());
        }
        if let Some(threads) = self.store.shell()?["threads"].as_array() {
            for thread in threads.iter().filter(|thread| {
                matches!(
                    thread["status"].as_str(),
                    Some("starting" | "running" | "waiting")
                )
            }) {
                if let Some(id) = thread["id"].as_str() {
                    self.queue_activity(id, ActivityKind::Normal).await;
                }
            }
        }
        Ok(())
    }

    pub(crate) async fn queue_activity(
        self: &Arc<Self>,
        thread_id: impl Into<String>,
        kind: ActivityKind,
    ) {
        let thread_id = thread_id.into();
        let mut state = self.activity.state.lock().await;
        state
            .pending
            .entry(thread_id.clone())
            .and_modify(|current| *current = current.merge(kind))
            .or_insert(kind);
        self.activity.changed.notify_waiters();
        if !state.running.insert(thread_id.clone()) {
            return;
        }
        drop(state);

        let services = Arc::downgrade(self);
        let task = tokio::spawn(async move {
            loop {
                let Some(services) = services.upgrade() else {
                    return;
                };
                let kind = {
                    let mut state = services.activity.state.lock().await;
                    match state.pending.remove(&thread_id) {
                        Some(kind) => kind,
                        None => {
                            state.running.remove(&thread_id);
                            return;
                        }
                    }
                };
                if let Err(error) = services.publish_activity(&thread_id, kind).await {
                    tracing::warn!(%error, thread_id, "Could not publish agent activity");
                }
            }
        });
        let mut tasks = self.tasks.lock().await;
        tasks.retain(|task| !task.is_finished());
        tasks.push(task);
    }

    pub(crate) async fn publish_activity(&self, thread_id: &str, kind: ActivityKind) -> Result<()> {
        let state = self.activity_snapshot(thread_id, kind)?;
        let identity = state
            .as_ref()
            .map(|state| {
                let mut identity = state.clone();
                identity.as_object_mut().unwrap().remove("updatedAt");
                identity.to_string()
            })
            .unwrap_or_else(|| "null".into());
        let generation = {
            let queue = self.activity.state.lock().await;
            if queue.published.get(thread_id) == Some(&identity)
                || (state.is_none()
                    && kind != ActivityKind::Tombstone
                    && !queue.published.contains_key(thread_id))
            {
                return Ok(());
            }
            queue.generation
        };

        let mut last_error = None;
        for delay in [0, 1, 2, 4, 8, 16] {
            if self
                .wait_for_retry(thread_id, generation, Duration::from_secs(delay))
                .await
            {
                return Ok(());
            }
            match self.publish_agent_activity(thread_id, state.clone()).await {
                Ok(false) => return Ok(()),
                Ok(true) => {
                    let mut queue = self.activity.state.lock().await;
                    if queue.generation == generation {
                        queue.published.insert(thread_id.into(), identity);
                    }
                    return Ok(());
                }
                Err(error) => last_error = Some(error),
            }
        }
        Err(last_error.context("Agent activity publish failed")?)
    }

    async fn wait_for_retry(&self, thread_id: &str, generation: u64, delay: Duration) -> bool {
        let deadline = tokio::time::Instant::now() + delay;
        loop {
            let changed = self.activity.changed.notified();
            tokio::pin!(changed);
            {
                let state = self.activity.state.lock().await;
                if state.generation != generation || state.pending.contains_key(thread_id) {
                    return true;
                }
            }
            tokio::select! {
                _ = tokio::time::sleep_until(deadline) => return false,
                _ = &mut changed => {}
            }
        }
    }

    fn activity_snapshot(&self, thread_id: &str, kind: ActivityKind) -> Result<Option<Value>> {
        if kind == ActivityKind::Tombstone {
            return Ok(None);
        }
        let snapshot = self.store.thread(thread_id)?;
        let thread = &snapshot["thread"];
        if !thread["deletedAt"].is_null() || !thread["archivedAt"].is_null() {
            return Ok(None);
        }
        let status = required(thread, "status")?;
        let phase = match status {
            "starting" => "starting",
            "running" => "running",
            "waiting" if !thread["pendingAcpPermission"].is_null() => "waiting_for_approval",
            "waiting" => "waiting_for_input",
            "idle" if kind == ActivityKind::Terminal => "completed",
            "error" if kind == ActivityKind::Terminal => "failed",
            _ => return Ok(None),
        };
        let shell = self.store.shell()?;
        let project = shell["projects"]
            .as_array()
            .into_iter()
            .flatten()
            .find(|project| project["id"] == thread["projectId"])
            .context("Activity project is unavailable")?;
        let headline = match phase {
            "starting" => "Starting agent",
            "running" => "Agent is working",
            "waiting_for_approval" => "Approval needed",
            "waiting_for_input" => "Waiting for input",
            "completed" => "Agent finished",
            "failed" => "Agent failed",
            _ => unreachable!(),
        };
        let mut state = json!({
            "environmentId":required(&self.descriptor,"environmentId")?,
            "threadId":thread_id,
            "projectTitle":required(project,"title")?,
            "threadTitle":required(thread,"title")?,
            "phase":phase,
            "headline":headline,
            "modelTitle":thread["modelSelection"]["model"].as_str().unwrap_or("default"),
            "updatedAt":required(thread,"updatedAt")?,
            "deepLink":format!("/threads/{}/{}",required(&self.descriptor,"environmentId")?,url::form_urlencoded::byte_serialize(thread_id.as_bytes()).collect::<String>())
        });
        if phase == "completed" {
            state["detail"] = json!("Review the completed task.");
        } else if phase == "failed" {
            state["detail"] = json!("The agent run failed.");
        }
        Ok(Some(state))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn queued_activity_keeps_the_strongest_required_publish() {
        assert_eq!(
            ActivityKind::Normal.merge(ActivityKind::Terminal),
            ActivityKind::Terminal
        );
        assert_eq!(
            ActivityKind::Terminal.merge(ActivityKind::Tombstone),
            ActivityKind::Tombstone
        );
        assert_eq!(
            ActivityKind::Tombstone.merge(ActivityKind::Normal),
            ActivityKind::Normal
        );
    }
}
