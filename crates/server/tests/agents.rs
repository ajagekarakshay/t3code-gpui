use anyhow::{Context, Result};
use serde_json::{Value, json};
use std::collections::BTreeMap;
use t3_acp::AgentCommand;
use t3_server::{Server, ServerOptions};
async fn milestone(
    events: &mut tokio::sync::broadcast::Receiver<Value>,
    kind: &str,
) -> Result<Value> {
    tokio::time::timeout(std::time::Duration::from_secs(10), async {
        loop {
            let change = events.recv().await?;
            if change["agentEvent"]["type"] == kind {
                return Ok(change);
            }
        }
    })
    .await?
}
fn options(directory: &std::path::Path) -> ServerOptions {
    let command = AgentCommand::new(env!("CARGO_BIN_EXE_t3-server-acp-fixture"));
    ServerOptions::new(directory).agents(BTreeMap::from([
        ("codex".into(), command.clone()),
        ("claude-code".into(), command),
    ]))
}
#[tokio::test]
async fn durable_dispatch_runs_acp_permissions_cancellation_and_native_resume() -> Result<()> {
    let directory = tempfile::tempdir()?;
    let server = Server::open(options(directory.path()))?;
    server.dispatch(json!({"type":"project.create","commandId":"project","projectId":"project","workspaceRoot":directory.path(),"title":"Project"})).await?;
    server.dispatch(json!({"type":"thread.create","commandId":"thread","threadId":"thread","projectId":"project","title":"Work","modelSelection":{"instanceId":"codex","model":"default"}})).await?;
    let mut events = server.subscribe();
    let prompt = json!({"type":"message.dispatch","commandId":"first","threadId":"thread","messageId":"first","text":"first prompt","attachments":[]});
    let result = server.dispatch(prompt.clone()).await?;
    assert_eq!(server.dispatch(prompt).await?, result);
    let finished = milestone(&mut events, "acp.turn.finished").await?;
    assert_eq!(finished["thread"]["status"], "idle");
    let snapshot = server.thread("thread")?;
    assert_eq!(snapshot["messages"].as_array().unwrap().len(), 2);
    assert_eq!(snapshot["messages"][1]["text"], "first prompt second chunk");
    assert_eq!(snapshot["thread"]["nativeSessionId"], "native-test");
    server.dispatch(json!({"type":"message.dispatch","commandId":"permission","threadId":"thread","messageId":"permission","text":"permission","attachments":[]})).await?;
    let pending = milestone(&mut events, "acp.permission").await?;
    let id = pending["agentEvent"]["id"]
        .as_str()
        .context("Permission ID")?;
    assert_eq!(
        server.thread("thread")?["thread"]["pendingAcpPermission"]["id"],
        id
    );
    let permission_result = server
        .dispatch(json!({"type":"runtime-request.respond","commandId":"permission-response","threadId":"thread","requestId":id,"decision":"decline"}))
        .await?;
    assert_eq!(permission_result["commandId"], "permission-response");
    milestone(&mut events, "acp.turn.finished").await?;
    let snapshot = server.thread("thread")?;
    assert_eq!(snapshot["messages"][3]["text"], "deny");
    assert!(snapshot["thread"]["pendingAcpPermission"].is_null());
    server.dispatch(json!({"type":"message.dispatch","commandId":"wait","threadId":"thread","messageId":"wait","text":"wait","attachments":[]})).await?;
    milestone(&mut events, "acp.update").await?;
    server.dispatch(json!({"type":"message.dispatch","commandId":"queued","threadId":"thread","messageId":"queued","text":"queued","attachments":[],"deliveryIntent":"auto","dispatchMode":{"type":"start_immediately"}})).await?;
    assert_eq!(
        server.thread("thread")?["messages"]
            .as_array()
            .unwrap()
            .last()
            .unwrap()["runStatus"],
        "queued"
    );
    let run_id = server.thread("thread")?["thread"]["activeRunId"]
        .as_str()
        .context("Active run ID")?
        .to_owned();
    let interrupt = json!({"type":"run.interrupt","commandId":"interrupt","threadId":"thread","runId":run_id,"holdQueue":true});
    let interrupt_result = server.dispatch(interrupt.clone()).await?;
    assert_eq!(server.dispatch(interrupt).await?, interrupt_result);
    let cancelled = milestone(&mut events, "acp.turn.finished").await?;
    assert_eq!(cancelled["agentEvent"]["data"]["stopReason"], "cancelled");
    assert_eq!(
        server.thread("thread")?["messages"][4]["runStatus"],
        "interrupted"
    );
    assert!(server.thread("thread")?["thread"]["activeRunId"].is_null());
    server
        .dispatch(json!({"type":"queue.resume","commandId":"resume-queue","threadId":"thread"}))
        .await?;
    milestone(&mut events, "acp.turn.finished").await?;
    assert_eq!(
        server.thread("thread")?["messages"]
            .as_array()
            .unwrap()
            .last()
            .unwrap()["text"],
        "queued second chunk"
    );
    server.shutdown().await;
    drop(server);
    let reopened = Server::open(options(directory.path()))?;
    let mut events = reopened.subscribe();
    reopened.dispatch(json!({"type":"message.dispatch","commandId":"resumed","threadId":"thread","messageId":"resumed","text":"after restart","attachments":[]})).await?;
    let ready = milestone(&mut events, "acp.ready").await?;
    assert_eq!(ready["agentEvent"]["data"]["sessionId"], "native-test");
    milestone(&mut events, "acp.turn.finished").await?;
    let snapshot = reopened.thread("thread")?;
    assert_eq!(
        snapshot["messages"].as_array().unwrap().last().unwrap()["text"],
        "after restart second chunk"
    );
    reopened.shutdown().await;
    Ok(())
}
