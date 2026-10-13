use anyhow::Result;
use futures::{SinkExt, StreamExt};
use serde_json::{Value, json};
use t3_server::{Server, ServerOptions};
use tokio_tungstenite::{connect_async, tungstenite::Message};

#[tokio::test]
async fn bearer_pairing_and_effect_rpc_round_trip() -> Result<()> {
    let directory = tempfile::tempdir()?;
    let server = Server::open(ServerOptions::new(directory.path()))?;
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let address = listener.local_addr()?;
    let router = server.router();
    let task = tokio::spawn(async move { axum::serve(listener, router).await.unwrap() });
    let base = format!("http://{address}");
    let client = reqwest::Client::new();
    assert_eq!(
        client
            .get(format!("{base}/api/orchestration/shell"))
            .send()
            .await?
            .status(),
        401
    );
    let credential = server.bootstrap_credential()?;
    let token: Value = client
        .post(format!("{base}/oauth/token"))
        .form(&[
            (
                "grant_type",
                "urn:ietf:params:oauth:grant-type:token-exchange",
            ),
            (
                "subject_token_type",
                "urn:t3:params:oauth:token-type:environment-bootstrap",
            ),
            (
                "requested_token_type",
                "urn:ietf:params:oauth:token-type:access_token",
            ),
            ("subject_token", credential.as_str()),
        ])
        .send()
        .await?
        .error_for_status()?
        .json()
        .await?;
    let access = token["access_token"].as_str().unwrap();
    let auth_state: Value = client
        .get(format!("{base}/api/auth/session"))
        .bearer_auth(access)
        .send()
        .await?
        .error_for_status()?
        .json()
        .await?;
    assert_eq!(auth_state["auth"]["policy"], "remote-reachable");
    assert_eq!(auth_state["sessionMethod"], "bearer-access-token");
    let missing = client
        .get(format!("{base}/api/orchestration/threads/missing/bounded"))
        .bearer_auth(access)
        .send()
        .await?;
    assert_eq!(missing.status(), reqwest::StatusCode::NOT_FOUND);
    let missing: Value = missing.json().await?;
    assert_eq!(missing["_tag"], "EnvironmentResourceNotFoundError");
    assert_eq!(missing["reason"], "thread_not_found");
    let ticket: Value = client
        .post(format!("{base}/api/auth/websocket-ticket"))
        .bearer_auth(access)
        .send()
        .await?
        .error_for_status()?
        .json()
        .await?;
    let url = format!(
        "ws://{address}/ws?wsTicket={}",
        ticket["ticket"].as_str().unwrap()
    );
    let (mut socket, _) = connect_async(&url).await?;
    assert!(connect_async(&url).await.is_err());
    socket.send(Message::Text(json!({"_tag":"Request","id":"request-1","tag":"projects.mutate","payload":{"type":"project.create","commandId":"command","projectId":"project","title":"Transport test","workspaceRoot":directory.path()},"headers":[]}).to_string().into())).await?;
    let response: Value = serde_json::from_str(socket.next().await.unwrap()?.to_text()?)?;
    assert_eq!(response["_tag"], "Exit");
    assert_eq!(response["requestId"], "request-1");
    assert_eq!(response["exit"]["_tag"], "Success");
    let snapshot: Value = client
        .get(format!("{base}/api/orchestration/shell"))
        .bearer_auth(access)
        .send()
        .await?
        .error_for_status()?
        .json()
        .await?;
    assert_eq!(snapshot["projects"][0]["title"], "Transport test");
    socket
        .send(Message::Text(
            json!({
                "_tag":"Request",
                "id":"launch",
                "tag":"orchestration.launchThread",
                "payload":{
                    "commandId":"launch-command",
                    "creationSource":"web",
                    "threadId":"thread",
                    "projectId":"project",
                    "title":"Compatible thread",
                    "modelSelection":{"instanceId":"codex","model":"default"},
                    "runtimeMode":"full-access",
                    "interactionMode":"default",
                    "workspaceStrategy":{"type":"root"}
                },
                "headers":[]
            })
            .to_string()
            .into(),
        ))
        .await?;
    let launch: Value = serde_json::from_str(socket.next().await.unwrap()?.to_text()?)?;
    assert_eq!(launch["requestId"], "launch");
    assert_eq!(launch["exit"]["value"]["threadId"], "thread");
    assert_eq!(launch["exit"]["value"]["resumed"], false);
    let thread_snapshot: Value = client
        .get(format!("{base}/api/orchestration/threads/thread"))
        .bearer_auth(access)
        .send()
        .await?
        .error_for_status()?
        .json()
        .await?;
    assert_eq!(
        thread_snapshot["projection"]["thread"]["title"],
        "Compatible thread"
    );
    socket.send(Message::Text(json!([
        {"_tag":"Request","id":"config","tag":"server.getConfig","payload":{},"headers":[]},
        {"_tag":"Request","id":"thread-subscription","tag":"orchestration.subscribeThread","payload":{"threadId":"thread"},"headers":[]}
    ]).to_string().into())).await?;
    let config: Value = serde_json::from_str(socket.next().await.unwrap()?.to_text()?)?;
    assert_eq!(config["requestId"], "config");
    assert_eq!(
        config["exit"]["value"]["environment"]["orchestrationProtocolVersion"],
        2
    );
    assert_eq!(
        config["exit"]["value"]["providers"]
            .as_array()
            .unwrap()
            .len(),
        2
    );
    let thread: Value = serde_json::from_str(socket.next().await.unwrap()?.to_text()?)?;
    assert_eq!(thread["requestId"], "thread-subscription");
    assert_eq!(thread["values"][0]["kind"], "snapshot");
    assert_eq!(thread["values"][0]["projection"]["thread"]["id"], "thread");
    socket
        .send(Message::Text(
            json!({"_tag":"Interrupt","requestId":"thread-subscription"})
                .to_string()
                .into(),
        ))
        .await?;
    socket.send(Message::Text(json!([
        {"_tag":"Request","id":"lifecycle","tag":"subscribeServerLifecycle","payload":{},"headers":[]},
        {"_tag":"Request","id":"access","tag":"subscribeAuthAccess","payload":{},"headers":[]}
    ]).to_string().into())).await?;
    let lifecycle: Value = serde_json::from_str(socket.next().await.unwrap()?.to_text()?)?;
    assert_eq!(lifecycle["requestId"], "lifecycle");
    assert_eq!(lifecycle["values"][0]["type"], "welcome");
    assert_eq!(
        lifecycle["values"][0]["payload"]["bootstrapStatus"],
        "complete"
    );
    let access_state: Value = serde_json::from_str(socket.next().await.unwrap()?.to_text()?)?;
    assert_eq!(access_state["requestId"], "access");
    assert_eq!(access_state["values"][0]["type"], "snapshot");
    assert!(
        access_state["values"][0]["payload"]["clientSessions"]
            .as_array()
            .unwrap()
            .is_empty()
    );
    socket
        .send(Message::Text(
            json!([
                {"_tag":"Interrupt","requestId":"lifecycle"},
                {"_tag":"Interrupt","requestId":"access"}
            ])
            .to_string()
            .into(),
        ))
        .await?;
    socket.send(Message::Text(json!({"_tag":"Request","id":9,"tag":"orchestration.subscribeShell","payload":{},"headers":[]}).to_string().into())).await?;
    let initial: Value = serde_json::from_str(socket.next().await.unwrap()?.to_text()?)?;
    assert_eq!(initial["requestId"], 9);
    assert_eq!(initial["_tag"], "Chunk");
    socket.send(Message::Text(json!([
        {"_tag":"Request","id":2,"tag":"projects.mutate","payload":{"type":"project.create","commandId":"second-command","projectId":"second-project","title":"Batched project","workspaceRoot":directory.path()},"headers":[]},
        {"_tag":"Request","id":"unsupported","tag":"missing.method","payload":{},"headers":[]},
        {"_tag":"Ack","requestId":9}
    ]).to_string().into())).await?;
    let mutation: Value = serde_json::from_str(socket.next().await.unwrap()?.to_text()?)?;
    assert_eq!(mutation["requestId"], 2);
    assert_eq!(mutation["exit"]["_tag"], "Success");
    let failure: Value = serde_json::from_str(socket.next().await.unwrap()?.to_text()?)?;
    assert_eq!(failure["requestId"], "unsupported");
    assert!(failure["exit"]["cause"].is_array());
    assert_eq!(failure["exit"]["cause"][0]["_tag"], "Die");
    let changed = tokio::time::timeout(std::time::Duration::from_secs(10), socket.next())
        .await?
        .unwrap()?;
    let changed: Value = serde_json::from_str(changed.to_text()?)?;
    assert_eq!(changed["requestId"], 9);
    assert_eq!(
        changed["values"][0]["snapshot"]["projects"]
            .as_array()
            .unwrap()
            .len(),
        2
    );
    socket
        .send(Message::Text(
            json!([{"_tag":"Interrupt","requestId":9},{"_tag":"Ping"}])
                .to_string()
                .into(),
        ))
        .await?;
    let pong: Value = serde_json::from_str(socket.next().await.unwrap()?.to_text()?)?;
    assert_eq!(pong["_tag"], "Pong");
    socket.close(None).await?;
    task.abort();
    let _ = task.await;
    server.shutdown().await;
    Ok(())
}
