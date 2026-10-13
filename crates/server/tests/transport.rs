use anyhow::Result;
use futures::{SinkExt, StreamExt};
use serde_json::{Value, json};
use t3_server::{Server, ServerOptions};
use tokio_tungstenite::{connect_async, tungstenite::Message};

#[tokio::test]
async fn bearer_pairing_and_effect_rpc_round_trip() -> Result<()> {
    let directory = tempfile::tempdir()?;
    let workspace = tempfile::tempdir()?;
    std::fs::create_dir(workspace.path().join("src"))?;
    std::fs::write(
        workspace.path().join("src/lib.rs"),
        "pub fn transport_contract() {}\n",
    )?;
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
    let pairing: Value = client
        .post(format!("{base}/api/auth/pairing-token"))
        .bearer_auth(access)
        .json(&json!({"label":"Transport phone"}))
        .send()
        .await?
        .error_for_status()?
        .json()
        .await?;
    assert_eq!(pairing["label"], "Transport phone");
    let pairing_links: Value = client
        .get(format!("{base}/api/auth/pairing-links"))
        .bearer_auth(access)
        .send()
        .await?
        .error_for_status()?
        .json()
        .await?;
    assert_eq!(pairing_links.as_array().unwrap().len(), 1);
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
    socket.send(Message::Text(json!({"_tag":"Request","id":"request-1","tag":"projects.mutate","payload":{"type":"project.create","commandId":"command","projectId":"project","title":"Transport test","workspaceRoot":workspace.path()},"headers":[]}).to_string().into())).await?;
    let response: Value = serde_json::from_str(socket.next().await.unwrap()?.to_text()?)?;
    assert_eq!(response["_tag"], "Exit");
    assert_eq!(response["requestId"], "request-1");
    assert_eq!(response["exit"]["_tag"], "Success");
    socket.send(Message::Text(json!([
        {"_tag":"Request","id":"write","tag":"projects.writeFile","payload":{"cwd":workspace.path(),"relativePath":"notes/status.md","contents":"filesystem ready"},"headers":[]},
        {"_tag":"Request","id":"read","tag":"projects.readFile","payload":{"cwd":workspace.path(),"relativePath":"src/lib.rs"},"headers":[]},
        {"_tag":"Request","id":"paths","tag":"projects.searchEntries","payload":{"cwd":workspace.path(),"query":"lib","limit":20},"headers":[]},
        {"_tag":"Request","id":"contents","tag":"projects.searchContents","payload":{"cwd":workspace.path(),"query":"transport_contract","limit":20,"caseSensitive":true,"wholeWord":true,"useRegex":false},"headers":[]},
        {"_tag":"Request","id":"list","tag":"projects.listEntries","payload":{"cwd":workspace.path(),"directoryPath":"src"},"headers":[]}
    ]).to_string().into())).await?;
    let write: Value = serde_json::from_str(socket.next().await.unwrap()?.to_text()?)?;
    assert_eq!(write["exit"]["value"]["relativePath"], "notes/status.md");
    let read: Value = serde_json::from_str(socket.next().await.unwrap()?.to_text()?)?;
    assert_eq!(read["exit"]["value"]["truncated"], false);
    assert!(
        read["exit"]["value"]["contents"]
            .as_str()
            .unwrap()
            .contains("transport_contract")
    );
    let paths: Value = serde_json::from_str(socket.next().await.unwrap()?.to_text()?)?;
    assert_eq!(paths["exit"]["value"]["entries"][0]["path"], "src/lib.rs");
    let contents: Value = serde_json::from_str(socket.next().await.unwrap()?.to_text()?)?;
    assert_eq!(contents["exit"]["value"]["matches"][0]["lineNumber"], 1);
    let list: Value = serde_json::from_str(socket.next().await.unwrap()?.to_text()?)?;
    assert_eq!(list["exit"]["value"]["entries"][0]["path"], "src/lib.rs");
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
        {"_tag":"Request","id":"search-threads","tag":"orchestration.searchThreads","payload":{"query":"missing","limit":10},"headers":[]},
        {"_tag":"Request","id":"search-thread","tag":"orchestration.searchThread","payload":{"threadId":"thread","query":"missing"},"headers":[]},
        {"_tag":"Request","id":"search-stream","tag":"orchestration.searchThreadStream","payload":{"threadId":"thread","query":"missing"},"headers":[]}
    ]).to_string().into())).await?;
    let search_threads: Value = serde_json::from_str(socket.next().await.unwrap()?.to_text()?)?;
    assert!(
        search_threads["exit"]["value"]["matches"]
            .as_array()
            .unwrap()
            .is_empty()
    );
    let search_thread: Value = serde_json::from_str(socket.next().await.unwrap()?.to_text()?)?;
    assert_eq!(search_thread["exit"]["value"]["complete"], true);
    assert_eq!(search_thread["exit"]["value"]["totalMatches"], 0);
    let search_stream: Value = serde_json::from_str(socket.next().await.unwrap()?.to_text()?)?;
    assert_eq!(search_stream["_tag"], "Chunk");
    assert_eq!(search_stream["values"][0]["match"], Value::Null);
    socket
        .send(Message::Text(
            json!({"_tag":"Interrupt","requestId":"search-stream"})
                .to_string()
                .into(),
        ))
        .await?;
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
    assert_eq!(
        access_state["values"][0]["payload"]["clientSessions"]
            .as_array()
            .unwrap()
            .len(),
        1
    );
    assert_eq!(
        access_state["values"][0]["payload"]["pairingLinks"][0]["label"],
        "Transport phone"
    );
    assert_eq!(
        access_state["values"][0]["payload"]["clientSessions"][0]["current"],
        true
    );
    assert_eq!(
        access_state["values"][0]["payload"]["clientSessions"][0]["connected"],
        true
    );
    socket
        .send(Message::Text(
            json!({"_tag":"Ack","requestId":"access"})
                .to_string()
                .into(),
        ))
        .await?;
    let second_token: Value = client
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
            ("subject_token", pairing["credential"].as_str().unwrap()),
            ("client_label", "Transport phone"),
            ("client_device_type", "mobile"),
            ("client_os", "iOS"),
        ])
        .send()
        .await?
        .error_for_status()?
        .json()
        .await?;
    assert!(second_token["access_token"].as_str().is_some());
    let changed_access: Value = serde_json::from_str(
        tokio::time::timeout(std::time::Duration::from_secs(10), socket.next())
            .await?
            .unwrap()?
            .to_text()?,
    )?;
    assert_eq!(changed_access["requestId"], "access");
    assert_eq!(
        changed_access["values"][0]["payload"]["clientSessions"]
            .as_array()
            .unwrap()
            .len(),
        2
    );
    assert!(
        changed_access["values"][0]["payload"]["pairingLinks"]
            .as_array()
            .unwrap()
            .is_empty()
    );
    let other_session = changed_access["values"][0]["payload"]["clientSessions"]
        .as_array()
        .unwrap()
        .iter()
        .find(|session| session["current"] == false)
        .unwrap()["sessionId"]
        .as_str()
        .unwrap()
        .to_owned();
    socket
        .send(Message::Text(
            json!({"_tag":"Ack","requestId":"access"})
                .to_string()
                .into(),
        ))
        .await?;
    let revoked: Value = client
        .post(format!("{base}/api/auth/clients/revoke"))
        .bearer_auth(access)
        .json(&json!({"sessionId":other_session}))
        .send()
        .await?
        .error_for_status()?
        .json()
        .await?;
    assert_eq!(revoked["revoked"], true);
    let revoked_access: Value = serde_json::from_str(
        tokio::time::timeout(std::time::Duration::from_secs(10), socket.next())
            .await?
            .unwrap()?
            .to_text()?,
    )?;
    assert_eq!(
        revoked_access["values"][0]["payload"]["clientSessions"]
            .as_array()
            .unwrap()
            .len(),
        1
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
