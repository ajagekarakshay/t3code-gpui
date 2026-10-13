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
    socket.close(None).await?;
    task.abort();
    let _ = task.await;
    server.shutdown().await;
    Ok(())
}
