use anyhow::Result;
use serde_json::{Value, json};
use t3_client::{Client, PairingTarget, SavedSession};
use t3_server::{Server, ServerOptions};

#[test]
fn pairing_links_preserve_hosted_authority_and_fragment_precedence() -> Result<()> {
    assert_eq!(PairingTarget::from_url("https://app.t3.codes/pair?host=https%3A%2F%2Fmachine.tailnet.ts.net%3A444%2F&token=wrong#token=right")?.origin().as_str(), "https://machine.tailnet.ts.net:444/");
    assert_eq!(
        PairingTarget::from_url("wss://host.example/pair#token=code")?
            .origin()
            .as_str(),
        "https://host.example/"
    );
    assert!(PairingTarget::from_url("ftp://host.example/pair#token=secret").is_err());
    assert!(PairingTarget::new("https://user:password@host.example", "secret").is_err());
    assert!(PairingTarget::from_url("https://host.example/pair").is_err());
    Ok(())
}

#[tokio::test]
async fn proof_bound_pairing_saved_session_and_resumable_rpc_events() -> Result<()> {
    let directory = tempfile::tempdir()?;
    let server = Server::open(ServerOptions::new(directory.path()))?;
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let origin = format!("http://{}", listener.local_addr()?);
    let router = server.router();
    let task = tokio::spawn(async move { axum::serve(listener, router).await.unwrap() });
    let session = PairingTarget::from_url(&server.pairing_url(&origin)?)?
        .exchange()
        .await?;
    assert_eq!(
        session.environment_id(),
        server.descriptor()["environmentId"]
    );
    assert!(session.scopes().iter().any(|scope| scope == "relay:write"));
    assert!(session.scopes().iter().any(|scope| scope == "access:write"));
    let saved: SavedSession = serde_json::from_slice(&serde_json::to_vec(&session)?)?;
    assert_eq!(
        saved.get("/api/auth/session").await?["method"],
        "dpop-access-token"
    );
    assert!(saved.get("//other.example/secret").await.is_err());
    let grant = saved
        .post(
            "/api/auth/pairing-token",
            &json!({"scopes":["orchestration:read","orchestration:operate"]}),
        )
        .await?;
    let credential = grant["credential"].as_str().unwrap();
    let session = PairingTarget::from_url(&format!("{origin}/pair#token={credential}"))?
        .exchange()
        .await?;
    assert!(
        PairingTarget::new(&origin, credential)?
            .exchange()
            .await
            .is_err()
    );
    let client = Client::connect(&session).await?;
    assert_eq!(
        client.call("server.probe", json!({})).await?["environmentId"],
        saved.environment_id()
    );
    assert!(
        client
            .call("missing.method", json!({}))
            .await
            .unwrap_err()
            .to_string()
            .contains("not ported")
    );
    assert!(
        session
            .post(
                "/api/auth/pairing-token",
                &json!({"scopes":["access:write"]})
            )
            .await
            .is_err()
    );

    let mut subscription = client
        .subscribe("rust.subscribeEvents", json!({"afterSequence":0}))
        .await?;
    let first = subscription.next().await?;
    assert_eq!(first[0]["cursor"], 0);
    // Exceed both the live notification queue and the replay page while the client withholds ACK.
    for index in 0..300 {
        server.dispatch(json!({"type":"project.create","commandId":format!("command-{index}"),"projectId":format!("project-{index}"),"title":format!("Project {index}"),"workspaceRoot":directory.path()})).await?;
    }
    let mut cursor = 0;
    let mut observed = Vec::<Value>::new();
    while cursor < 300 {
        let pages = subscription.next().await?;
        for page in pages {
            let changes = page["changes"].as_array().unwrap();
            for change in changes {
                assert_eq!(change["sequence"], cursor + 1);
                cursor += 1;
                observed.push(change.clone());
            }
        }
    }
    assert_eq!(observed.len(), 300);
    assert_eq!(observed[299]["event"]["project"]["title"], "Project 299");
    drop(subscription);
    drop(client);
    let reconnected = Client::connect(&session).await?;
    let mut resumed = reconnected
        .subscribe("rust.subscribeEvents", json!({"afterSequence":299}))
        .await?;
    assert_eq!(resumed.next().await?[0]["changes"][0]["sequence"], 300);
    let shell = session.get("/api/orchestration/shell").await?;
    assert_eq!(shell["projects"].as_array().unwrap().len(), 300);
    drop(resumed);
    drop(reconnected);
    task.abort();
    let _ = task.await;
    server.shutdown().await;
    Ok(())
}
