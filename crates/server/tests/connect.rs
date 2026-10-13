use anyhow::{Context, Result};
use base64::{Engine as _, engine::general_purpose::URL_SAFE_NO_PAD as B64};
use serde_json::{Value, json};
use t3_connect::{DpopKey, EnvironmentIdentity};
use t3_server::{Server, ServerOptions};

fn claims(token: &str) -> Result<Value> {
    Ok(serde_json::from_slice(&B64.decode(
        token.split('.').nth(1).context("JWT payload")?,
    )?)?)
}
fn now() -> i64 {
    chrono::Utc::now().timestamp()
}
async fn exchange(
    client: &reqwest::Client,
    base: &str,
    credential: &str,
    key: Option<&DpopKey>,
) -> Result<reqwest::Response> {
    let url = format!("{base}/oauth/token");
    let mut request = client.post(&url).form(&[
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
        ("subject_token", credential),
    ]);
    if let Some(key) = key {
        request = request.header("DPoP", key.sign("POST", &url, now(), None)?);
    }
    Ok(request.send().await?)
}

#[tokio::test]
async fn relay_link_health_mint_and_dpop_work_over_the_existing_http_contract() -> Result<()> {
    let directory = tempfile::tempdir()?;
    let server = Server::open(ServerOptions::new(directory.path()))?;
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let address = listener.local_addr()?;
    server.start_connect(address).await?;
    let router = server.router();
    let task = tokio::spawn(async move { axum::serve(listener, router).await.unwrap() });
    struct Stop(tokio::task::JoinHandle<()>);
    impl Drop for Stop {
        fn drop(&mut self) {
            self.0.abort();
        }
    }
    let mut _stop = Stop(task);
    let base = format!("http://{address}");
    let client = reqwest::Client::new();
    let admin: Value = exchange(&client, &base, &server.bootstrap_credential()?, None)
        .await?
        .error_for_status()?
        .json()
        .await?;
    let access = admin["access_token"].as_str().unwrap();
    let endpoint = json!({"providerKind":"manual","httpBaseUrl":"https://environment.fixture.test","wsBaseUrl":"wss://environment.fixture.test"});
    let link = json!({"challenge":"link-challenge","relayIssuer":"https://relay.fixture.test/","endpoint":endpoint,"origin":{"localHttpHost":"127.0.0.1","localHttpPort":address.port()}});
    let proof: String = client
        .post(format!("{base}/api/connect/link-proof"))
        .bearer_auth(access)
        .json(&link)
        .send()
        .await?
        .error_for_status()?
        .json()
        .await?;
    let payload = claims(&proof)?;
    let environment = server.descriptor()["environmentId"]
        .as_str()
        .unwrap()
        .to_owned();
    let environment_issuer = format!("t3-env:{environment}");
    let public_key = payload["environmentPublicKey"].as_str().unwrap();
    t3_connect::verify_relay_proof(
        public_key,
        &proof,
        t3_connect::LINK_PROOF_TYPE,
        &environment_issuer,
        "https://relay.fixture.test",
        now(),
    )?;
    assert_eq!(payload["challenge"], "link-challenge");
    assert_eq!(payload["scopes"], json!(["agent_activity_notifications"]));
    assert_eq!(
        client
            .post(format!("{base}/api/connect/link-proof"))
            .bearer_auth(access)
            .header("x-forwarded-host", "environment.fixture.test")
            .json(&link)
            .send()
            .await?
            .status(),
        400
    );
    let mut invalid = link.clone();
    invalid["origin"]["localHttpPort"] = json!(address.port().saturating_sub(1));
    assert_eq!(
        client
            .post(format!("{base}/api/connect/link-proof"))
            .bearer_auth(access)
            .json(&invalid)
            .send()
            .await?
            .status(),
        400
    );
    let relay = EnvironmentIdentity::generate();
    let configuration = json!({"relayUrl":"https://relay.fixture.test","relayIssuer":"https://relay.fixture.test","cloudUserId":"cloud-user","environmentCredential":"synthetic-environment-credential","cloudMintPublicKey":relay.public_pem()?,"endpointRuntime":null});
    let configured: Value = client
        .post(format!("{base}/api/connect/relay-config"))
        .bearer_auth(access)
        .json(&configuration)
        .send()
        .await?
        .error_for_status()?
        .json()
        .await?;
    assert_eq!(
        configured,
        json!({"ok":true,"endpointRuntimeStatus":{"status":"disabled"}})
    );
    let state: Value = client
        .get(format!("{base}/api/connect/link-state"))
        .bearer_auth(access)
        .send()
        .await?
        .error_for_status()?
        .json()
        .await?;
    assert_eq!(state["linked"], true);
    assert_eq!(state["cloudUserId"], "cloud-user");
    assert_eq!(state["publishAgentActivity"], false);
    let health_claims = json!({"iss":"https://relay.fixture.test","aud":environment_issuer,"sub":"cloud-user","jti":"health-jti","iat":now(),"exp":now()+60,"environmentId":environment,"nonce":"health-nonce","scope":["environment:status"]});
    let health_request =
        json!({"proof":relay.sign(t3_connect::HEALTH_REQUEST_TYPE,&health_claims)?});
    let health: Value = client
        .post(format!("{base}/api/t3-connect/health"))
        .header("x-forwarded-proto", "https")
        .json(&health_request)
        .send()
        .await?
        .error_for_status()?
        .json()
        .await?;
    let verified = t3_connect::verify_relay_proof(
        public_key,
        health["proof"].as_str().unwrap(),
        t3_connect::HEALTH_RESPONSE_TYPE,
        &environment_issuer,
        "https://relay.fixture.test",
        now(),
    )?;
    assert_eq!(verified["requestNonce"], "health-nonce");
    assert_eq!(verified["descriptor"], *server.descriptor());
    assert_eq!(
        client
            .post(format!("{base}/api/t3-connect/health"))
            .json(&health_request)
            .send()
            .await?
            .status(),
        409
    );
    let key = DpopKey::generate();
    let thumbprint = key.thumbprint()?;
    let mint_claims = json!({"iss":"https://relay.fixture.test","aud":environment_issuer,"sub":"cloud-user","jti":"mint-jti","iat":now(),"exp":now()+60,"environmentId":environment,"nonce":"mint-nonce","scope":["environment:connect"],"clientProofKeyThumbprint":thumbprint,"cnf":{"jkt":thumbprint}});
    let mint_request = json!({"proof":relay.sign(t3_connect::MINT_REQUEST_TYPE,&mint_claims)?});
    let minted: Value = client
        .post(format!("{base}/api/connect/mint-credential"))
        .header("x-forwarded-proto", "https")
        .json(&mint_request)
        .send()
        .await?
        .error_for_status()?
        .json()
        .await?;
    let verified = t3_connect::verify_relay_proof(
        public_key,
        minted["proof"].as_str().unwrap(),
        t3_connect::MINT_RESPONSE_TYPE,
        &environment_issuer,
        "https://relay.fixture.test",
        now(),
    )?;
    assert_eq!(verified["credential"], minted["credential"]);
    assert_eq!(verified["requestNonce"], "mint-nonce");
    assert_eq!(verified["clientProofKeyThumbprint"], thumbprint);
    assert_eq!(
        client
            .post(format!("{base}/api/t3-connect/mint-credential"))
            .json(&mint_request)
            .send()
            .await?
            .status(),
        409
    );
    let credential = minted["credential"].as_str().unwrap();
    assert_eq!(
        exchange(&client, &base, credential, Some(&DpopKey::generate()))
            .await?
            .status(),
        401
    );
    let connected: Value = exchange(&client, &base, credential, Some(&key))
        .await?
        .error_for_status()?
        .json()
        .await?;
    let access_connected = connected["access_token"].as_str().unwrap();
    assert_eq!(connected["token_type"], "DPoP");
    let session_url = format!("{base}/api/auth/session");
    assert_eq!(
        client
            .get(&session_url)
            .bearer_auth(access_connected)
            .send()
            .await?
            .status(),
        401
    );
    let session: Value = client
        .get(&session_url)
        .header("Authorization", format!("DPoP {access_connected}"))
        .header(
            "DPoP",
            key.sign("GET", &session_url, now(), Some(access_connected))?,
        )
        .send()
        .await?
        .error_for_status()?
        .json()
        .await?;
    assert_eq!(session["authenticated"], true);
    let unlinked: Value = client
        .post(format!("{base}/api/connect/unlink"))
        .bearer_auth(access)
        .send()
        .await?
        .error_for_status()?
        .json()
        .await?;
    assert_eq!(unlinked["endpointRuntimeStatus"]["status"], "disabled");
    let mut fresh = health_claims;
    fresh["jti"] = json!("after-unlink");
    fresh["nonce"] = json!("after-unlink");
    assert_eq!(
        client
            .post(format!("{base}/api/t3-connect/health"))
            .json(&json!({"proof":relay.sign(t3_connect::HEALTH_REQUEST_TYPE,&fresh)?}))
            .send()
            .await?
            .status(),
        400
    );
    server.shutdown().await;
    // Restarting the same isolated home retains the environment's signing identity.
    _stop.0.abort();
    let _ = (&mut _stop.0).await;
    drop(server);
    let reopened = Server::open(ServerOptions::new(directory.path()))?;
    assert_eq!(reopened.descriptor()["environmentId"], environment);
    reopened.shutdown().await;
    Ok(())
}
