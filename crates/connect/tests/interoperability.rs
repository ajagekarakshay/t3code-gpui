use anyhow::Result;
use serde_json::{Value, json};
use t3_connect::{
    EnvironmentIdentity, MINT_REQUEST_TYPE, MINT_RESPONSE_TYPE, verify_dpop, verify_relay_proof,
};

fn fixture() -> Value {
    serde_json::from_str(include_str!("fixtures/connect.json")).unwrap()
}

#[test]
fn accepts_node_ed25519_relay_proofs_and_pkcs8_keys() -> Result<()> {
    let fixture = fixture();
    let claims = verify_relay_proof(
        fixture["relayPublicKey"].as_str().unwrap(),
        fixture["relayProof"].as_str().unwrap(),
        MINT_REQUEST_TYPE,
        "https://relay.fixture.test",
        "t3-env:fixture",
        1800000000,
    )?;
    assert_eq!(claims, fixture["relayClaims"]);
    let identity = EnvironmentIdentity::from_pem(fixture["relayPrivateKey"].as_str().unwrap())?;
    let response = json!({"iss":"t3-env:fixture","aud":"https://relay.fixture.test","sub":"fixture","jti":"response-id","iat":1800000000_i64,"exp":1800000060_i64,"requestNonce":"relay-nonce","credential":"test-only"});
    let signed = identity.sign(MINT_RESPONSE_TYPE, &response)?;
    assert_eq!(
        verify_relay_proof(
            fixture["relayPublicKey"].as_str().unwrap(),
            &signed,
            MINT_RESPONSE_TYPE,
            "t3-env:fixture",
            "https://relay.fixture.test",
            1800000000
        )?,
        response
    );
    Ok(())
}

#[test]
fn accepts_node_es256_dpop_and_rejects_wrong_request_and_token() -> Result<()> {
    let fixture = fixture();
    let token = fixture["dpopProof"].as_str().unwrap();
    let thumbprint = fixture["thumbprint"].as_str().unwrap();
    let url = "https://environment.fixture.test/api/auth/session?query=ignored";
    assert_eq!(
        verify_dpop(
            token,
            "GET",
            url,
            1800000000,
            Some(thumbprint),
            Some("fixture-access-token")
        )?
        .thumbprint(),
        thumbprint
    );
    assert!(
        verify_dpop(
            token,
            "POST",
            url,
            1800000000,
            Some(thumbprint),
            Some("fixture-access-token")
        )
        .is_err()
    );
    assert!(
        verify_dpop(
            token,
            "GET",
            url,
            1800000000,
            Some(thumbprint),
            Some("wrong-token")
        )
        .is_err()
    );
    assert!(
        verify_dpop(
            token,
            "GET",
            url,
            1800000400,
            Some(thumbprint),
            Some("fixture-access-token")
        )
        .is_err()
    );
    assert!(
        verify_dpop(
            token,
            "GET",
            "https://other.test/api/auth/session",
            1800000000,
            Some(thumbprint),
            Some("fixture-access-token")
        )
        .is_err()
    );
    Ok(())
}
