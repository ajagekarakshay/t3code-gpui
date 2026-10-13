use anyhow::{Context, Result, ensure};
use base64::{Engine as _, engine::general_purpose::URL_SAFE_NO_PAD as B64};
use ed25519_dalek::{
    Signature, Signer, SigningKey, VerifyingKey,
    pkcs8::{DecodePrivateKey, DecodePublicKey, EncodePrivateKey, EncodePublicKey},
};
use serde_json::{Value, json};

/// Environment signing authority. Key material stays on its owning environment.
pub struct EnvironmentIdentity {
    key: SigningKey,
}

impl EnvironmentIdentity {
    pub fn generate() -> Self {
        Self {
            key: SigningKey::generate(&mut rand::rngs::OsRng),
        }
    }

    pub fn from_pem(pem: &str) -> Result<Self> {
        Ok(Self {
            key: SigningKey::from_pkcs8_pem(&pem.replace("\\n", "\n"))?,
        })
    }

    pub fn private_pem(&self) -> Result<String> {
        Ok(self.key.to_pkcs8_pem(Default::default())?.to_string())
    }

    pub fn public_pem(&self) -> Result<String> {
        Ok(self
            .key
            .verifying_key()
            .to_public_key_pem(Default::default())?)
    }

    pub fn sign(&self, typ: &str, payload: &Value) -> Result<String> {
        let header = B64.encode(serde_json::to_vec(&json!({"alg":"EdDSA", "typ":typ}))?);
        let payload = B64.encode(serde_json::to_vec(payload)?);
        let input = format!("{header}.{payload}");
        let signature = self.key.sign(input.as_bytes());
        Ok(format!("{input}.{}", B64.encode(signature.to_bytes())))
    }
}

pub fn validate_relay_key(pem: &str) -> Result<()> {
    VerifyingKey::from_public_key_pem(&pem.replace("\\n", "\n"))?;
    Ok(())
}

/// Validates JOSE exactly as the existing relay does, before consuming replay guards.
pub fn verify_relay_proof(
    pem: &str,
    token: &str,
    typ: &str,
    issuer: &str,
    audience: &str,
    now: i64,
) -> Result<Value> {
    let parts: Vec<_> = token.split('.').collect();
    ensure!(parts.len() == 3, "Invalid compact JWT");
    let header: Value = serde_json::from_slice(&B64.decode(parts[0])?)?;
    ensure!(
        header["alg"] == "EdDSA" && header["typ"] == typ,
        "Unexpected JWT header"
    );
    ensure!(
        header.get("crit").is_none(),
        "Unsupported critical JWT header"
    );
    let key = VerifyingKey::from_public_key_pem(&pem.replace("\\n", "\n"))?;
    let signature = Signature::from_slice(&B64.decode(parts[2])?)?;
    key.verify_strict(format!("{}.{}", parts[0], parts[1]).as_bytes(), &signature)?;
    let claims: Value = serde_json::from_slice(&B64.decode(parts[1])?)?;
    ensure!(
        claims["iss"] == issuer && claims["aud"] == audience,
        "Unexpected JWT authority"
    );
    let issued = claims["iat"].as_i64().context("Missing iat")?;
    let expiry = claims["exp"].as_i64().context("Missing exp")?;
    ensure!(
        issued <= now + 60 && issued >= now - 360 && expiry > now - 60,
        "Expired JWT"
    );
    ensure!(
        expiry > issued && expiry - issued <= 300,
        "Unbounded JWT lifetime"
    );
    if let Some(nbf) = claims.get("nbf") {
        ensure!(
            nbf.as_i64().context("Invalid nbf")? <= now + 60,
            "JWT not active"
        );
    }
    for field in ["sub", "jti"] {
        ensure!(
            claims[field].as_str().is_some_and(|s| !s.trim().is_empty()),
            "Missing {field}"
        );
    }
    Ok(claims)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn relay_proof_binds_type_authority_time_and_signature() -> Result<()> {
        let key = EnvironmentIdentity::generate();
        let claims = json!({"iss":"https://relay.test", "aud":"t3-env:test", "sub":"user", "iat":1000, "exp":1060, "jti":"id"});
        let token = key.sign(crate::MINT_REQUEST_TYPE, &claims)?;
        assert_eq!(
            verify_relay_proof(
                &key.public_pem()?,
                &token,
                crate::MINT_REQUEST_TYPE,
                "https://relay.test",
                "t3-env:test",
                1000
            )?,
            claims
        );
        assert!(
            verify_relay_proof(
                &key.public_pem()?,
                &token,
                crate::HEALTH_REQUEST_TYPE,
                "https://relay.test",
                "t3-env:test",
                1000
            )
            .is_err()
        );
        assert!(
            verify_relay_proof(
                &key.public_pem()?,
                &token,
                crate::MINT_REQUEST_TYPE,
                "https://other.test",
                "t3-env:test",
                1000
            )
            .is_err()
        );
        assert!(
            verify_relay_proof(
                &key.public_pem()?,
                &token,
                crate::MINT_REQUEST_TYPE,
                "https://relay.test",
                "t3-env:other",
                1000
            )
            .is_err()
        );
        assert!(
            verify_relay_proof(
                &key.public_pem()?,
                &token,
                crate::MINT_REQUEST_TYPE,
                "https://relay.test",
                "t3-env:test",
                1500
            )
            .is_err()
        );
        assert!(
            verify_relay_proof(
                &EnvironmentIdentity::generate().public_pem()?,
                &token,
                crate::MINT_REQUEST_TYPE,
                "https://relay.test",
                "t3-env:test",
                1000
            )
            .is_err()
        );
        Ok(())
    }
}
