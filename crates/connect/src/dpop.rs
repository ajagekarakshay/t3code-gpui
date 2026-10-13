use anyhow::{Context, Result, ensure};
use base64::{Engine as _, engine::general_purpose::URL_SAFE_NO_PAD as B64};
use p256::ecdsa::{
    Signature, SigningKey, VerifyingKey,
    signature::{Signer, Verifier},
};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};

/// Client proof-of-possession key; the private key never travels to the relay.
pub struct DpopKey {
    key: SigningKey,
}
impl DpopKey {
    pub fn generate() -> Self {
        Self {
            key: SigningKey::random(&mut rand::rngs::OsRng),
        }
    }
    /// Only persist this material in the platform credential store.
    pub fn private_key_bytes(&self) -> [u8; 32] {
        self.key.to_bytes().into()
    }
    pub fn from_private_key_bytes(bytes: &[u8]) -> Result<Self> {
        Ok(Self {
            key: SigningKey::from_slice(bytes)?,
        })
    }
    pub fn public_jwk(&self) -> Value {
        let point = self.key.verifying_key().to_encoded_point(false);
        json!({"kty":"EC","crv":"P-256","x":B64.encode(point.x().expect("uncompressed point")),"y":B64.encode(point.y().expect("uncompressed point"))})
    }
    pub fn thumbprint(&self) -> Result<String> {
        let jwk = self.public_jwk();
        Ok(B64.encode(Sha256::digest(serde_json::to_vec(
            &json!({"crv":"P-256","kty":"EC","x":jwk["x"],"y":jwk["y"]}),
        )?)))
    }
    pub fn sign(
        &self,
        method: &str,
        url: &str,
        now: i64,
        access_token: Option<&str>,
    ) -> Result<String> {
        let mut url = url::Url::parse(url)?;
        url.set_query(None);
        url.set_fragment(None);
        let mut claims = json!({"htm":method.to_uppercase(),"htu":url,"iat":now,"jti":uuid::Uuid::new_v4().to_string()});
        if let Some(access) = access_token {
            claims["ath"] = json!(B64.encode(Sha256::digest(access.as_bytes())));
        }
        let input = format!(
            "{}.{}",
            B64.encode(serde_json::to_vec(
                &json!({"typ":"dpop+jwt","alg":"ES256","jwk":self.public_jwk()})
            )?),
            B64.encode(serde_json::to_vec(&claims)?)
        );
        let signature: Signature = self.key.sign(input.as_bytes());
        Ok(format!("{input}.{}", B64.encode(signature.to_bytes())))
    }
}

/// Verified proof identity. Persist `(thumbprint, jti)` atomically before granting access.
pub struct DpopProof {
    thumbprint: String,
    jti: String,
}
impl DpopProof {
    pub fn thumbprint(&self) -> &str {
        &self.thumbprint
    }
    pub fn replay_key(&self) -> String {
        format!("{}:{}", self.thumbprint, self.jti)
    }
}

pub fn verify_dpop(
    token: &str,
    method: &str,
    url: &str,
    now: i64,
    expected_thumbprint: Option<&str>,
    access_token: Option<&str>,
) -> Result<DpopProof> {
    let parts: Vec<_> = token.split('.').collect();
    ensure!(parts.len() == 3, "Malformed DPoP proof");
    let header: Value = serde_json::from_slice(&B64.decode(parts[0])?)?;
    ensure!(
        header["typ"] == "dpop+jwt" && header["alg"] == "ES256",
        "Invalid DPoP header"
    );
    ensure!(
        header.get("crit").is_none(),
        "Unsupported critical DPoP header"
    );
    let jwk = &header["jwk"];
    ensure!(
        jwk["kty"] == "EC" && jwk["crv"] == "P-256" && jwk.get("d").is_none(),
        "Invalid public DPoP key"
    );
    let x = jwk["x"].as_str().context("Missing key x")?;
    let y = jwk["y"].as_str().context("Missing key y")?;
    // RFC 7638 canonical member order, matching shared/dpop.ts.
    let canonical = serde_json::to_vec(&json!({"crv":"P-256", "kty":"EC", "x":x, "y":y}))?;
    let thumbprint = B64.encode(Sha256::digest(&canonical));
    if let Some(expected) = expected_thumbprint {
        ensure!(expected == thumbprint, "DPoP key mismatch");
    }
    let mut bytes = vec![4];
    let x = B64.decode(x)?;
    let y = B64.decode(y)?;
    ensure!(x.len() == 32 && y.len() == 32, "Invalid key coordinates");
    bytes.extend(x);
    bytes.extend(y);
    let key = VerifyingKey::from_sec1_bytes(&bytes)?;
    let signature = Signature::from_slice(&B64.decode(parts[2])?)?;
    key.verify(format!("{}.{}", parts[0], parts[1]).as_bytes(), &signature)?;
    let claims: Value = serde_json::from_slice(&B64.decode(parts[1])?)?;
    let mut url = url::Url::parse(url)?;
    ensure!(
        matches!(url.scheme(), "http" | "https"),
        "Invalid DPoP URL scheme"
    );
    url.set_query(None);
    url.set_fragment(None);
    ensure!(
        claims["htm"]
            .as_str()
            .is_some_and(|v| v.eq_ignore_ascii_case(method)),
        "DPoP method mismatch"
    );
    ensure!(claims["htu"] == url.as_str(), "DPoP URL mismatch");
    let issued = claims["iat"].as_i64().context("Missing DPoP iat")?;
    ensure!(
        issued <= now + 60 && issued >= now - 300,
        "DPoP time window"
    );
    if let Some(access) = access_token {
        ensure!(
            claims["ath"] == B64.encode(Sha256::digest(access.as_bytes())),
            "DPoP access token mismatch"
        );
    }
    let jti = claims["jti"]
        .as_str()
        .filter(|s| !s.trim().is_empty())
        .context("Missing DPoP jti")?
        .into();
    Ok(DpopProof { thumbprint, jti })
}
