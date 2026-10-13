use crate::store::Store;
use anyhow::{Context, Result, ensure};
use base64::{Engine as _, engine::general_purpose::URL_SAFE_NO_PAD as B64};
use rand::RngCore;
use rusqlite::{OptionalExtension, params};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};

pub(crate) const STANDARD_SCOPES: &[&str] = &[
    "orchestration:read",
    "orchestration:operate",
    "settings:write",
    "providers:manage",
    "environment:maintain",
    "preview:operate",
    "diagnostics:read",
    "terminal:read",
    "terminal:operate",
    "source-control:write",
    "filesystem:read",
    "filesystem:write",
    "relay:read",
];
pub(crate) const ADMIN_SCOPES: &[&str] = &["access:read", "access:write", "relay:write"];

pub(crate) fn random_token() -> String {
    let mut bytes = [0_u8; 32];
    rand::rngs::OsRng.fill_bytes(&mut bytes);
    B64.encode(bytes)
}
fn hash(token: &str) -> String {
    B64.encode(Sha256::digest(token.as_bytes()))
}
pub(crate) fn epoch() -> i64 {
    chrono::Utc::now().timestamp()
}
pub(crate) fn iso(epoch: i64) -> String {
    chrono::DateTime::from_timestamp(epoch, 0)
        .expect("valid timestamp")
        .to_rfc3339_opts(chrono::SecondsFormat::Millis, true)
}

#[derive(Clone)]
pub(crate) struct Session {
    id: String,
    scopes: Vec<String>,
    proof_key: Option<String>,
}
impl Session {
    pub(crate) fn id(&self) -> &str {
        &self.id
    }
    pub(crate) fn require(&self, scope: &str) -> Result<()> {
        ensure!(
            self.scopes.iter().any(|v| v == scope),
            "Missing permission: {scope}"
        );
        Ok(())
    }
    pub(crate) fn metadata(&self) -> Value {
        json!({"authenticated":true,"sessionId":self.id,"scopes":self.scopes,"permissions":self.scopes,"method":if self.proof_key.is_some(){"dpop-access-token"}else{"bearer-access-token"}})
    }
}

impl Store {
    pub(crate) fn grant(
        &self,
        scopes: &[String],
        proof_key: Option<&str>,
        ttl: i64,
        reusable: bool,
    ) -> Result<Value> {
        ensure!(ttl > 0 && ttl <= 86400, "Invalid credential lifetime");
        let token = random_token();
        let expires = epoch() + ttl;
        self.lock()?.execute(
            "INSERT INTO grants(hash,scopes,proof_key,expires,reusable) VALUES (?,?,?,?,?)",
            params![
                hash(&token),
                serde_json::to_string(scopes)?,
                proof_key,
                expires,
                reusable
            ],
        )?;
        Ok(
            json!({"id":uuid::Uuid::new_v4().to_string(),"credential":token,"expiresAt":iso(expires),"scopes":scopes,"permissions":scopes}),
        )
    }

    pub(crate) fn delegate(&self, session: &Session, scopes: &[String]) -> Result<Value> {
        session.require("access:write")?;
        ensure!(!scopes.is_empty(), "At least one permission is required");
        for scope in scopes {
            session.require(scope)?;
        }
        self.grant(scopes, None, 120, false)
    }

    pub(crate) fn exchange(&self, input: &Value, proof: Option<&str>, url: &str) -> Result<Value> {
        ensure!(
            input["grant_type"] == "urn:ietf:params:oauth:grant-type:token-exchange"
                && input["subject_token_type"]
                    == "urn:t3:params:oauth:token-type:environment-bootstrap"
                && input["requested_token_type"] == "urn:ietf:params:oauth:token-type:access_token",
            "Unsupported token exchange"
        );
        let token = input["subject_token"]
            .as_str()
            .context("Missing bootstrap credential")?;
        let mut connection = self.lock()?;
        let tx = connection.transaction()?;
        let grant: Option<(String, Option<String>, i64, bool)> = tx
            .query_row(
                "SELECT scopes,proof_key,expires,reusable FROM grants WHERE hash=?",
                [hash(token)],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
            )
            .optional()?;
        let (scopes, mut key, expires, reusable) = grant.context("Invalid bootstrap credential")?;
        ensure!(expires > epoch(), "Bootstrap credential expired");
        let granted: Vec<String> = serde_json::from_str(&scopes)?;
        let requested: Vec<String> = match input.get("scope") {
            Some(value) => value
                .as_str()
                .context("Invalid scope")?
                .split_whitespace()
                .map(str::to_owned)
                .collect(),
            None => granted.clone(),
        };
        ensure!(
            !requested.is_empty() && requested.iter().all(|scope| granted.contains(scope)),
            "Requested permissions exceed grant"
        );
        if key.is_some() || proof.is_some() {
            let verified = t3_connect::verify_dpop(
                proof.context("DPoP proof required")?,
                "POST",
                url,
                epoch(),
                key.as_deref(),
                None,
            )?;
            consume_replay(
                &tx,
                &format!("dpop:{}", verified.replay_key()),
                epoch() + 360,
            )?;
            ensure!(
                key.as_deref()
                    .is_none_or(|expected| expected == verified.thumbprint()),
                "DPoP key mismatch"
            );
            key = Some(verified.thumbprint().to_owned());
        }
        if !reusable {
            tx.execute("DELETE FROM grants WHERE hash=?", [hash(token)])?;
        }
        let access = random_token();
        let duration = 30 * 86400;
        tx.execute(
            "INSERT INTO sessions(id,hash,scopes,proof_key,expires) VALUES (?,?,?,?,?)",
            params![
                uuid::Uuid::new_v4().to_string(),
                hash(&access),
                serde_json::to_string(&requested)?,
                key,
                epoch() + duration
            ],
        )?;
        tx.commit()?;
        Ok(
            json!({"access_token":access,"issued_token_type":"urn:ietf:params:oauth:token-type:access_token","token_type":if key.is_some(){"DPoP"}else{"Bearer"},"expires_in":duration,"scope":requested.join(" ")}),
        )
    }

    pub(crate) fn authenticate(
        &self,
        authorization: Option<&str>,
        proof: Option<&str>,
        method: &str,
        url: &str,
    ) -> Result<Session> {
        let (scheme, token) = authorization
            .context("Authentication required")?
            .split_once(' ')
            .context("Invalid authorization")?;
        ensure!(
            matches!(scheme, "Bearer" | "DPoP"),
            "Invalid authorization scheme"
        );
        let mut connection = self.lock()?;
        let tx = connection.transaction()?;
        let row: Option<(String, String, Option<String>, i64)> = tx
            .query_row(
                "SELECT id,scopes,proof_key,expires FROM sessions WHERE hash=?",
                [hash(token)],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
            )
            .optional()?;
        let (id, scopes, key, expires) = row.context("Invalid access token")?;
        ensure!(expires > epoch(), "Session expired");
        if let Some(key) = &key {
            ensure!(
                scheme == "DPoP",
                "Proof-bound token requires DPoP authorization"
            );
            let verified = t3_connect::verify_dpop(
                proof.context("DPoP proof required")?,
                method,
                url,
                epoch(),
                Some(key),
                Some(token),
            )?;
            consume_replay(
                &tx,
                &format!("dpop:{}", verified.replay_key()),
                epoch() + 360,
            )?;
        } else {
            ensure!(scheme == "Bearer", "Token is not bound to a proof key");
        }
        tx.commit()?;
        Ok(Session {
            id,
            scopes: serde_json::from_str(&scopes)?,
            proof_key: key,
        })
    }

    pub(crate) fn session_valid(&self, id: &str) -> Result<bool> {
        Ok(self.lock()?.query_row(
            "SELECT EXISTS(SELECT 1 FROM sessions WHERE id=? AND expires>?)",
            params![id, epoch()],
            |row| row.get(0),
        )?)
    }

    pub(crate) fn ticket(&self, session: &Session) -> Result<Value> {
        ensure!(self.session_valid(session.id())?, "Session revoked");
        let ticket = random_token();
        let expires = epoch() + 30;
        self.lock()?.execute(
            "INSERT INTO tickets(hash,session_id,expires) VALUES (?,?,?)",
            params![hash(&ticket), session.id, expires],
        )?;
        Ok(json!({"ticket":ticket,"expiresAt":iso(expires)}))
    }

    pub(crate) fn redeem_ticket(&self, ticket: &str) -> Result<Session> {
        let mut connection = self.lock()?;
        let tx = connection.transaction()?;
        let row:Option<(String,String,Option<String>)>=tx.query_row("SELECT s.id,s.scopes,s.proof_key FROM tickets t JOIN sessions s ON s.id=t.session_id WHERE t.hash=? AND t.expires>? AND s.expires>?",params![hash(ticket),epoch(),epoch()],|row|Ok((row.get(0)?,row.get(1)?,row.get(2)?))).optional()?;
        let (id, scopes, key) = row.context("Invalid or expired WebSocket ticket")?;
        tx.execute("DELETE FROM tickets WHERE hash=?", [hash(ticket)])?;
        tx.commit()?;
        Ok(Session {
            id,
            scopes: serde_json::from_str(&scopes)?,
            proof_key: key,
        })
    }

    pub(crate) fn consume_relay_proof(&self, operation: &str, claims: &Value) -> Result<()> {
        let mut connection = self.lock()?;
        let tx = connection.transaction()?;
        for field in ["jti", "nonce"] {
            let value = claims[field]
                .as_str()
                .filter(|v| !v.trim().is_empty())
                .context("Invalid replay identity")?;
            consume_replay(
                &tx,
                &format!("relay:{operation}:{field}:{value}"),
                epoch() + 360,
            )?;
        }
        tx.commit()?;
        Ok(())
    }
}

fn consume_replay(tx: &rusqlite::Transaction<'_>, key: &str, expires: i64) -> Result<()> {
    tx.execute("DELETE FROM replay_guards WHERE expires<=?", [epoch()])?;
    tx.execute(
        "INSERT INTO replay_guards(key,expires) VALUES (?,?)",
        params![key, expires],
    )
    .context("Proof replayed")?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    fn store() -> (tempfile::TempDir, Store) {
        let directory = tempfile::tempdir().unwrap();
        let store = Store::open(&directory.path().join("state.sqlite")).unwrap();
        (directory, store)
    }
    fn exchange_input(credential: &str) -> Value {
        json!({"grant_type":"urn:ietf:params:oauth:grant-type:token-exchange","subject_token_type":"urn:t3:params:oauth:token-type:environment-bootstrap","requested_token_type":"urn:ietf:params:oauth:token-type:access_token","subject_token":credential})
    }

    #[test]
    fn pairing_is_one_time_and_scopes_cannot_be_widened() -> Result<()> {
        let (_directory, store) = store();
        let grant = store.grant(&["orchestration:read".into()], None, 120, false)?;
        let input = exchange_input(grant["credential"].as_str().unwrap());
        let mut widened = input.clone();
        widened["scope"] = json!("orchestration:operate");
        assert!(
            store
                .exchange(&widened, None, "http://localhost/oauth/token")
                .is_err()
        );
        let token = store.exchange(&input, None, "http://localhost/oauth/token")?;
        assert!(
            store
                .exchange(&input, None, "http://localhost/oauth/token")
                .is_err()
        );
        let session = store.authenticate(
            Some(&format!(
                "Bearer {}",
                token["access_token"].as_str().unwrap()
            )),
            None,
            "GET",
            "http://localhost/api",
        )?;
        session.require("orchestration:read")?;
        assert!(session.require("orchestration:operate").is_err());
        assert!(
            store
                .delegate(&session, &["orchestration:operate".into()])
                .is_err()
        );
        Ok(())
    }

    #[test]
    fn bound_credentials_require_the_proof_key_and_prevent_bearer_downgrades() -> Result<()> {
        let (_directory, store) = store();
        let key = t3_connect::DpopKey::generate();
        let url = "https://environment.test/oauth/token";
        let grant = store.grant(
            &["orchestration:read".into()],
            Some(&key.thumbprint()?),
            120,
            false,
        )?;
        let input = exchange_input(grant["credential"].as_str().unwrap());
        assert!(store.exchange(&input, None, url).is_err());
        let other = t3_connect::DpopKey::generate();
        assert!(
            store
                .exchange(&input, Some(&other.sign("POST", url, epoch(), None)?), url)
                .is_err()
        );
        let token = store.exchange(&input, Some(&key.sign("POST", url, epoch(), None)?), url)?;
        let access = token["access_token"].as_str().unwrap();
        let resource = "https://environment.test/api/auth/session";
        assert!(
            store
                .authenticate(Some(&format!("Bearer {access}")), None, "GET", resource)
                .is_err()
        );
        assert!(
            store
                .authenticate(Some(&format!("DPoP {access}")), None, "GET", resource)
                .is_err()
        );
        let proof = key.sign("GET", resource, epoch(), Some(access))?;
        let session = store.authenticate(
            Some(&format!("DPoP {access}")),
            Some(&proof),
            "GET",
            resource,
        )?;
        assert!(
            store
                .authenticate(
                    Some(&format!("DPoP {access}")),
                    Some(&proof),
                    "GET",
                    resource
                )
                .is_err()
        );
        let ticket = store.ticket(&session)?;
        let value = ticket["ticket"].as_str().unwrap();
        assert_eq!(store.redeem_ticket(value)?.id(), session.id());
        assert!(store.redeem_ticket(value).is_err());
        Ok(())
    }

    #[test]
    fn relay_nonce_and_jti_guards_commit_together() -> Result<()> {
        let (_directory, store) = store();
        store.consume_relay_proof("mint", &json!({"jti":"original","nonce":"nonce"}))?;
        assert!(
            store
                .consume_relay_proof("mint", &json!({"jti":"fresh","nonce":"nonce"}))
                .is_err()
        );
        // The failed nonce check must not burn the fresh jti.
        store.consume_relay_proof("mint", &json!({"jti":"fresh","nonce":"fresh-nonce"}))?;
        Ok(())
    }
}
