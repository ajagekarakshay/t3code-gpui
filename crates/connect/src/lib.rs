//! T3 Connect's existing environment proof and DPoP wire formats.

mod dpop;
mod identity;
mod tunnel;
pub use dpop::{DpopKey, DpopProof, verify_dpop};
pub use identity::{EnvironmentIdentity, validate_relay_key, verify_relay_proof};
pub use tunnel::{Tunnel, TunnelConfiguration, TunnelState};

pub const LINK_PROOF_TYPE: &str = "t3-env-link+jwt";
pub const MINT_REQUEST_TYPE: &str = "t3-cloud-mint+jwt";
pub const HEALTH_REQUEST_TYPE: &str = "t3-cloud-health+jwt";
pub const MINT_RESPONSE_TYPE: &str = "t3-env-mint+jwt";
pub const HEALTH_RESPONSE_TYPE: &str = "t3-env-health+jwt";
pub const RECOVERY_PROOF_TYPE: &str = "t3-env-managed-tunnel-recovery+jwt";

pub fn normalize_issuer(issuer: &str) -> String {
    issuer.trim().trim_end_matches('/').into()
}
