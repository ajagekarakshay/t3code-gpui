//! Native environment authentication and Effect RPC transport.

mod rpc;
mod session;

pub use rpc::{Client, Subscription};
pub use session::{PairingTarget, SavedSession};
