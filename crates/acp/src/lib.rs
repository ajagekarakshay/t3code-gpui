//! A single ACP integration shared by every supported provider.

pub use agent_client_protocol as protocol;
use serde::{Deserialize, Serialize};
use std::{collections::BTreeMap, path::PathBuf};

mod session;
pub use session::{AgentEvent, Session, SessionCommand};

/// A literal executable and argument vector, never evaluated by a shell.
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct AgentCommand {
    executable: PathBuf,
    #[serde(default)]
    arguments: Vec<String>,
    #[serde(default)]
    environment: BTreeMap<String, String>,
}

impl AgentCommand {
    pub fn new(executable: impl Into<PathBuf>) -> Self {
        Self {
            executable: executable.into(),
            arguments: Vec::new(),
            environment: BTreeMap::new(),
        }
    }

    pub fn arguments(mut self, arguments: impl IntoIterator<Item = impl Into<String>>) -> Self {
        self.arguments = arguments.into_iter().map(Into::into).collect();
        self
    }

    pub fn environment(mut self, environment: BTreeMap<String, String>) -> Self {
        self.environment = environment;
        self
    }

    pub fn executable(&self) -> &std::path::Path {
        &self.executable
    }

    pub(crate) fn transport(&self) -> protocol::AcpAgent {
        protocol::AcpAgent::new(
            protocol::AcpAgentConfig::new(self.executable.clone())
                .args(self.arguments.clone())
                .envs(self.environment.clone()),
        )
    }
}

/// The executable can be overridden without introducing provider-specific protocols.
pub fn supported_agents() -> BTreeMap<String, AgentCommand> {
    BTreeMap::from([
        ("claude-code".into(), AgentCommand::new("claude-agent-acp")),
        ("codex".into(), AgentCommand::new("codex-acp")),
    ])
}
