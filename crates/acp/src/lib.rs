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
    let npx = if cfg!(windows) { "npx.cmd" } else { "npx" };
    BTreeMap::from([
        (
            "claude-code".into(),
            AgentCommand::new(npx).arguments([
                "--yes",
                "--package",
                "@agentclientprotocol/claude-agent-acp@0.89.1",
                "claude-agent-acp",
            ]),
        ),
        (
            "codex".into(),
            AgentCommand::new(npx).arguments([
                "--yes",
                "--package",
                "@agentclientprotocol/codex-acp@2.2.2",
                "codex-acp",
            ]),
        ),
    ])
}

/// Resolve the literal command using the host's normal executable search rules.
pub fn command_available(command: &AgentCommand) -> bool {
    let executable = command.executable();
    if executable.is_absolute() || executable.components().count() > 1 {
        return executable.is_file();
    }
    let Some(path) = std::env::var_os("PATH") else {
        return false;
    };
    let extensions = if cfg!(windows) {
        std::env::var_os("PATHEXT")
            .map(|value| {
                value
                    .to_string_lossy()
                    .split(';')
                    .map(str::to_owned)
                    .collect::<Vec<_>>()
            })
            .unwrap_or_else(|| vec![".COM".into(), ".EXE".into(), ".BAT".into(), ".CMD".into()])
    } else {
        vec![String::new()]
    };
    std::env::split_paths(&path).any(|directory| {
        directory.join(executable).is_file()
            || extensions.iter().any(|extension| {
                let mut name = executable.as_os_str().to_os_string();
                name.push(extension);
                directory.join(name).is_file()
            })
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn maintained_agents_use_pinned_registry_packages() {
        let agents = supported_agents();
        let claude = serde_json::to_value(&agents["claude-code"]).unwrap();
        let codex = serde_json::to_value(&agents["codex"]).unwrap();
        assert_eq!(claude["executable"], "npx");
        assert!(
            claude["arguments"][2]
                .as_str()
                .unwrap()
                .starts_with("@agentclientprotocol/claude-agent-acp@")
        );
        assert!(
            codex["arguments"][2]
                .as_str()
                .unwrap()
                .starts_with("@agentclientprotocol/codex-acp@")
        );
    }
}
