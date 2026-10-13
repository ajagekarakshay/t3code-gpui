# T3 Code Native

A Rust desktop and environment-server rewrite of T3 Code, using [GPUI Kit](https://gpui-kit.com) and [Agent Client Protocol](https://agentclientprotocol.com). Claude Code and Codex share one ACP integration.

**The migration is in progress. This is not yet a feature-complete replacement for the Electron application.** The native path currently uses its own database and does not advertise full orchestration V2 compatibility to existing clients. The original implementation remains available for completing and checking parity, including T3 Connect and mobile compatibility. iOS and Android source is unchanged.

## Run the native desktop

Use Rust 1.96 and your platform's GPUI build dependencies. On macOS, install Xcode and its command line tools. The [native CI workflow](.github/workflows/native.yml) lists the Linux build packages.

Install and authenticate Claude Code or Codex, then install their maintained ACP adapters:

```sh
npm install --global @agentclientprotocol/claude-agent-acp @agentclientprotocol/codex-acp
cargo run -p t3-desktop -- --home-dir .t3/rust
```

The adapters must be available as `claude-agent-acp` and `codex-acp` on `PATH`. The desktop and server are Rust; the separately maintained adapters have their own runtime dependencies. ACP provides a common protocol while each agent retains its own capabilities and behavior.

Add a project directory, create a thread, and send a prompt. The desktop runs its Rust environment server on an available loopback port. Agent configuration comes from the active ACP session. Command/Control-K opens the command palette; Command/Control-Enter sends a prompt.

Settings can pair another native environment from a direct or hosted T3 pairing link. Sessions are proof-bound to the desktop, stored in the operating system credential store, and reconnect automatically. The desktop resumes from committed event sequence numbers so reconnecting does not drop streamed agent updates.

Copy `.env.example` to `.env` to enable the native T3 Connect account controls. Settings uses Clerk's device authorization flow, keeps the refreshable account credential in the operating system credential store, and links the local Rust environment through the existing relay challenge and proof protocol. When a managed tunnel is linked, the Rust server uses a compatible `cloudflared` from `T3CODE_CLOUDFLARED_PATH` or `PATH`. On a clean machine it downloads the pinned release for the platform, verifies its SHA-256 checksum, validates the reported version, and activates it atomically under the Rust data directory.

The standalone development server is also available:

```sh
cargo run -p t3-server -- --home-dir .t3/rust --listen 127.0.0.1:3773
```

To print a one-use link for another native desktop, add `--pair`. When listening on a LAN address or through a reverse proxy, pass the address clients actually use:

```sh
cargo run -p t3-server -- --home-dir .t3/rust --listen 0.0.0.0:3773 --pair --pair-origin https://machine.example.com
```

Use a different data directory when running it alongside the desktop. Each Rust data directory has one server owner. The migration rejects the live `~/.t3` tree, and does not import or modify the existing installation's database.

## Verify the native code

```sh
cargo test -p t3-acp -p t3-connect -p t3-client -p t3-server --features t3-acp/test-support,t3-connect/test-support,t3-server/test-support --locked
cargo test -p t3-desktop --locked
cargo clippy -p t3-acp -p t3-connect -p t3-client -p t3-server -p t3-desktop --all-targets --features t3-acp/test-support,t3-connect/test-support,t3-server/test-support --locked -- -D warnings
cargo build -p t3-desktop -p t3-server --locked
```

ACP and tunnel tests run deterministic subprocess peers. Connect tests use synthetic signing keys and isolated local servers. Desktop interaction tests run GPUI Kit components in headless windows. These checks do not establish live relay, real-agent, or packaged-application parity.

Vendored references are preserved under `backup/references`. The [upstream README](backup/README-upstream.md) describes the original application and its installation procedures.
