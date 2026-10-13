use clap::Parser;
use std::{net::SocketAddr, path::PathBuf};
use t3_server::{Server, ServerOptions};

#[derive(Parser)]
#[command(about = "T3 Code Rust environment server")]
struct Arguments {
    #[arg(long, default_value = "127.0.0.1:3773")]
    listen: SocketAddr,
    /// A separate Rust data directory is required during migration.
    #[arg(long, default_value = ".t3/rust")]
    home_dir: PathBuf,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .init();
    let arguments = Arguments::parse();
    let server = Server::open(ServerOptions::new(arguments.home_dir))?;
    let listener = tokio::net::TcpListener::bind(arguments.listen).await?;
    server.start_connect(listener.local_addr()?).await?;
    eprintln!(
        "T3 Rust server listening at http://{}",
        listener.local_addr()?
    );
    axum::serve(listener, server.router())
        .with_graceful_shutdown(async {
            let _ = tokio::signal::ctrl_c().await;
        })
        .await?;
    server.shutdown().await;
    Ok(())
}
