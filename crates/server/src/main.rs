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
    /// Print a one-use pairing link valid for two minutes.
    #[arg(long)]
    pair: bool,
    /// Public HTTP(S) origin to embed in `--pair` output.
    #[arg(long, requires = "pair")]
    pair_origin: Option<String>,
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
    if arguments.pair {
        let address = listener.local_addr()?;
        let origin = arguments.pair_origin.unwrap_or_else(|| {
            let host = if address.ip().is_unspecified() {
                "127.0.0.1".to_owned()
            } else {
                address.ip().to_string()
            };
            format!("http://{host}:{}", address.port())
        });
        println!("{}", server.pairing_url(&origin)?);
    }
    axum::serve(listener, server.router())
        .with_graceful_shutdown(async {
            let _ = tokio::signal::ctrl_c().await;
        })
        .await?;
    server.shutdown().await;
    Ok(())
}
