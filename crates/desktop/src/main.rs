use clap::Parser;
use gpui_kit::*;
use std::{net::SocketAddr, path::PathBuf, sync::Arc};
use t3_server::{Server, ServerOptions};

mod backend;
mod workspace;

#[derive(Parser)]
#[command(about = "T3 Code native Rust desktop")]
struct Arguments {
    #[arg(long, default_value = ".t3/rust")]
    home_dir: PathBuf,
    #[arg(long, default_value = "127.0.0.1:0")]
    listen: SocketAddr,
}

fn main() -> anyhow::Result<()> {
    let arguments = Arguments::parse();
    let runtime = Arc::new(tokio::runtime::Runtime::new()?);
    let server = Server::open(ServerOptions::new(arguments.home_dir))?;
    let listener = runtime.block_on(tokio::net::TcpListener::bind(arguments.listen))?;
    let address = listener.local_addr()?;
    runtime.block_on(server.start_connect(address))?;
    let router = server.router();
    runtime.spawn(async move {
        if let Err(error) = axum::serve(listener, router).await {
            eprintln!("Environment server stopped: {error}");
        }
    });
    let shutdown = server.clone();
    let shutdown_runtime = runtime.clone();
    application().with_assets(assets::Assets).run(move |cx| {
        init(cx);
        workspace::bind_keys(cx);
        let runtime = runtime.clone();
        open_window(
            WindowOptions {
                // Physical window-system boundary; application content uses rem units.
                window_bounds: Some(WindowBounds::Windowed(Bounds::centered(
                    None,
                    size(px(1200.), px(800.)),
                    cx,
                ))),
                window_min_size: Some(size(px(800.), px(560.))),
                ..Default::default()
            },
            cx,
            move |window, cx| {
                cx.new(|cx| {
                    workspace::Workspace::new(server.clone(), runtime.clone(), address, window, cx)
                })
            },
        )
        .expect("Could not open T3 Code");
        cx.on_app_quit(move |_| {
            let server = shutdown.clone();
            let runtime = shutdown_runtime.clone();
            async move {
                let _ = runtime
                    .spawn(async move {
                        server.shutdown().await;
                    })
                    .await;
            }
        })
        .detach();
        cx.activate(true);
    });
    Ok(())
}
