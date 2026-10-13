//! A connector peer that stays alive until its owning test terminates it.
fn main() {
    if std::env::args().nth(1).as_deref() == Some("version") {
        println!("cloudflared version 2026.10.0");
        return;
    }
    assert_eq!(
        std::env::args().skip(1).collect::<Vec<_>>(),
        [
            "tunnel",
            "--no-autoupdate",
            "--loglevel",
            "info",
            "--output",
            "default",
            "run"
        ]
    );
    match std::env::var("TUNNEL_TOKEN").as_deref() {
        Ok("fixture-connector-token") => eprintln!("INF Registered tunnel connection connIndex=0"),
        Ok("fixture-rejected-token") => {
            for _ in 0..4 {
                eprintln!(
                    "ERR Register tunnel error from server side error=\"Unauthorized: fixture\""
                );
            }
        }
        _ => panic!("Token was not passed through the environment"),
    }
    loop {
        std::thread::park();
    }
}
