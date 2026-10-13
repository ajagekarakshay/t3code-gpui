use anyhow::{Context, Result, bail, ensure};
use sha2::{Digest, Sha256};
use std::{
    io::Cursor,
    path::{Path, PathBuf},
    time::{Duration, SystemTime},
};

const VERSION: &str = "2026.10.0";
const MINIMUM_VERSION: [u32; 3] = [2025, 6, 1];
const MAX_DOWNLOAD_BYTES: usize = 100 * 1024 * 1024;

#[derive(Clone, Copy)]
enum ArchiveKind {
    Binary,
    Tgz,
}

#[derive(Clone)]
struct ReleaseAsset {
    url: String,
    sha256: String,
    archive: ArchiveKind,
}

fn platform() -> (&'static str, &'static str) {
    let platform = match std::env::consts::OS {
        "macos" => "darwin",
        "windows" => "win32",
        other => other,
    };
    let architecture = match std::env::consts::ARCH {
        "aarch64" => "arm64",
        "x86_64" => "x64",
        other => other,
    };
    (platform, architecture)
}

fn executable_name() -> &'static str {
    if cfg!(windows) {
        "cloudflared.exe"
    } else {
        "cloudflared"
    }
}

fn release_asset() -> Option<ReleaseAsset> {
    let (platform, architecture) = platform();
    let (name, checksum, archive) = match (platform, architecture) {
        ("darwin", "arm64") => (
            "cloudflared-darwin-arm64.tgz",
            "a2f79ff7b9420aa537d74af239f376da170bbabeb529aec416002adac6a72e70",
            ArchiveKind::Tgz,
        ),
        ("darwin", "x64") => (
            "cloudflared-darwin-amd64.tgz",
            "903845b81828c8cb3c5d13d816a2de71c06a3da5785469df8eb0e1b736d92f9f",
            ArchiveKind::Tgz,
        ),
        ("linux", "arm64") => (
            "cloudflared-linux-arm64",
            "e6422b9d4f72d3194bc5a38676f13667c06666523217b842a877d72a80b5ac08",
            ArchiveKind::Binary,
        ),
        ("linux", "x64") => (
            "cloudflared-linux-amd64",
            "d33ff2d14475178d2012c2c56beba87389ac5ded27649519f198a7d3134a99db",
            ArchiveKind::Binary,
        ),
        ("win32", "x64" | "arm64") => (
            "cloudflared-windows-amd64.exe",
            "86aee4017b26625cee8484c113558f48effa4cd47f7aa05fcf425604e5d2b23c",
            ArchiveKind::Binary,
        ),
        _ => return None,
    };
    Some(ReleaseAsset {
        url: format!(
            "https://github.com/cloudflare/cloudflared/releases/download/{VERSION}/{name}"
        ),
        sha256: checksum.into(),
        archive,
    })
}

fn managed_path(home: &Path) -> PathBuf {
    let (platform, architecture) = platform();
    home.join("tools/cloudflared")
        .join(VERSION)
        .join(format!("{platform}-{architecture}"))
        .join(executable_name())
}

fn version_parts(value: &str) -> Option<[u32; 3]> {
    value.split_whitespace().find_map(|word| {
        let word =
            word.trim_matches(|character: char| !character.is_ascii_digit() && character != '.');
        let parts = word
            .split('.')
            .map(str::parse::<u32>)
            .collect::<std::result::Result<Vec<_>, _>>()
            .ok()?;
        (parts.len() == 3).then(|| [parts[0], parts[1], parts[2]])
    })
}

async fn probe(path: &Path, expected: Option<&str>) -> Option<String> {
    if !path.is_file() {
        return None;
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt as _;
        if std::fs::metadata(path).ok()?.permissions().mode() & 0o111 == 0 {
            return None;
        }
    }
    let output = tokio::time::timeout(
        Duration::from_secs(10),
        tokio::process::Command::new(path)
            .arg("version")
            .kill_on_drop(true)
            .output(),
    )
    .await
    .ok()?
    .ok()?;
    if !output.status.success() {
        return None;
    }
    let text = format!(
        "{} {}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    let parts = version_parts(&text)?;
    if parts < MINIMUM_VERSION {
        return None;
    }
    let version = format!("{}.{}.{}", parts[0], parts[1], parts[2]);
    expected
        .is_none_or(|expected| expected == version)
        .then_some(version)
}

pub(crate) async fn resolve_existing(home: &Path) -> Option<PathBuf> {
    if let Some(path) = std::env::var_os("T3CODE_CLOUDFLARED_PATH") {
        let path = PathBuf::from(path);
        return probe(&path, None).await.map(|_| path);
    }
    let managed = managed_path(home);
    if probe(&managed, Some(VERSION)).await.is_some() {
        return Some(managed);
    }
    if let Some(path) = std::env::var_os("PATH") {
        for directory in std::env::split_paths(&path) {
            let candidate = directory.join(executable_name());
            if probe(&candidate, None).await.is_some() {
                return Some(candidate);
            }
        }
    }
    None
}

struct InstallLock(PathBuf);
impl Drop for InstallLock {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.0);
    }
}

async fn acquire_lock(path: PathBuf) -> Result<InstallLock> {
    for _ in 0..100 {
        match std::fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&path)
        {
            Ok(_) => return Ok(InstallLock(path)),
            Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {
                let stale = std::fs::metadata(&path)
                    .and_then(|metadata| metadata.modified())
                    .ok()
                    .and_then(|modified| SystemTime::now().duration_since(modified).ok())
                    .is_some_and(|age| age > Duration::from_secs(300));
                if stale {
                    let _ = std::fs::remove_file(&path);
                    continue;
                }
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
            Err(error) => return Err(error.into()),
        }
    }
    bail!("Another relay client installation is still in progress")
}

async fn download(asset: &ReleaseAsset) -> Result<Vec<u8>> {
    let client = reqwest::Client::builder()
        .redirect(reqwest::redirect::Policy::limited(5))
        .timeout(Duration::from_secs(600))
        .build()?;
    let mut response = client.get(&asset.url).send().await?.error_for_status()?;
    if let Some(length) = response.content_length() {
        ensure!(
            length <= MAX_DOWNLOAD_BYTES as u64,
            "Relay client download exceeds the size limit"
        );
    }
    let mut bytes = Vec::new();
    while let Some(chunk) = response.chunk().await? {
        ensure!(
            bytes.len() + chunk.len() <= MAX_DOWNLOAD_BYTES,
            "Relay client download exceeds the size limit"
        );
        bytes.extend_from_slice(&chunk);
    }
    let actual = format!("{:x}", Sha256::digest(&bytes));
    ensure!(
        actual == asset.sha256,
        "Downloaded relay client checksum did not match the pinned release"
    );
    Ok(bytes)
}

async fn install(home: &Path, asset: ReleaseAsset) -> Result<PathBuf> {
    let destination = managed_path(home);
    let directory = destination.parent().context("Invalid relay client path")?;
    tokio::fs::create_dir_all(directory).await?;
    let lock_path = PathBuf::from(format!("{}.lock", destination.display()));
    let _lock = acquire_lock(lock_path).await?;
    if probe(&destination, Some(VERSION)).await.is_some() {
        return Ok(destination);
    }

    let staging = directory.join(format!(".install-{}", uuid::Uuid::new_v4()));
    tokio::fs::create_dir(&staging).await?;
    let result = async {
        let bytes = download(&asset).await?;
        let executable = staging.join(executable_name());
        match asset.archive {
            ArchiveKind::Binary => tokio::fs::write(&executable, bytes).await?,
            ArchiveKind::Tgz => {
                let staging = staging.clone();
                tokio::task::spawn_blocking(move || -> Result<()> {
                    let decoder = flate2::read::GzDecoder::new(Cursor::new(bytes));
                    let mut archive = tar::Archive::new(decoder);
                    archive.unpack(&staging)?;
                    Ok(())
                })
                .await??;
            }
        }
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt as _;
            tokio::fs::set_permissions(&executable, std::fs::Permissions::from_mode(0o755)).await?;
        }
        ensure!(
            probe(&executable, Some(VERSION)).await.is_some(),
            "The downloaded relay client did not report the pinned version"
        );
        let staged = PathBuf::from(format!(
            "{}.{}.tmp",
            destination.display(),
            uuid::Uuid::new_v4()
        ));
        tokio::fs::rename(&executable, &staged).await?;
        if destination.exists() {
            tokio::fs::remove_file(&destination).await?;
        }
        let activated = tokio::fs::rename(&staged, &destination).await;
        if activated.is_err() {
            let _ = tokio::fs::remove_file(&staged).await;
        }
        activated?;
        Ok(destination.clone())
    }
    .await;
    let _ = tokio::fs::remove_dir_all(&staging).await;
    result
}

pub(crate) async fn resolve_or_install(home: &Path) -> Result<PathBuf> {
    if let Some(path) = resolve_existing(home).await {
        return Ok(path);
    }
    ensure!(
        std::env::var_os("T3CODE_CLOUDFLARED_PATH").is_none(),
        "T3CODE_CLOUDFLARED_PATH must point to cloudflared 2025.6.1 or newer"
    );
    let asset =
        release_asset().context("No managed relay client is published for this platform")?;
    install(home, asset).await
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::{Router, routing::get};

    async fn asset_server(bytes: Vec<u8>) -> Result<(String, tokio::task::JoinHandle<()>)> {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
        let address = listener.local_addr()?;
        let router = Router::new().route(
            "/cloudflared",
            get(move || {
                let bytes = bytes.clone();
                async move { bytes }
            }),
        );
        let task = tokio::spawn(async move { axum::serve(listener, router).await.unwrap() });
        Ok((format!("http://{address}/cloudflared"), task))
    }

    #[tokio::test]
    async fn refuses_a_download_with_the_wrong_checksum() -> Result<()> {
        let directory = tempfile::tempdir()?;
        let (url, task) = asset_server(b"not cloudflared".to_vec()).await?;
        let error = install(
            directory.path(),
            ReleaseAsset {
                url,
                sha256: "00".into(),
                archive: ArchiveKind::Binary,
            },
        )
        .await
        .unwrap_err();
        task.abort();
        let _ = task.await;
        assert!(error.to_string().contains("checksum"));
        assert!(!managed_path(directory.path()).exists());
        Ok(())
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn atomically_installs_and_reuses_a_verified_binary() -> Result<()> {
        let directory = tempfile::tempdir()?;
        let script = format!("#!/bin/sh\necho 'cloudflared version {VERSION}'\n").into_bytes();
        let checksum = format!("{:x}", Sha256::digest(&script));
        let (url, task) = asset_server(script).await?;
        let asset = ReleaseAsset {
            url,
            sha256: checksum,
            archive: ArchiveKind::Binary,
        };
        let installed = install(directory.path(), asset.clone()).await?;
        assert_eq!(
            probe(&installed, Some(VERSION)).await.as_deref(),
            Some(VERSION)
        );
        assert_eq!(install(directory.path(), asset).await?, installed);
        task.abort();
        let _ = task.await;
        assert!(!PathBuf::from(format!("{}.lock", installed.display())).exists());
        Ok(())
    }
}
