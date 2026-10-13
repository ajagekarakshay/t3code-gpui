use anyhow::{Context, Result, ensure};
use serde_json::{Map, Value, json};
use std::{
    collections::BTreeMap,
    path::{Path, PathBuf},
    process::Output,
};
use tokio::process::Command;

async fn cwd(input: &Value) -> Result<PathBuf> {
    let path = PathBuf::from(crate::store::required(input, "cwd")?);
    let path = tokio::fs::canonicalize(path)
        .await
        .context("Source control directory does not exist")?;
    ensure!(path.is_dir(), "Source control path must be a directory");
    Ok(path)
}

async fn git(directory: &Path, args: &[&str]) -> Result<Output> {
    Command::new("git")
        .args(args)
        .current_dir(directory)
        .env("GIT_TERMINAL_PROMPT", "0")
        .output()
        .await
        .context("Git is not installed")
}

async fn checked_git(directory: &Path, operation: &str, args: &[&str]) -> Result<String> {
    let output = git(directory, args).await?;
    ensure!(
        output.status.success(),
        "Git {operation} failed (exit {}, {} bytes of diagnostics)",
        output.status.code().unwrap_or(-1),
        output.stderr.len()
    );
    String::from_utf8(output.stdout).context("Git returned non-UTF-8 output")
}

async fn is_repo(directory: &Path) -> Result<bool> {
    let output = git(directory, &["rev-parse", "--is-inside-work-tree"]).await?;
    Ok(output.status.success() && output.stdout.starts_with(b"true"))
}

async fn current_ref(directory: &Path) -> Result<Option<String>> {
    let output = git(directory, &["symbolic-ref", "--quiet", "--short", "HEAD"]).await?;
    if !output.status.success() {
        return Ok(None);
    }
    let value = String::from_utf8(output.stdout)?.trim().to_owned();
    Ok((!value.is_empty()).then_some(value))
}

async fn has_primary_remote(directory: &Path) -> Result<bool> {
    let output = git(directory, &["remote", "get-url", "origin"]).await?;
    Ok(output.status.success())
}

async fn default_ref(directory: &Path) -> Result<Option<String>> {
    let output = git(
        directory,
        &[
            "symbolic-ref",
            "--quiet",
            "--short",
            "refs/remotes/origin/HEAD",
        ],
    )
    .await?;
    if output.status.success() {
        let value = String::from_utf8(output.stdout)?.trim().to_owned();
        return Ok(value.strip_prefix("origin/").map(str::to_owned));
    }
    for candidate in ["main", "master"] {
        if git(
            directory,
            &[
                "show-ref",
                "--verify",
                "--quiet",
                &format!("refs/heads/{candidate}"),
            ],
        )
        .await?
        .status
        .success()
        {
            return Ok(Some(candidate.into()));
        }
    }
    Ok(None)
}

fn parse_numstat(stdout: &str) -> BTreeMap<String, (u64, u64)> {
    stdout
        .lines()
        .filter_map(|line| {
            let mut fields = line.splitn(3, '\t');
            let insertions = fields.next()?.parse().ok().unwrap_or(0);
            let deletions = fields.next()?.parse().ok().unwrap_or(0);
            let path = fields.next()?.trim();
            (!path.is_empty()).then(|| (path.to_owned(), (insertions, deletions)))
        })
        .collect()
}

fn status_paths(stdout: &[u8]) -> Vec<String> {
    let entries = stdout.split(|byte| *byte == 0).collect::<Vec<_>>();
    let mut paths = Vec::new();
    let mut index = 0;
    while index < entries.len() {
        let entry = entries[index];
        if entry.len() >= 4 {
            let path = String::from_utf8_lossy(&entry[3..]).trim().to_owned();
            if !path.is_empty() {
                paths.push(path);
            }
            if matches!(entry.first(), Some(b'R' | b'C'))
                || matches!(entry.get(1), Some(b'R' | b'C'))
            {
                index += 1;
            }
        }
        index += 1;
    }
    paths
}

fn non_repo_status() -> Value {
    json!({
        "isRepo":false,
        "hasPrimaryRemote":false,
        "isDefaultRef":false,
        "refName":null,
        "hasWorkingTreeChanges":false,
        "workingTree":{"files":[],"insertions":0,"deletions":0},
        "branchChanges":{"baseRef":null,"insertions":0,"deletions":0},
        "hasUpstream":false,
        "aheadCount":0,
        "behindCount":0,
        "aheadOfDefaultCount":0,
        "pr":null
    })
}

pub(crate) async fn status(input: &Value) -> Result<Value> {
    let directory = cwd(input).await?;
    if !is_repo(&directory).await? {
        return Ok(non_repo_status());
    }
    let ref_name = current_ref(&directory).await?;
    let default = default_ref(&directory).await?;
    let primary_remote = has_primary_remote(&directory).await?;
    let porcelain = checked_git(
        &directory,
        "status",
        &["status", "--porcelain=v1", "-z", "--untracked-files=all"],
    )
    .await?;
    let paths = status_paths(porcelain.as_bytes());
    let stats = checked_git(&directory, "diff", &["diff", "--numstat", "HEAD", "--"])
        .await
        .map(|output| parse_numstat(&output))
        .unwrap_or_default();
    let files = paths
        .iter()
        .map(|path| {
            let (insertions, deletions) = stats.get(path).copied().unwrap_or_default();
            json!({"path":path,"insertions":insertions,"deletions":deletions})
        })
        .collect::<Vec<_>>();
    let insertions = files
        .iter()
        .filter_map(|file| file["insertions"].as_u64())
        .sum::<u64>();
    let deletions = files
        .iter()
        .filter_map(|file| file["deletions"].as_u64())
        .sum::<u64>();

    let upstream = git(&directory, &["rev-parse", "--abbrev-ref", "@{upstream}"]).await?;
    let upstream_name = upstream
        .status
        .success()
        .then(|| String::from_utf8_lossy(&upstream.stdout).trim().to_owned())
        .filter(|value| !value.is_empty());
    let (ahead, behind) = if upstream_name.is_some() {
        let counts = checked_git(
            &directory,
            "compare upstream",
            &["rev-list", "--left-right", "--count", "HEAD...@{upstream}"],
        )
        .await
        .unwrap_or_default();
        let mut fields = counts.split_whitespace();
        (
            fields
                .next()
                .and_then(|value| value.parse().ok())
                .unwrap_or(0),
            fields
                .next()
                .and_then(|value| value.parse().ok())
                .unwrap_or(0),
        )
    } else {
        (0, 0)
    };
    let ahead_of_default = if let Some(default) = default.as_deref() {
        checked_git(
            &directory,
            "compare default branch",
            &["rev-list", "--count", &format!("{default}..HEAD")],
        )
        .await
        .ok()
        .and_then(|value| value.trim().parse().ok())
        .unwrap_or(0)
    } else {
        0
    };
    Ok(json!({
        "isRepo":true,
        "hasPrimaryRemote":primary_remote,
        "isDefaultRef":ref_name.is_some() && ref_name == default,
        "refName":ref_name,
        "hasWorkingTreeChanges":!paths.is_empty(),
        "workingTree":{"files":files,"insertions":insertions,"deletions":deletions},
        "branchChanges":{"baseRef":default,"insertions":insertions,"deletions":deletions},
        "hasUpstream":upstream_name.is_some(),
        "aheadCount":ahead,
        "behindCount":behind,
        "aheadOfDefaultCount":ahead_of_default,
        "pr":null
    }))
}

async fn worktree_paths(directory: &Path) -> Result<BTreeMap<String, String>> {
    let output = checked_git(
        directory,
        "list worktrees",
        &["worktree", "list", "--porcelain"],
    )
    .await?;
    let mut paths = BTreeMap::new();
    let mut path = None;
    for line in output.lines().chain(std::iter::once("")) {
        if let Some(value) = line.strip_prefix("worktree ") {
            path = Some(value.to_owned());
        } else if let Some(value) = line.strip_prefix("branch refs/heads/") {
            if let Some(path) = path.as_ref() {
                paths.insert(value.to_owned(), path.clone());
            }
        } else if line.is_empty() {
            path = None;
        }
    }
    Ok(paths)
}

pub(crate) async fn list_refs(input: &Value) -> Result<Value> {
    let directory = cwd(input).await?;
    if !is_repo(&directory).await? {
        return Ok(
            json!({"refs":[],"isRepo":false,"hasPrimaryRemote":false,"nextCursor":null,"totalCount":0}),
        );
    }
    let current = current_ref(&directory).await?;
    let default = default_ref(&directory).await?;
    let worktrees = worktree_paths(&directory).await?;
    let output = checked_git(
        &directory,
        "list refs",
        &[
            "for-each-ref",
            "--format=%(refname)",
            "refs/heads",
            "refs/remotes",
        ],
    )
    .await?;
    let query = input["query"].as_str().unwrap_or_default().to_lowercase();
    let kind = input["refKind"].as_str().unwrap_or("all");
    let mut refs = output
        .lines()
        .filter_map(|full| {
            let (name, remote, remote_name) = if let Some(name) = full.strip_prefix("refs/heads/") {
                (name, false, None)
            } else if let Some(name) = full.strip_prefix("refs/remotes/") {
                let (remote, branch) = name.split_once('/')?;
                if branch == "HEAD" {
                    return None;
                }
                (name, true, Some(remote))
            } else {
                return None;
            };
            if (kind == "local" && remote)
                || (kind == "remote" && !remote)
                || (!query.is_empty() && !name.to_lowercase().contains(&query))
            {
                return None;
            }
            Some(json!({
                "name":name,
                "isRemote":remote,
                "remoteName":remote_name,
                "current":!remote && current.as_deref() == Some(name),
                "isDefault":if remote {name.split_once('/').is_some_and(|(_, branch)| default.as_deref()==Some(branch))} else {default.as_deref()==Some(name)},
                "worktreePath":if remote {None} else {worktrees.get(name).cloned()}
            }))
        })
        .collect::<Vec<_>>();
    refs.sort_by(|a, b| {
        b["current"]
            .as_bool()
            .cmp(&a["current"].as_bool())
            .then_with(|| a["name"].as_str().cmp(&b["name"].as_str()))
    });
    let total = refs.len();
    let cursor = input["cursor"].as_u64().unwrap_or(0) as usize;
    let limit = input["limit"].as_u64().unwrap_or(100).min(200) as usize;
    let page = refs
        .into_iter()
        .skip(cursor)
        .take(limit)
        .collect::<Vec<_>>();
    let next = (cursor + page.len() < total).then_some(cursor + page.len());
    Ok(json!({
        "refs":page,
        "isRepo":true,
        "hasPrimaryRemote":has_primary_remote(&directory).await?,
        "nextCursor":next,
        "totalCount":total
    }))
}

fn safe_component(value: &str) -> String {
    let value = value
        .chars()
        .map(|character| {
            if character.is_ascii_alphanumeric() || matches!(character, '.' | '_' | '-') {
                character
            } else {
                '-'
            }
        })
        .collect::<String>();
    let value = value.trim_matches('-');
    if value.is_empty() {
        "worktree".into()
    } else {
        value.into()
    }
}

fn default_worktree_path(home: &Path, directory: &Path, reference: &str) -> PathBuf {
    let repository = directory
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or("repository");
    home.join("worktrees")
        .join(safe_component(repository))
        .join(safe_component(reference))
}

pub(crate) async fn create_worktree(home: &Path, input: &Value) -> Result<Value> {
    let directory = cwd(input).await?;
    ensure!(
        is_repo(&directory).await?,
        "Source control path is not a Git repository"
    );
    let reference = crate::store::required(input, "refName")?;
    let new_reference = input["newRefName"].as_str();
    let base = input["baseRefName"].as_str().unwrap_or(reference);
    let path = input["path"]
        .as_str()
        .map(PathBuf::from)
        .unwrap_or_else(|| {
            default_worktree_path(home, &directory, new_reference.unwrap_or(reference))
        });
    ensure!(path.is_absolute(), "Worktree path must be absolute");
    if let Some(parent) = path.parent() {
        tokio::fs::create_dir_all(parent).await?;
    }
    let path_text = path.to_string_lossy().into_owned();
    if let Some(new_reference) = new_reference {
        checked_git(
            &directory,
            "create worktree",
            &["worktree", "add", "-b", new_reference, &path_text, base],
        )
        .await?;
    } else {
        checked_git(
            &directory,
            "create worktree",
            &["worktree", "add", &path_text, reference],
        )
        .await?;
    }
    let path = tokio::fs::canonicalize(path).await?;
    Ok(json!({"worktree":{"path":path,"refName":new_reference.unwrap_or(reference)}}))
}

pub(crate) async fn fetch_ref(directory: &str, remote: &str, reference: &str) -> Result<()> {
    let directory = tokio::fs::canonicalize(directory)
        .await
        .context("Source control directory does not exist")?;
    checked_git(
        &directory,
        "fetch branch",
        &["fetch", "--quiet", remote, reference],
    )
    .await?;
    Ok(())
}

pub(crate) async fn remove_worktree(input: &Value) -> Result<Value> {
    let directory = cwd(input).await?;
    let path = crate::store::required(input, "path")?;
    let mut args = vec!["worktree", "remove"];
    if input["force"] == true {
        args.push("--force");
    }
    args.push(path);
    checked_git(&directory, "remove worktree", &args).await?;
    Ok(Value::Null)
}

pub(crate) async fn create_ref(input: &Value) -> Result<Value> {
    let directory = cwd(input).await?;
    let reference = crate::store::required(input, "refName")?;
    checked_git(&directory, "create branch", &["branch", reference]).await?;
    if input["switchRef"] == true {
        checked_git(&directory, "switch branch", &["switch", reference]).await?;
    }
    Ok(json!({"refName":reference}))
}

pub(crate) async fn switch_ref(input: &Value) -> Result<Value> {
    let directory = cwd(input).await?;
    let reference = crate::store::required(input, "refName")?;
    checked_git(&directory, "switch branch", &["switch", reference]).await?;
    Ok(json!({"refName":current_ref(&directory).await?}))
}

pub(crate) async fn init(input: &Value) -> Result<Value> {
    ensure!(
        input["kind"].as_str().is_none_or(|kind| kind == "git"),
        "Only Git repositories are supported"
    );
    let directory = cwd(input).await?;
    checked_git(&directory, "initialize repository", &["init"]).await?;
    Ok(Value::Null)
}

pub(crate) async fn pull(input: &Value) -> Result<Value> {
    let directory = cwd(input).await?;
    let reference = current_ref(&directory)
        .await?
        .context("Detached HEAD cannot be pulled")?;
    let upstream = checked_git(
        &directory,
        "read upstream",
        &["rev-parse", "--abbrev-ref", "@{upstream}"],
    )
    .await?
    .trim()
    .to_owned();
    let before = checked_git(&directory, "read HEAD", &["rev-parse", "HEAD"]).await?;
    checked_git(&directory, "pull", &["pull", "--ff-only"]).await?;
    let after = checked_git(&directory, "read HEAD", &["rev-parse", "HEAD"]).await?;
    Ok(json!({
        "status":if before == after {"skipped_up_to_date"} else {"pulled"},
        "refName":reference,
        "upstreamRef":upstream
    }))
}

pub(crate) fn status_stream_snapshot(status: Value) -> Value {
    let mut local = Map::new();
    let mut remote = Map::new();
    let local_keys = [
        "isRepo",
        "sourceControlProvider",
        "hasPrimaryRemote",
        "isDefaultRef",
        "refName",
        "hasWorkingTreeChanges",
        "workingTree",
        "branchChanges",
    ];
    let remote_keys = [
        "hasUpstream",
        "aheadCount",
        "behindCount",
        "aheadOfDefaultCount",
        "pr",
    ];
    for key in local_keys {
        if let Some(value) = status.get(key) {
            local.insert(key.into(), value.clone());
        }
    }
    for key in remote_keys {
        if let Some(value) = status.get(key) {
            remote.insert(key.into(), value.clone());
        }
    }
    json!({"_tag":"snapshot","local":local,"remote":remote})
}

#[cfg(test)]
mod tests {
    use super::*;

    async fn repository() -> Result<tempfile::TempDir> {
        let directory = tempfile::tempdir()?;
        checked_git(directory.path(), "init", &["init", "-b", "main"]).await?;
        tokio::fs::write(directory.path().join("README.md"), "hello\n").await?;
        checked_git(directory.path(), "add", &["add", "."]).await?;
        Command::new("git")
            .args([
                "-c",
                "user.name=T3 Test",
                "-c",
                "user.email=test@t3.invalid",
                "commit",
                "-m",
                "initial",
            ])
            .current_dir(directory.path())
            .output()
            .await?
            .status
            .success()
            .then_some(())
            .context("Failed to create test commit")?;
        Ok(directory)
    }

    #[tokio::test]
    async fn reports_changes_and_manages_refs_and_worktrees() -> Result<()> {
        let repository = repository().await?;
        tokio::fs::write(repository.path().join("README.md"), "hello\nworld\n").await?;
        let status = status(&json!({"cwd":repository.path()})).await?;
        assert_eq!(status["isRepo"], true);
        assert_eq!(status["refName"], "main");
        assert_eq!(status["workingTree"]["insertions"], 1);

        create_ref(&json!({"cwd":repository.path(),"refName":"feature"})).await?;
        let refs = list_refs(&json!({"cwd":repository.path(),"refKind":"local"})).await?;
        assert_eq!(refs["totalCount"], 2);

        let home = tempfile::tempdir()?;
        let worktree = create_worktree(
            home.path(),
            &json!({
                "cwd":repository.path(),
                "refName":"main",
                "newRefName":"worktree-branch",
                "path":null
            }),
        )
        .await?;
        let worktree_path = worktree["worktree"]["path"]
            .as_str()
            .context("Missing path")?;
        assert!(Path::new(worktree_path).is_dir());
        remove_worktree(&json!({"cwd":repository.path(),"path":worktree_path,"force":true}))
            .await?;
        assert!(!Path::new(worktree_path).exists());
        Ok(())
    }
}
