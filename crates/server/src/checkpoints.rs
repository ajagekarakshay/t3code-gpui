use anyhow::{Context, Result, ensure};
use serde_json::{Value, json};
use std::{
    collections::BTreeMap,
    path::{Path, PathBuf},
    process::Output,
};
use tokio::process::Command;

async fn git(cwd: &Path, args: &[String], index: Option<&Path>) -> Result<Output> {
    let mut command = Command::new("git");
    command
        .args(args)
        .current_dir(cwd)
        .env("GIT_TERMINAL_PROMPT", "0");
    if let Some(index) = index {
        command.env("GIT_INDEX_FILE", index);
    }
    command.output().await.context("Git is not installed")
}

async fn checked(
    cwd: &Path,
    operation: &str,
    args: Vec<String>,
    index: Option<&Path>,
) -> Result<String> {
    let output = git(cwd, &args, index).await?;
    ensure!(
        output.status.success(),
        "Git {operation} failed (exit {}, {} bytes of diagnostics)",
        output.status.code().unwrap_or(-1),
        output.stderr.len()
    );
    String::from_utf8(output.stdout).context("Git returned non-UTF-8 output")
}

pub(crate) async fn is_checkpointable(cwd: &Path) -> Result<bool> {
    let output = git(
        cwd,
        &["rev-parse".into(), "--is-inside-work-tree".into()],
        None,
    )
    .await?;
    Ok(output.status.success() && output.stdout.starts_with(b"true"))
}

/// Capture the workspace through a private index so the user's staged state is untouched.
pub(crate) async fn capture(cwd: &Path, checkpoint_ref: &str) -> Result<()> {
    ensure!(
        checkpoint_ref.starts_with("refs/t3/checkpoints/"),
        "Invalid checkpoint ref"
    );
    let common_dir = checked(
        cwd,
        "resolve checkpoint storage",
        vec![
            "rev-parse".into(),
            "--path-format=absolute".into(),
            "--git-common-dir".into(),
        ],
        None,
    )
    .await?;
    let common_dir = PathBuf::from(common_dir.trim());
    let index = common_dir.join(format!("t3-rust-checkpoint-index-{}", uuid::Uuid::new_v4()));
    let lock = PathBuf::from(format!("{}.lock", index.display()));
    let result = async {
        let head = git(
            cwd,
            &[
                "rev-parse".into(),
                "--verify".into(),
                "HEAD^{commit}".into(),
            ],
            None,
        )
        .await?;
        let has_head = head.status.success();
        checked(
            cwd,
            "prepare checkpoint index",
            if has_head {
                vec!["read-tree".into(), "HEAD".into()]
            } else {
                vec!["read-tree".into(), "--empty".into()]
            },
            Some(&index),
        )
        .await?;
        checked(
            cwd,
            "stage checkpoint workspace",
            vec!["add".into(), "-A".into(), "--".into(), ".".into()],
            Some(&index),
        )
        .await?;
        let tree = checked(
            cwd,
            "write checkpoint tree",
            vec!["write-tree".into()],
            Some(&index),
        )
        .await?;
        let tree = tree.trim();
        ensure!(!tree.is_empty(), "Git returned an empty checkpoint tree ID");
        let mut commit_args = vec!["commit-tree".into(), tree.into()];
        if has_head {
            commit_args.extend(["-p".into(), "HEAD".into()]);
        }
        commit_args.extend(["-m".into(), format!("t3 checkpoint ref={checkpoint_ref}")]);
        let output = Command::new("git")
            .args(&commit_args)
            .current_dir(cwd)
            .env("GIT_TERMINAL_PROMPT", "0")
            .env("GIT_INDEX_FILE", &index)
            .env("GIT_AUTHOR_NAME", "T3 Code")
            .env("GIT_AUTHOR_EMAIL", "t3code@users.noreply.github.com")
            .env("GIT_COMMITTER_NAME", "T3 Code")
            .env("GIT_COMMITTER_EMAIL", "t3code@users.noreply.github.com")
            .output()
            .await
            .context("Git is not installed")?;
        ensure!(
            output.status.success(),
            "Git commit checkpoint failed (exit {}, {} bytes of diagnostics)",
            output.status.code().unwrap_or(-1),
            output.stderr.len()
        );
        let commit = String::from_utf8(output.stdout)?;
        let commit = commit.trim();
        ensure!(
            !commit.is_empty(),
            "Git returned an empty checkpoint commit ID"
        );
        checked(
            cwd,
            "publish checkpoint",
            vec!["update-ref".into(), checkpoint_ref.into(), commit.into()],
            None,
        )
        .await?;
        Ok(())
    }
    .await;
    let _ = tokio::fs::remove_file(&index).await;
    let _ = tokio::fs::remove_file(&lock).await;
    result
}

pub(crate) async fn restore(cwd: &Path, checkpoint_ref: &str) -> Result<()> {
    ensure!(
        checkpoint_ref.starts_with("refs/t3/checkpoints/"),
        "Invalid checkpoint ref"
    );
    let commit = checked(
        cwd,
        "resolve checkpoint",
        vec![
            "rev-parse".into(),
            "--verify".into(),
            format!("{checkpoint_ref}^{{commit}}"),
        ],
        None,
    )
    .await?;
    let commit = commit.trim();
    let tracked = checked(
        cwd,
        "inspect checkpoint",
        vec![
            "ls-files".into(),
            "--cached".into(),
            format!("--with-tree={commit}"),
            "-z".into(),
            "--".into(),
            ".".into(),
        ],
        None,
    )
    .await?;
    if !tracked.is_empty() {
        checked(
            cwd,
            "restore checkpoint",
            vec![
                "restore".into(),
                "--source".into(),
                commit.into(),
                "--worktree".into(),
                "--staged".into(),
                "--".into(),
                ".".into(),
            ],
            None,
        )
        .await?;
    }
    tokio::fs::create_dir_all(cwd)
        .await
        .context("Could not recreate checkpoint workspace")?;
    checked(
        cwd,
        "clean checkpoint workspace",
        vec!["clean".into(), "-fd".into(), "--".into(), ".".into()],
        None,
    )
    .await?;
    tokio::fs::create_dir_all(cwd)
        .await
        .context("Could not recreate checkpoint workspace")?;
    let head = git(
        cwd,
        &[
            "rev-parse".into(),
            "--verify".into(),
            "HEAD^{commit}".into(),
        ],
        None,
    )
    .await?;
    if head.status.success() {
        checked(
            cwd,
            "unstage restored workspace",
            vec!["reset".into(), "--quiet".into(), "--".into(), ".".into()],
            None,
        )
        .await?;
    }
    Ok(())
}

pub(crate) async fn file_summaries(cwd: &Path, from: &str, to: &str) -> Vec<Value> {
    let output = checked(
        cwd,
        "summarize checkpoint",
        vec![
            "diff".into(),
            "--numstat".into(),
            from.into(),
            to.into(),
            "--".into(),
            ".".into(),
        ],
        None,
    )
    .await;
    let Ok(output) = output else {
        return Vec::new();
    };
    let mut files = BTreeMap::<String, (u64, u64)>::new();
    for line in output.lines() {
        let mut fields = line.splitn(3, '\t');
        let additions = fields
            .next()
            .and_then(|value| value.parse().ok())
            .unwrap_or(0);
        let deletions = fields
            .next()
            .and_then(|value| value.parse().ok())
            .unwrap_or(0);
        if let Some(path) = fields.next().filter(|path| !path.is_empty()) {
            files.insert(path.to_owned(), (additions, deletions));
        }
    }
    files.into_iter().map(|(path, (additions, deletions))| json!({"path":path,"kind":"modified","additions":additions,"deletions":deletions})).collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    async fn run(cwd: &Path, args: &[&str]) -> Result<()> {
        checked(
            cwd,
            "test setup",
            args.iter().map(|value| (*value).into()).collect(),
            None,
        )
        .await?;
        Ok(())
    }

    #[tokio::test]
    async fn capture_and_restore_preserve_head_and_the_user_index() -> Result<()> {
        let directory = tempfile::tempdir()?;
        let cwd = directory.path();
        run(cwd, &["init"]).await?;
        run(cwd, &["config", "user.email", "test@example.com"]).await?;
        run(cwd, &["config", "user.name", "Test"]).await?;
        tokio::fs::write(cwd.join("file.txt"), "initial\n").await?;
        run(cwd, &["add", "."]).await?;
        run(cwd, &["commit", "-m", "initial"]).await?;
        tokio::fs::write(cwd.join("staged.txt"), "staged\n").await?;
        run(cwd, &["add", "staged.txt"]).await?;
        tokio::fs::write(cwd.join("file.txt"), "checkpoint\n").await?;
        let staged_before = checked(
            cwd,
            "read index",
            vec!["diff".into(), "--cached".into()],
            None,
        )
        .await?;
        let head_before = checked(
            cwd,
            "read head",
            vec!["rev-parse".into(), "HEAD".into()],
            None,
        )
        .await?;
        let checkpoint_ref = "refs/t3/checkpoints/test/1";
        capture(cwd, checkpoint_ref).await?;
        assert_eq!(
            checked(
                cwd,
                "read index",
                vec!["diff".into(), "--cached".into()],
                None
            )
            .await?,
            staged_before
        );
        tokio::fs::write(cwd.join("file.txt"), "later\n").await?;
        tokio::fs::write(cwd.join("untracked.txt"), "remove\n").await?;
        restore(cwd, checkpoint_ref).await?;
        assert_eq!(
            tokio::fs::read_to_string(cwd.join("file.txt")).await?,
            "checkpoint\n"
        );
        assert!(!cwd.join("untracked.txt").exists());
        assert_eq!(
            checked(
                cwd,
                "read head",
                vec!["rev-parse".into(), "HEAD".into()],
                None
            )
            .await?,
            head_before
        );
        assert!(
            checked(
                cwd,
                "read index",
                vec!["diff".into(), "--cached".into()],
                None
            )
            .await?
            .is_empty()
        );
        Ok(())
    }
}
