use anyhow::{Context, Result, ensure};
use ignore::WalkBuilder;
use regex::RegexBuilder;
use serde_json::{Value, json};
use std::{
    collections::HashMap,
    ffi::OsStr,
    path::{Component, Path, PathBuf},
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};

const MAX_INDEX_ENTRIES: usize = 25_000;
const MAX_READ_BYTES: usize = 1024 * 1024;
const CONTENT_SEARCH_BUDGET: Duration = Duration::from_millis(250);
const MAX_MATCHES_PER_FILE: usize = 100;
const INDEX_TTL: Duration = Duration::from_secs(2);

#[derive(Clone)]
struct Entry {
    path: String,
    kind: &'static str,
}

struct CachedIndex {
    entries: Arc<Vec<Entry>>,
    truncated: bool,
    scanned_at: Instant,
}

#[derive(Clone, Default)]
pub(crate) struct WorkspaceFilesystem {
    indexes: Arc<Mutex<HashMap<PathBuf, CachedIndex>>>,
}

impl WorkspaceFilesystem {
    fn index(&self, root: &Path) -> Result<(Arc<Vec<Entry>>, bool)> {
        if let Some(index) = self
            .indexes
            .lock()
            .map_err(|_| anyhow::anyhow!("Workspace index lock poisoned"))?
            .get(root)
            .filter(|index| index.scanned_at.elapsed() < INDEX_TTL)
        {
            return Ok((index.entries.clone(), index.truncated));
        }
        let (entries, truncated) = scan(root)?;
        let entries = Arc::new(entries);
        self.indexes
            .lock()
            .map_err(|_| anyhow::anyhow!("Workspace index lock poisoned"))?
            .insert(
                root.to_owned(),
                CachedIndex {
                    entries: entries.clone(),
                    truncated,
                    scanned_at: Instant::now(),
                },
            );
        Ok((entries, truncated))
    }

    fn invalidate(&self, root: &Path) -> Result<()> {
        self.indexes
            .lock()
            .map_err(|_| anyhow::anyhow!("Workspace index lock poisoned"))?
            .remove(root);
        Ok(())
    }
}

fn string(input: &Value, key: &str) -> Result<String> {
    input[key]
        .as_str()
        .filter(|value| !value.trim().is_empty())
        .map(str::to_owned)
        .with_context(|| format!("Missing {key}"))
}

fn positive_limit(input: &Value, maximum: usize) -> Result<usize> {
    let limit = input["limit"].as_u64().context("Invalid limit")? as usize;
    ensure!(limit > 0 && limit <= maximum, "Invalid limit");
    Ok(limit)
}

fn root(input: &Value) -> Result<PathBuf> {
    let requested = string(input, "cwd")?;
    let root = std::fs::canonicalize(&requested)
        .with_context(|| format!("Workspace root is unavailable: {requested}"))?;
    ensure!(root.is_dir(), "Workspace root is not a directory");
    Ok(root)
}

fn posix_relative(root: &Path, path: &Path) -> Result<String> {
    let relative = path.strip_prefix(root)?;
    let value = relative
        .components()
        .map(|component| component.as_os_str().to_string_lossy())
        .collect::<Vec<_>>()
        .join("/");
    ensure!(!value.is_empty(), "Workspace root is not an entry");
    Ok(value)
}

fn scan(root: &Path) -> Result<(Vec<Entry>, bool)> {
    let mut entries = Vec::new();
    let mut truncated = false;
    let walker = WalkBuilder::new(root)
        .hidden(false)
        .follow_links(false)
        .filter_entry(|entry| entry.file_name() != OsStr::new(".git"))
        .build();
    for item in walker.skip(1) {
        let item = match item {
            Ok(item) => item,
            Err(_) => continue,
        };
        let Some(kind) = item.file_type().and_then(|kind| {
            if kind.is_file() {
                Some("file")
            } else if kind.is_dir() {
                Some("directory")
            } else {
                None
            }
        }) else {
            continue;
        };
        if entries.len() == MAX_INDEX_ENTRIES {
            truncated = true;
            break;
        }
        entries.push(Entry {
            path: posix_relative(root, item.path())?,
            kind,
        });
    }
    Ok((entries, truncated))
}

fn json_entries(entries: impl IntoIterator<Item = Entry>, truncated: bool) -> Value {
    json!({
        "entries":entries.into_iter().map(|entry|json!({"path":entry.path,"kind":entry.kind})).collect::<Vec<_>>(),
        "truncated":truncated
    })
}

fn is_image(path: &str) -> bool {
    Path::new(path)
        .extension()
        .and_then(OsStr::to_str)
        .is_some_and(|extension| {
            matches!(
                extension.to_ascii_lowercase().as_str(),
                "avif" | "gif" | "icns" | "ico" | "jpg" | "jpeg" | "png" | "svg" | "webp"
            )
        })
}

pub(crate) async fn search_entries(filesystem: WorkspaceFilesystem, input: Value) -> Result<Value> {
    tokio::task::spawn_blocking(move || {
        let root = root(&input)?;
        let query = input["query"]
            .as_str()
            .context("Invalid query")?
            .trim_start_matches(['@', '.', '/'])
            .to_ascii_lowercase();
        ensure!(query.len() <= 256, "Query is too long");
        let limit = positive_limit(&input, 200)?;
        let kind = input["kind"].as_str();
        ensure!(
            kind.is_none_or(|kind| matches!(kind, "file" | "directory")),
            "Invalid entry kind"
        );
        let image_only = input["imageOnly"].as_bool().unwrap_or(false);
        let (entries, scan_truncated) = filesystem.index(&root)?;
        let mut matches = entries
            .iter()
            .filter(|entry| kind.is_none_or(|kind| entry.kind == kind))
            .filter(|entry| !image_only || entry.kind == "file" && is_image(&entry.path))
            .filter_map(|entry| {
                let candidate = entry.path.to_ascii_lowercase();
                let position = candidate.find(&query)?;
                let name_position = candidate.rsplit_once('/').map_or(position, |(_, name)| {
                    name.find(&query).unwrap_or(usize::MAX)
                });
                Some(((name_position, position, candidate.len()), entry.clone()))
            })
            .collect::<Vec<_>>();
        matches.sort_by(|left, right| left.0.cmp(&right.0).then(left.1.path.cmp(&right.1.path)));
        let truncated = scan_truncated || matches.len() > limit;
        matches.truncate(limit);
        Ok(json_entries(
            matches.into_iter().map(|(_, entry)| entry),
            truncated,
        ))
    })
    .await?
}

fn safe_relative(input: &str) -> Result<PathBuf> {
    let path = Path::new(input);
    ensure!(!path.is_absolute(), "Workspace path must be relative");
    let mut relative = PathBuf::new();
    for component in path.components() {
        match component {
            Component::Normal(value) => relative.push(value),
            Component::CurDir => {}
            Component::ParentDir | Component::RootDir | Component::Prefix(_) => {
                anyhow::bail!("Workspace path is outside the root")
            }
        }
    }
    ensure!(!relative.as_os_str().is_empty(), "Workspace path is empty");
    Ok(relative)
}

fn resolve_existing(root: &Path, relative: &str) -> Result<(String, PathBuf)> {
    let requested = Path::new(relative);
    if requested.is_absolute() {
        let canonical = std::fs::canonicalize(requested)?;
        return Ok((relative.to_owned(), canonical));
    }
    let relative_path = safe_relative(relative)?;
    let canonical = std::fs::canonicalize(root.join(&relative_path))?;
    ensure!(
        canonical.starts_with(root),
        "Workspace path resolves outside the root"
    );
    Ok((
        relative_path.to_string_lossy().replace('\\', "/"),
        canonical,
    ))
}

pub(crate) async fn list_entries(filesystem: WorkspaceFilesystem, input: Value) -> Result<Value> {
    tokio::task::spawn_blocking(move || {
        let root = root(&input)?;
        let Some(directory_path) = input.get("directoryPath").and_then(Value::as_str) else {
            let (entries, truncated) = filesystem.index(&root)?;
            let mut entries = entries.as_ref().clone();
            entries.sort_by(|left, right| left.path.cmp(&right.path));
            return Ok(json_entries(entries, truncated));
        };
        let (prefix, directory) = if directory_path.is_empty() {
            (String::new(), root.clone())
        } else {
            let relative = safe_relative(directory_path)?;
            ensure!(
                !relative
                    .components()
                    .any(|part| part.as_os_str() == OsStr::new(".git")),
                "The .git directory cannot be listed"
            );
            let directory = std::fs::canonicalize(root.join(&relative))?;
            ensure!(
                directory.starts_with(&root),
                "Directory resolves outside the root"
            );
            (relative.to_string_lossy().replace('\\', "/"), directory)
        };
        ensure!(directory.is_dir(), "Workspace path is not a directory");
        let mut entries = Vec::new();
        for child in std::fs::read_dir(directory)? {
            let child = child?;
            if child.file_name() == OsStr::new(".git") {
                continue;
            }
            let kind = child.file_type()?;
            let kind = if kind.is_file() {
                "file"
            } else if kind.is_dir() {
                "directory"
            } else {
                continue;
            };
            let name = child.file_name().to_string_lossy().into_owned();
            entries.push(Entry {
                path: if prefix.is_empty() {
                    name
                } else {
                    format!("{prefix}/{name}")
                },
                kind,
            });
        }
        entries.sort_by(|left, right| left.path.cmp(&right.path));
        Ok(json_entries(entries, false))
    })
    .await?
}

pub(crate) async fn read_file(input: Value) -> Result<Value> {
    tokio::task::spawn_blocking(move || {
        let root = root(&input)?;
        let relative_path = string(&input, "relativePath")?;
        ensure!(relative_path.len() <= 512, "Path is too long");
        let (relative_path, target) = resolve_existing(&root, &relative_path)?;
        let metadata = std::fs::metadata(&target)?;
        ensure!(metadata.is_file(), "Workspace path is not a file");
        let byte_length = metadata.len();
        let mut bytes = std::fs::read(&target)?;
        ensure!(
            !bytes.contains(&0),
            "Binary files cannot be previewed as text"
        );
        let truncated = bytes.len() > MAX_READ_BYTES;
        bytes.truncate(MAX_READ_BYTES);
        Ok(json!({
            "relativePath":relative_path,
            "contents":String::from_utf8_lossy(&bytes),
            "byteLength":byte_length,
            "truncated":truncated
        }))
    })
    .await?
}

fn ensure_write_parent(root: &Path, relative: &Path) -> Result<PathBuf> {
    let mut current = root.to_owned();
    if let Some(parent) = relative.parent() {
        for component in parent.components() {
            let Component::Normal(name) = component else {
                anyhow::bail!("Workspace path is outside the root")
            };
            let next = current.join(name);
            match std::fs::symlink_metadata(&next) {
                Ok(metadata) => {
                    ensure!(
                        metadata.is_dir() || metadata.file_type().is_symlink(),
                        "Parent path is not a directory"
                    );
                    let canonical = std::fs::canonicalize(&next)?;
                    ensure!(
                        canonical.starts_with(root),
                        "Workspace path resolves outside the root"
                    );
                    ensure!(canonical.is_dir(), "Parent path is not a directory");
                    current = canonical;
                }
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                    std::fs::create_dir(&next)?;
                    current = next;
                }
                Err(error) => return Err(error.into()),
            }
        }
    }
    Ok(current)
}

pub(crate) async fn write_file(filesystem: WorkspaceFilesystem, input: Value) -> Result<Value> {
    tokio::task::spawn_blocking(move || {
        let root = root(&input)?;
        let requested = string(&input, "relativePath")?;
        ensure!(requested.len() <= 512, "Path is too long");
        let relative = safe_relative(&requested)?;
        let parent = ensure_write_parent(&root, &relative)?;
        let name = relative.file_name().context("Workspace path is empty")?;
        let target = parent.join(name);
        if let Ok(metadata) = std::fs::symlink_metadata(&target) {
            ensure!(
                metadata.is_file() || metadata.file_type().is_symlink(),
                "Workspace path is not a file"
            );
            let canonical = std::fs::canonicalize(&target)?;
            ensure!(
                canonical.starts_with(&root),
                "Workspace path resolves outside the root"
            );
            ensure!(canonical.is_file(), "Workspace path is not a file");
        }
        std::fs::write(
            &target,
            input["contents"].as_str().context("Invalid contents")?,
        )?;
        filesystem.invalidate(&root)?;
        Ok(json!({"relativePath":relative.to_string_lossy().replace('\\', "/")}))
    })
    .await?
}

fn utf16_offset(value: &str, byte: usize) -> usize {
    value[..byte].encode_utf16().count()
}

fn word_character(value: Option<char>) -> bool {
    value.is_some_and(|value| value.is_alphanumeric() || value == '_')
}

fn whole_word(line: &str, start: usize, end: usize) -> bool {
    !word_character(line[..start].chars().next_back())
        && !word_character(line[end..].chars().next())
}

pub(crate) async fn search_contents(
    filesystem: WorkspaceFilesystem,
    input: Value,
) -> Result<Value> {
    tokio::task::spawn_blocking(move || {
        let root = root(&input)?;
        let query = input["query"].as_str().context("Invalid query")?;
        ensure!(!query.is_empty() && query.len() <= 256, "Invalid query");
        let limit = positive_limit(&input, 500)?;
        let case_sensitive = input["caseSensitive"].as_bool().context("Invalid caseSensitive")?;
        let whole_words = input["wholeWord"].as_bool().context("Invalid wholeWord")?;
        let use_regex = input["useRegex"].as_bool().context("Invalid useRegex")?;
        let (regex, regex_fallback_error) = if use_regex {
            match RegexBuilder::new(query).case_insensitive(!case_sensitive).build() {
                Ok(regex) => (Some(regex), None),
                Err(error) => (
                    Some(
                        RegexBuilder::new(&regex::escape(query))
                            .case_insensitive(!case_sensitive)
                            .build()?,
                    ),
                    Some(error.to_string()),
                ),
            }
        } else {
            (
                Some(
                    RegexBuilder::new(&regex::escape(query))
                        .case_insensitive(!case_sensitive)
                        .build()?,
                ),
                None,
            )
        };
        let (entries, scan_truncated) = filesystem.index(&root)?;
        let deadline = Instant::now() + CONTENT_SEARCH_BUDGET;
        let mut matches = Vec::new();
        let mut truncated = scan_truncated;
        'files: for entry in entries.iter().filter(|entry| entry.kind == "file") {
            if Instant::now() >= deadline {
                truncated = true;
                break;
            }
            let Ok(metadata) = std::fs::metadata(root.join(&entry.path)) else {
                continue;
            };
            if metadata.len() > 4 * 1024 * 1024 {
                continue;
            }
            let Ok(bytes) = std::fs::read(root.join(&entry.path)) else {
                continue;
            };
            if bytes.contains(&0) {
                continue;
            }
            let text = String::from_utf8_lossy(&bytes);
            let mut file_matches = 0;
            for (line_index, line) in text.lines().enumerate() {
                let ranges = regex
                    .as_ref()
                    .expect("literal fallback always compiles")
                    .find_iter(line)
                    .map(|found| (found.start(), found.end()))
                    .collect::<Vec<_>>();
                let ranges = ranges
                    .into_iter()
                    .filter(|(start, end)| !whole_words || whole_word(line, *start, *end))
                    .map(|(start, end)| json!({"start":utf16_offset(line,start),"end":utf16_offset(line,end)}))
                    .collect::<Vec<_>>();
                if ranges.is_empty() {
                    continue;
                }
                matches.push(json!({
                    "path":entry.path,
                    "lineNumber":line_index + 1,
                    "lineContent":line,
                    "matchRanges":ranges
                }));
                file_matches += 1;
                if matches.len() == limit {
                    truncated = true;
                    break 'files;
                }
                if file_matches == MAX_MATCHES_PER_FILE {
                    break;
                }
            }
        }
        let mut result = json!({"matches":matches,"truncated":truncated});
        if let Some(error) = regex_fallback_error {
            result["regexFallbackError"] = json!(error);
        }
        Ok(result)
    })
    .await?
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn browses_searches_and_edits_workspace_files() -> Result<()> {
        let filesystem = WorkspaceFilesystem::default();
        let directory = tempfile::tempdir()?;
        std::fs::create_dir(directory.path().join("src"))?;
        std::fs::write(
            directory.path().join("src/main.rs"),
            "fn main() {\n    println!(\"hello\");\n}\n",
        )?;
        std::fs::write(directory.path().join("logo.png"), "not really an image")?;
        let cwd = directory.path().to_string_lossy();

        let listed =
            list_entries(filesystem.clone(), json!({"cwd":cwd,"directoryPath":""})).await?;
        assert_eq!(listed["entries"].as_array().unwrap().len(), 2);
        let paths = search_entries(
            filesystem.clone(),
            json!({"cwd":cwd,"query":"main","limit":20}),
        )
        .await?;
        assert_eq!(paths["entries"][0]["path"], "src/main.rs");
        let contents = search_contents(filesystem.clone(), json!({"cwd":cwd,"query":"hello","limit":20,"caseSensitive":false,"wholeWord":true,"useRegex":false})).await?;
        assert_eq!(contents["matches"][0]["lineNumber"], 2);
        assert_eq!(
            contents["matches"][0]["matchRanges"][0],
            json!({"start":14,"end":19})
        );

        let written = write_file(
            filesystem.clone(),
            json!({"cwd":cwd,"relativePath":"notes/todo.md","contents":"ship it"}),
        )
        .await?;
        assert_eq!(written["relativePath"], "notes/todo.md");
        let refreshed =
            search_entries(filesystem, json!({"cwd":cwd,"query":"todo","limit":20})).await?;
        assert_eq!(refreshed["entries"][0]["path"], "notes/todo.md");
        let read = read_file(json!({"cwd":cwd,"relativePath":"notes/todo.md"})).await?;
        assert_eq!(read["contents"], "ship it");
        assert_eq!(read["byteLength"], 7);
        Ok(())
    }

    #[tokio::test]
    async fn rejects_relative_paths_outside_the_workspace() -> Result<()> {
        let filesystem = WorkspaceFilesystem::default();
        let directory = tempfile::tempdir()?;
        let cwd = directory.path().to_string_lossy();
        assert!(
            write_file(
                filesystem.clone(),
                json!({"cwd":cwd,"relativePath":"../escape","contents":"no"})
            )
            .await
            .is_err()
        );
        assert!(
            read_file(json!({"cwd":cwd,"relativePath":"../escape"}))
                .await
                .is_err()
        );
        assert!(
            list_entries(filesystem, json!({"cwd":cwd,"directoryPath":"../"}))
                .await
                .is_err()
        );
        Ok(())
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn rejects_symlinks_that_escape_on_read_and_write() -> Result<()> {
        use std::os::unix::fs::symlink;
        let filesystem = WorkspaceFilesystem::default();
        let directory = tempfile::tempdir()?;
        let outside = tempfile::tempdir()?;
        std::fs::write(outside.path().join("secret"), "secret")?;
        symlink(outside.path(), directory.path().join("outside"))?;
        let cwd = directory.path().to_string_lossy();
        assert!(
            read_file(json!({"cwd":cwd,"relativePath":"outside/secret"}))
                .await
                .is_err()
        );
        assert!(
            write_file(
                filesystem,
                json!({"cwd":cwd,"relativePath":"outside/new","contents":"no"})
            )
            .await
            .is_err()
        );
        assert!(!outside.path().join("new").exists());
        Ok(())
    }
}
