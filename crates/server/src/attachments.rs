use anyhow::{Context, Result, ensure};
use base64::{Engine as _, engine::general_purpose::STANDARD};
use serde_json::{Value, json};
use std::{
    collections::HashMap,
    path::{Path, PathBuf},
    sync::Arc,
    time::{SystemTime, UNIX_EPOCH},
};
use t3_acp::protocol::schema::v1::{ContentBlock, ImageContent, ResourceLink, TextContent};
use tokio::sync::Mutex;

const UPLOAD_TTL_MS: u64 = 10 * 60 * 1_000;
const ASSET_TTL_MS: u64 = 10 * 60 * 1_000;
const MAX_FILE_BYTES: u64 = 50 * 1024 * 1024;
const MAX_IMAGE_BYTES: u64 = 10 * 1024 * 1024;

#[derive(Clone)]
struct UploadClaim {
    attachment_id: String,
    extension: String,
    size: u64,
    expires_at: u64,
}

#[derive(Clone)]
struct AssetClaim {
    path: PathBuf,
    mime_type: String,
    file_name: String,
    disposition: String,
    expires_at: u64,
}

#[derive(Clone)]
pub(crate) struct AttachmentStore {
    root: PathBuf,
    uploads: Arc<Mutex<HashMap<String, UploadClaim>>>,
    assets: Arc<Mutex<HashMap<String, AssetClaim>>>,
}

pub(crate) struct AssetFile {
    pub(crate) bytes: Vec<u8>,
    pub(crate) mime_type: String,
    pub(crate) file_name: String,
    pub(crate) disposition: String,
}

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

fn extension(name: &str, mime_type: &str, kind: &str) -> String {
    let image = match mime_type.to_ascii_lowercase().as_str() {
        "image/png" => Some("png"),
        "image/jpeg" => Some("jpg"),
        "image/gif" => Some("gif"),
        "image/webp" => Some("webp"),
        _ => None,
    };
    let candidate = if kind == "image" {
        image
    } else {
        Path::new(name).extension().and_then(|value| value.to_str())
    };
    let candidate = candidate
        .filter(|value| !value.is_empty() && value.len() <= 12)
        .filter(|value| {
            value
                .chars()
                .all(|character| character.is_ascii_alphanumeric())
        })
        .unwrap_or("bin");
    format!(".{candidate}")
}

fn safe_id(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 256
        && value
            .chars()
            .all(|character| character.is_ascii_alphanumeric() || matches!(character, '-' | '_'))
}

fn thread_segment(value: &str) -> String {
    let segment = value
        .chars()
        .take(48)
        .map(|character| {
            if character.is_ascii_alphanumeric() || matches!(character, '-' | '_') {
                character.to_ascii_lowercase()
            } else {
                '-'
            }
        })
        .collect::<String>();
    if segment.trim_matches('-').is_empty() {
        "thread".into()
    } else {
        segment
    }
}

impl AttachmentStore {
    pub(crate) fn new(home: &Path) -> Self {
        Self {
            root: home.join("attachments"),
            uploads: Arc::default(),
            assets: Arc::default(),
        }
    }

    pub(crate) async fn issue_upload(&self, input: &Value) -> Result<Value> {
        let kind = input["type"].as_str().unwrap_or("image");
        ensure!(
            matches!(kind, "image" | "file"),
            "Unsupported attachment type"
        );
        let name = crate::store::required(input, "name")?;
        let mime_type = crate::store::required(input, "mimeType")?;
        let size = input["sizeBytes"]
            .as_u64()
            .context("Invalid attachment size")?;
        let limit = if kind == "image" {
            MAX_IMAGE_BYTES
        } else {
            MAX_FILE_BYTES
        };
        ensure!(
            size > 0 && size <= limit,
            "Attachment exceeds the upload limit"
        );
        if kind == "image" {
            ensure!(
                matches!(
                    mime_type,
                    "image/gif" | "image/jpeg" | "image/png" | "image/webp"
                ),
                "Unsupported image type"
            );
        }
        let attachment_id = format!("pending-{}", uuid::Uuid::new_v4());
        let token = uuid::Uuid::new_v4().to_string();
        let expires_at = now_ms() + UPLOAD_TTL_MS;
        let mut uploads = self.uploads.lock().await;
        uploads.retain(|_, claim| claim.expires_at > now_ms());
        uploads.insert(
            token.clone(),
            UploadClaim {
                attachment_id: attachment_id.clone(),
                extension: extension(name, mime_type, kind),
                size,
                expires_at,
            },
        );
        Ok(json!({
            "attachmentId":attachment_id,
            "relativeUrl":format!("/api/attachments/upload/{token}"),
            "expiresAt":expires_at
        }))
    }

    pub(crate) async fn store_upload(&self, token: &str, bytes: &[u8]) -> Result<bool> {
        let claim = self.uploads.lock().await.remove(token);
        let Some(claim) = claim else { return Ok(false) };
        ensure!(claim.expires_at > now_ms(), "Upload URL expired");
        ensure!(
            bytes.len() as u64 == claim.size,
            "Upload size does not match"
        );
        tokio::fs::create_dir_all(&self.root).await?;
        let final_path = self
            .root
            .join(format!("{}{}", claim.attachment_id, claim.extension));
        let temporary = self.root.join(format!(
            "{}.{}.part",
            claim.attachment_id,
            uuid::Uuid::new_v4()
        ));
        tokio::fs::write(&temporary, bytes).await?;
        if let Err(error) = tokio::fs::rename(&temporary, &final_path).await {
            let _ = tokio::fs::remove_file(&temporary).await;
            return Err(error.into());
        }
        Ok(true)
    }

    async fn find(&self, id: &str) -> Result<Option<PathBuf>> {
        if !safe_id(id) {
            return Ok(None);
        }
        let mut entries = match tokio::fs::read_dir(&self.root).await {
            Ok(entries) => entries,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
            Err(error) => return Err(error.into()),
        };
        let prefix = format!("{id}.");
        while let Some(entry) = entries.next_entry().await? {
            if entry.file_name().to_string_lossy().starts_with(&prefix)
                && entry.file_type().await?.is_file()
            {
                return Ok(Some(entry.path()));
            }
        }
        Ok(None)
    }

    pub(crate) async fn delete_pending(&self, id: &str) -> Result<()> {
        if !id.starts_with("pending-") {
            return Ok(());
        }
        if let Some(path) = self.find(id).await? {
            let _ = tokio::fs::remove_file(path).await;
        }
        Ok(())
    }

    pub(crate) async fn claim(&self, thread_id: &str, attachments: &mut Value) -> Result<()> {
        let values = attachments
            .as_array_mut()
            .context("Attachments must be an array")?;
        ensure!(values.len() <= 100, "Too many attachments");
        let mut image_bytes = 0_u64;
        for attachment in values {
            let kind = crate::store::required(attachment, "type")?;
            ensure!(
                matches!(kind, "image" | "file"),
                "Unsupported attachment type"
            );
            let id = crate::store::required(attachment, "id")?.to_owned();
            let name = crate::store::required(attachment, "name")?;
            let mime_type = crate::store::required(attachment, "mimeType")?;
            let declared = attachment["sizeBytes"]
                .as_u64()
                .context("Invalid attachment size")?;
            if kind == "image" {
                image_bytes = image_bytes.saturating_add(declared);
            }
            ensure!(
                image_bytes <= 80 * 1024 * 1024,
                "Images exceed the total attachment limit"
            );
            let suffix = extension(name, mime_type, kind);
            if id.starts_with("pending-") {
                let final_id = format!(
                    "{}-{}",
                    thread_segment(thread_id),
                    id.trim_start_matches("pending-")
                );
                let pending = self.root.join(format!("{id}{suffix}"));
                let final_path = self.root.join(format!("{final_id}{suffix}"));
                if tokio::fs::metadata(&final_path).await.is_err() {
                    ensure!(
                        tokio::fs::metadata(&pending).await.is_ok(),
                        "Attachment upload is missing"
                    );
                    tokio::fs::copy(&pending, &final_path).await?;
                }
                ensure!(
                    tokio::fs::metadata(&final_path).await?.len() == declared,
                    "Attachment size does not match"
                );
                attachment["id"] = json!(final_id);
            } else {
                let path = self.find(&id).await?.context("Attachment is missing")?;
                ensure!(
                    tokio::fs::metadata(path).await?.len() == declared,
                    "Attachment size does not match"
                );
            }
        }
        Ok(())
    }

    pub(crate) async fn prompt(
        &self,
        text: String,
        attachments: &Value,
    ) -> Result<Vec<ContentBlock>> {
        let mut blocks = vec![ContentBlock::Text(TextContent::new(text))];
        for attachment in attachments
            .as_array()
            .context("Attachments must be an array")?
        {
            let id = crate::store::required(attachment, "id")?;
            let kind = crate::store::required(attachment, "type")?;
            let name = crate::store::required(attachment, "name")?;
            let mime_type = crate::store::required(attachment, "mimeType")?;
            let path = self.find(id).await?.context("Attachment is missing")?;
            let metadata = tokio::fs::metadata(&path).await?;
            if kind == "image" {
                ensure!(
                    metadata.len() <= MAX_IMAGE_BYTES,
                    "Image exceeds the attachment limit"
                );
                let data = STANDARD.encode(tokio::fs::read(&path).await?);
                let uri = url::Url::from_file_path(&path)
                    .ok()
                    .map(|uri| uri.to_string());
                blocks.push(ContentBlock::Image(
                    ImageContent::new(data, mime_type).uri(uri),
                ));
            } else {
                let uri = url::Url::from_file_path(&path)
                    .map_err(|_| anyhow::anyhow!("Invalid attachment path"))?;
                blocks.push(ContentBlock::ResourceLink(
                    ResourceLink::new(name, uri.to_string())
                        .mime_type(mime_type.to_owned())
                        .size(metadata.len() as i64),
                ));
            }
        }
        Ok(blocks)
    }

    pub(crate) async fn issue_asset(&self, input: &Value) -> Result<Value> {
        let resource = &input["resource"];
        ensure!(
            resource["_tag"] == "attachment",
            "Only attachment assets are supported"
        );
        let id = crate::store::required(resource, "attachmentId")?;
        let path = self.find(id).await?.context("Attachment not found")?;
        let token = uuid::Uuid::new_v4().to_string();
        let expires_at = now_ms() + ASSET_TTL_MS;
        let file_name = resource["fileName"].as_str().unwrap_or(id).to_owned();
        let mime_type = resource["mimeType"]
            .as_str()
            .unwrap_or("application/octet-stream")
            .to_owned();
        let disposition = resource["disposition"]
            .as_str()
            .unwrap_or("inline")
            .to_owned();
        let mut assets = self.assets.lock().await;
        assets.retain(|_, claim| claim.expires_at > now_ms());
        assets.insert(
            token.clone(),
            AssetClaim {
                path,
                mime_type,
                file_name,
                disposition,
                expires_at,
            },
        );
        Ok(json!({"relativeUrl":format!("/api/assets/{token}"),"expiresAt":expires_at}))
    }

    pub(crate) async fn read_asset(&self, token: &str) -> Result<Option<AssetFile>> {
        let claim = self.assets.lock().await.get(token).cloned();
        let Some(claim) = claim else { return Ok(None) };
        if claim.expires_at <= now_ms() {
            return Ok(None);
        }
        Ok(Some(AssetFile {
            bytes: tokio::fs::read(claim.path).await?,
            mime_type: claim.mime_type,
            file_name: claim.file_name,
            disposition: claim.disposition,
        }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn uploads_claims_and_builds_acp_image_content() -> Result<()> {
        let home = tempfile::tempdir()?;
        let store = AttachmentStore::new(home.path());
        let issued = store
            .issue_upload(
                &json!({"type":"image","name":"pixel.png","mimeType":"image/png","sizeBytes":4}),
            )
            .await?;
        let token = issued["relativeUrl"]
            .as_str()
            .unwrap()
            .rsplit('/')
            .next()
            .unwrap();
        assert!(store.store_upload(token, b"data").await?);
        let mut attachments = json!([{"type":"image","id":issued["attachmentId"],"name":"pixel.png","mimeType":"image/png","sizeBytes":4}]);
        store.claim("thread", &mut attachments).await?;
        assert!(
            attachments[0]["id"]
                .as_str()
                .unwrap()
                .starts_with("thread-")
        );
        let blocks = store.prompt("look".into(), &attachments).await?;
        assert_eq!(blocks.len(), 2);
        assert!(matches!(blocks[1], ContentBlock::Image(_)));

        let issued = store
            .issue_upload(
                &json!({"type":"file","name":"notes.txt","mimeType":"text/plain","sizeBytes":5}),
            )
            .await?;
        let token = issued["relativeUrl"]
            .as_str()
            .unwrap()
            .rsplit('/')
            .next()
            .unwrap();
        assert!(store.store_upload(token, b"notes").await?);
        let mut files = json!([{"type":"file","id":issued["attachmentId"],"name":"notes.txt","mimeType":"text/plain","sizeBytes":5}]);
        store.claim("thread", &mut files).await?;
        let blocks = store.prompt("read".into(), &files).await?;
        assert!(matches!(blocks[1], ContentBlock::ResourceLink(_)));
        Ok(())
    }
}
