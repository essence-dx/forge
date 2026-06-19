//! Dropbox backend with simple upload and upload-session support.
use std::sync::Arc;

use async_trait::async_trait;

use crate::mirror::{
    auth::AuthStore, MediaType, MirrorBackend, MirrorError, MirrorMetadata, MirrorTarget,
};

const SIMPLE_LIMIT: usize = 150 * 1024 * 1024;
const SESSION_CHUNK_SIZE: usize = 8 * 1024 * 1024;

pub struct DropboxBackend {
    auth: Arc<AuthStore>,
}

impl DropboxBackend {
    pub fn new(auth: Arc<AuthStore>) -> Self {
        Self { auth }
    }
}

#[async_trait]
impl MirrorBackend for DropboxBackend {
    fn name(&self) -> &'static str {
        "dropbox"
    }

    fn can_handle(&self, _: &MediaType) -> bool {
        true
    }

    async fn upload(
        &self,
        data: Vec<u8>,
        meta: &MirrorMetadata,
    ) -> Result<MirrorTarget, MirrorError> {
        let bundle = self
            .auth
            .load("dropbox")
            .map_err(|error| MirrorError::Upload(error.to_string()))?
            .ok_or(MirrorError::AuthMissing("dropbox"))?;

        let client = reqwest::Client::new();
        let path = format!("/forge-mirror/{}", meta.filename);

        if data.len() > SIMPLE_LIMIT {
            upload_via_session(&client, &bundle.access_token, &path, data).await?;
        } else {
            let arg = serde_json::json!({
                "path": path,
                "mode": "overwrite",
                "autorename": true,
                "mute": true
            });

            let response = client
                .post("https://content.dropboxapi.com/2/files/upload")
                .bearer_auth(&bundle.access_token)
                .header("Dropbox-API-Arg", serde_json::to_string(&arg).unwrap())
                .header("Content-Type", "application/octet-stream")
                .body(data)
                .send()
                .await?;

            if !response.status().is_success() {
                let message = response.text().await.unwrap_or_default();
                return Err(MirrorError::Upload(format!(
                    "Dropbox upload failed: {message}"
                )));
            }
        }

        tracing::info!("Dropbox ok {}", path);
        Ok(MirrorTarget::Dropbox { path })
    }
}

async fn upload_via_session(
    client: &reqwest::Client,
    access_token: &str,
    path: &str,
    data: Vec<u8>,
) -> Result<(), MirrorError> {
    let start_len = SESSION_CHUNK_SIZE.min(data.len());
    let start_response = client
        .post("https://content.dropboxapi.com/2/files/upload_session/start")
        .bearer_auth(access_token)
        .header(
            "Dropbox-API-Arg",
            serde_json::to_string(&serde_json::json!({ "close": false })).unwrap(),
        )
        .header("Content-Type", "application/octet-stream")
        .body(data[..start_len].to_vec())
        .send()
        .await?;

    if !start_response.status().is_success() {
        let message = start_response.text().await.unwrap_or_default();
        return Err(MirrorError::Upload(format!(
            "Dropbox upload session start failed: {message}"
        )));
    }

    let start_json: serde_json::Value = start_response
        .json()
        .await
        .map_err(|error| MirrorError::Upload(format!("Dropbox session parse failed: {error}")))?;
    let session_id = start_json["session_id"]
        .as_str()
        .ok_or_else(|| MirrorError::Upload("Dropbox session_id missing".into()))?
        .to_string();

    let mut offset = start_len;
    while data.len().saturating_sub(offset) > SESSION_CHUNK_SIZE {
        let end = offset + SESSION_CHUNK_SIZE;
        let append_response = client
            .post("https://content.dropboxapi.com/2/files/upload_session/append_v2")
            .bearer_auth(access_token)
            .header(
                "Dropbox-API-Arg",
                serde_json::to_string(&serde_json::json!({
                    "cursor": {
                        "session_id": session_id.as_str(),
                        "offset": offset
                    },
                    "close": false
                }))
                .unwrap(),
            )
            .header("Content-Type", "application/octet-stream")
            .body(data[offset..end].to_vec())
            .send()
            .await?;

        if !append_response.status().is_success() {
            let message = append_response.text().await.unwrap_or_default();
            return Err(MirrorError::Upload(format!(
                "Dropbox session append failed: {message}"
            )));
        }

        offset = end;
    }

    let finish_response = client
        .post("https://content.dropboxapi.com/2/files/upload_session/finish")
        .bearer_auth(access_token)
        .header(
            "Dropbox-API-Arg",
            serde_json::to_string(&serde_json::json!({
                "cursor": {
                    "session_id": session_id.as_str(),
                    "offset": offset
                },
                "commit": {
                    "path": path,
                    "mode": "overwrite",
                    "autorename": true,
                    "mute": true
                }
            }))
            .unwrap(),
        )
        .header("Content-Type", "application/octet-stream")
        .body(data[offset..].to_vec())
        .send()
        .await?;

    if !finish_response.status().is_success() {
        let message = finish_response.text().await.unwrap_or_default();
        return Err(MirrorError::Upload(format!(
            "Dropbox session finish failed: {message}"
        )));
    }

    Ok(())
}
