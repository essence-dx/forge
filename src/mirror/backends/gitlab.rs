//! GitLab backend - pushes files into a project via the repository files API.
use std::sync::Arc;

use async_trait::async_trait;
use base64::{engine::general_purpose::STANDARD, Engine};

use crate::mirror::{
    auth::AuthStore, MediaType, MirrorBackend, MirrorError, MirrorMetadata, MirrorTarget,
};

pub struct GitLabBackend {
    auth: Arc<AuthStore>,
    /// "group/project" path
    project: String,
}

impl GitLabBackend {
    pub fn new(auth: Arc<AuthStore>, project: String) -> Self {
        Self { auth, project }
    }
}

#[async_trait]
impl MirrorBackend for GitLabBackend {
    fn name(&self) -> &'static str {
        "gitlab"
    }

    fn can_handle(&self, media_type: &MediaType) -> bool {
        matches!(
            media_type,
            MediaType::Code | MediaType::Document | MediaType::Archive | MediaType::Unknown
        )
    }

    async fn upload(
        &self,
        data: Vec<u8>,
        meta: &MirrorMetadata,
    ) -> Result<MirrorTarget, MirrorError> {
        let bundle = self
            .auth
            .load("gitlab")
            .map_err(|error| MirrorError::Upload(error.to_string()))?
            .ok_or(MirrorError::AuthMissing("gitlab"))?;

        let file_path = format!("forge-mirror/{}", meta.filename);
        let encoded_project = encode_path_component(&self.project);
        let encoded_file_path = encode_path_component(&file_path);
        let api_url = format!(
            "https://gitlab.com/api/v4/projects/{encoded_project}/repository/files/{encoded_file_path}"
        );
        let client = reqwest::Client::new();

        let body = serde_json::json!({
            "branch": "main",
            "commit_message": format!("forge mirror: {}", meta.filename),
            "content": STANDARD.encode(&data),
            "encoding": "base64",
        });

        let update_response = client
            .put(&api_url)
            .header("PRIVATE-TOKEN", &bundle.access_token)
            .json(&body)
            .send()
            .await?;

        let response = if update_response.status().is_success() {
            update_response
        } else {
            let create_response = client
                .post(&api_url)
                .header("PRIVATE-TOKEN", &bundle.access_token)
                .json(&body)
                .send()
                .await?;
            create_response
        };

        if !response.status().is_success() {
            let message = response.text().await.unwrap_or_default();
            return Err(MirrorError::Upload(format!(
                "GitLab upload failed: {message}"
            )));
        }

        Ok(MirrorTarget::GitLab {
            project: self.project.clone(),
            path: file_path,
        })
    }
}

fn encode_path_component(value: &str) -> String {
    value
        .bytes()
        .flat_map(|byte| match byte {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                vec![byte as char]
            }
            _ => format!("%{:02X}", byte).chars().collect(),
        })
        .collect()
}
