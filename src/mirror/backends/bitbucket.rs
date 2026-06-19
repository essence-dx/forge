//! Bitbucket Cloud backend - creates commits by uploading files to the source API.
use std::sync::Arc;

use async_trait::async_trait;

use crate::mirror::{
    auth::AuthStore, MediaType, MirrorBackend, MirrorError, MirrorMetadata, MirrorTarget,
};

pub struct BitbucketBackend {
    auth: Arc<AuthStore>,
    workspace: String,
    repo: String,
}

impl BitbucketBackend {
    pub fn new(auth: Arc<AuthStore>, workspace: String, repo: String) -> Self {
        Self {
            auth,
            workspace,
            repo,
        }
    }
}

#[async_trait]
impl MirrorBackend for BitbucketBackend {
    fn name(&self) -> &'static str {
        "bitbucket"
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
            .load("bitbucket")
            .map_err(|error| MirrorError::Upload(error.to_string()))?
            .ok_or(MirrorError::AuthMissing("bitbucket"))?;

        let file_path = format!("forge-mirror/{}", meta.filename);
        let url = format!(
            "https://api.bitbucket.org/2.0/repositories/{}/{}/src",
            self.workspace, self.repo
        );

        let form = reqwest::multipart::Form::new()
            .text("message", format!("forge mirror: {}", meta.filename))
            .text("branch", "main")
            .part(file_path.clone(), reqwest::multipart::Part::bytes(data));

        let response = reqwest::Client::new()
            .post(&url)
            .bearer_auth(&bundle.access_token)
            .multipart(form)
            .send()
            .await?;

        if !response.status().is_success() {
            let message = response.text().await.unwrap_or_default();
            return Err(MirrorError::Upload(format!(
                "Bitbucket upload failed: {message}"
            )));
        }

        Ok(MirrorTarget::Bitbucket {
            workspace: self.workspace.clone(),
            repo: self.repo.clone(),
            path: file_path,
        })
    }
}
