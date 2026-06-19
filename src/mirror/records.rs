use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};

use crate::mirror::{MediaType, MirrorTarget};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StoredMirrorRecord {
    pub backend: String,
    pub url: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub download_url: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub repository: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub artifact_path: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub remote: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub commit_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub file_path: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub media_type: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub size_bytes: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub file_hash_blake3: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub synced_at_unix_ms: Option<i64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub priority: Option<u16>,
    #[serde(default)]
    pub restorable: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StoredMirrorFailure {
    pub backend: String,
    pub error: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StoredMirrorFile {
    pub path: String,
    pub media_type: String,
    pub size_bytes: u64,
    pub file_hash_blake3: String,
    pub mirrors: Vec<StoredMirrorRecord>,
    #[serde(default)]
    pub failures: Vec<StoredMirrorFailure>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StoredMirrorRun {
    pub version: u32,
    pub commit_id: String,
    pub remote: String,
    pub mirror_mode: String,
    pub created_at_unix_ms: i64,
    pub success_count: usize,
    pub failure_count: usize,
    pub files: Vec<StoredMirrorFile>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct LegacyMirrorRecord {
    backend: String,
    url: String,
}

pub fn media_type_label(media_type: &MediaType) -> &'static str {
    match media_type {
        MediaType::Video => "video",
        MediaType::Image => "image",
        MediaType::Audio => "audio",
        MediaType::Model3D => "model3d",
        MediaType::Code => "code",
        MediaType::Document => "document",
        MediaType::Archive => "archive",
        MediaType::Unknown => "unknown",
    }
}

pub fn record_priority(backend: &str, preferred_remote: Option<&str>) -> u16 {
    let backend_weight = match backend {
        "r2" => 950,
        "gdrive" => 900,
        "dropbox" => 880,
        "github" => 860,
        "gitlab" => 850,
        "bitbucket" => 840,
        "mega" => 820,
        "sketchfab" => 550,
        "youtube" => 520,
        "soundcloud" => 500,
        "pinterest" => 480,
        _ => 400,
    };

    let remote_boost = match preferred_remote {
        Some("origin") | None => 0,
        Some(_) => 10,
    };

    backend_weight + remote_boost
}

pub fn is_restorable_backend(backend: &str) -> bool {
    matches!(
        backend,
        "github" | "gitlab" | "bitbucket" | "gdrive" | "dropbox" | "mega" | "r2"
    )
}

pub fn resolve_download_url(url: &str) -> Option<String> {
    if url.starts_with("http://") || url.starts_with("https://") {
        if url.contains("github.com") && url.contains("/blob/") {
            return Some(
                url.replace("github.com", "raw.githubusercontent.com")
                    .replace("/blob/", "/"),
            );
        }

        if url.contains("drive.google.com/file/d/") {
            if let Some(id) = url
                .strip_prefix("https://drive.google.com/file/d/")
                .and_then(|s| s.split('/').next())
            {
                return Some(format!(
                    "https://drive.google.com/uc?export=download&id={id}"
                ));
            }
        }

        if url.contains("gitlab.com") && url.contains("/-/blob/") {
            return Some(url.replace("/-/blob/", "/-/raw/"));
        }

        if url.contains("bitbucket.org") && url.contains("/src/") {
            return Some(url.replace("/src/", "/raw/"));
        }

        if url.contains("dropbox.com") {
            let base = url.split('?').next().unwrap_or(url);
            return Some(format!("{base}?dl=1"));
        }

        if url.contains("mega.nz") {
            return None;
        }

        return Some(url.to_string());
    }

    None
}

pub fn can_restore_record(record: &StoredMirrorRecord) -> bool {
    record.restorable
        && (record.download_url.as_deref().is_some()
            || has_structured_restore_locator(
                &record.backend,
                record.repository.as_deref(),
                record.artifact_path.as_deref(),
            ))
}

pub fn decode_records(bytes: &[u8]) -> Result<Vec<StoredMirrorRecord>> {
    let raw: serde_json::Value =
        serde_json::from_slice(bytes).context("parse mirror records JSON")?;
    if !is_legacy_mirror_record_array(&raw) {
        return serde_json::from_value(raw).context("parse structured mirror records");
    }

    let legacy: Vec<LegacyMirrorRecord> =
        serde_json::from_value(raw).context("parse legacy mirror records")?;
    Ok(legacy
        .into_iter()
        .map(|record| StoredMirrorRecord {
            download_url: resolve_download_url(&record.url),
            priority: Some(record_priority(&record.backend, None)),
            restorable: is_restorable_backend(&record.backend),
            backend: record.backend,
            url: record.url,
            repository: None,
            artifact_path: None,
            remote: None,
            commit_id: None,
            file_path: None,
            media_type: None,
            size_bytes: None,
            file_hash_blake3: None,
            synced_at_unix_ms: None,
        })
        .collect())
}

fn is_legacy_mirror_record_array(value: &serde_json::Value) -> bool {
    let Some(records) = value.as_array() else {
        return false;
    };
    !records.is_empty()
        && records.iter().all(|record| {
            let Some(object) = record.as_object() else {
                return false;
            };
            object.contains_key("backend")
                && object.contains_key("url")
                && object.keys().all(|key| key == "backend" || key == "url")
        })
}

pub fn encode_records(records: &[StoredMirrorRecord]) -> Result<Vec<u8>> {
    serde_json::to_vec_pretty(records).context("serialize mirror records")
}

pub fn ordered_pull_records(
    mut records: Vec<StoredMirrorRecord>,
    preferred_remote: Option<&str>,
) -> Vec<StoredMirrorRecord> {
    records.sort_by(|left, right| {
        let left_remote_match = preferred_remote
            .and_then(|preferred| left.remote.as_deref().map(|remote| preferred == remote))
            .unwrap_or(false);
        let right_remote_match = preferred_remote
            .and_then(|preferred| right.remote.as_deref().map(|remote| preferred == remote))
            .unwrap_or(false);
        let left_has_restore_path = left.download_url.is_some()
            || has_structured_restore_locator(
                &left.backend,
                left.repository.as_deref(),
                left.artifact_path.as_deref(),
            );
        let right_has_restore_path = right.download_url.is_some()
            || has_structured_restore_locator(
                &right.backend,
                right.repository.as_deref(),
                right.artifact_path.as_deref(),
            );
        let left_score = left
            .priority
            .unwrap_or_else(|| record_priority(&left.backend, preferred_remote));
        let right_score = right
            .priority
            .unwrap_or_else(|| record_priority(&right.backend, preferred_remote));

        right_remote_match
            .cmp(&left_remote_match)
            .then_with(|| right_score.cmp(&left_score))
            .then_with(|| right.restorable.cmp(&left.restorable))
            .then_with(|| right_has_restore_path.cmp(&left_has_restore_path))
            .then_with(|| left.backend.cmp(&right.backend))
    });
    records
}

pub struct MirrorRecordInput<'a> {
    pub backend: &'a str,
    pub target: &'a MirrorTarget,
    pub preferred_remote: &'a str,
    pub commit_id: &'a str,
    pub file_path: &'a str,
    pub media_type: &'a MediaType,
    pub size_bytes: u64,
    pub file_hash_blake3: &'a str,
    pub synced_at_unix_ms: i64,
}

pub fn make_record(input: MirrorRecordInput<'_>) -> StoredMirrorRecord {
    let MirrorRecordInput {
        backend,
        target,
        preferred_remote,
        commit_id,
        file_path,
        media_type,
        size_bytes,
        file_hash_blake3,
        synced_at_unix_ms,
    } = input;

    let url = target.public_url();
    let download_url = resolve_download_url(&url);
    let (repository, artifact_path) = match target {
        MirrorTarget::GitHub { repo, path } => (Some(repo.clone()), Some(path.clone())),
        MirrorTarget::GitLab { project, path } => (Some(project.clone()), Some(path.clone())),
        MirrorTarget::Bitbucket {
            workspace,
            repo,
            path,
        } => (Some(format!("{workspace}/{repo}")), Some(path.clone())),
        MirrorTarget::GoogleDrive { file_id } => (None, Some(file_id.clone())),
        MirrorTarget::Dropbox { path } => (None, Some(path.clone())),
        MirrorTarget::R2 { bucket, key } => (Some(bucket.clone()), Some(key.clone())),
        _ => (None, None),
    };
    let restorable = is_restorable_backend(backend)
        && (download_url.is_some()
            || has_structured_restore_locator(
                backend,
                repository.as_deref(),
                artifact_path.as_deref(),
            ));
    StoredMirrorRecord {
        backend: backend.to_string(),
        url,
        download_url,
        repository,
        artifact_path,
        remote: Some(preferred_remote.to_string()),
        commit_id: Some(commit_id.to_string()),
        file_path: Some(file_path.to_string()),
        media_type: Some(media_type_label(media_type).to_string()),
        size_bytes: Some(size_bytes),
        file_hash_blake3: Some(file_hash_blake3.to_string()),
        synced_at_unix_ms: Some(synced_at_unix_ms),
        priority: Some(record_priority(backend, Some(preferred_remote))),
        restorable,
    }
}

fn has_structured_restore_locator(
    backend: &str,
    repository: Option<&str>,
    artifact_path: Option<&str>,
) -> bool {
    match backend {
        "github" | "gitlab" | "bitbucket" | "r2" => repository.is_some() && artifact_path.is_some(),
        "gdrive" | "dropbox" => artifact_path.is_some(),
        _ => false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn github_blob_urls_become_raw_urls() {
        let url = "https://github.com/acme/repo/blob/main/forge-mirror/file.txt";
        let resolved = resolve_download_url(url).expect("github raw url");
        assert_eq!(
            resolved,
            "https://raw.githubusercontent.com/acme/repo/main/forge-mirror/file.txt"
        );
    }

    #[test]
    fn legacy_records_upgrade_to_structured_records() {
        let legacy =
            br#"[{"backend":"github","url":"https://github.com/acme/repo/blob/main/a.txt"}]"#;
        let decoded = decode_records(legacy).expect("decode legacy records");
        assert_eq!(decoded.len(), 1);
        assert_eq!(decoded[0].backend, "github");
        assert!(decoded[0].restorable);
        assert!(decoded[0].download_url.is_some());
        assert!(decoded[0].repository.is_none());
    }

    #[test]
    fn pull_records_prefer_matching_remote_and_restorable_targets() {
        let preferred = Some("backup");
        let ordered = ordered_pull_records(
            vec![
                StoredMirrorRecord {
                    backend: "youtube".to_string(),
                    url: "https://youtube.com/watch?v=1".to_string(),
                    download_url: None,
                    repository: None,
                    artifact_path: None,
                    remote: Some("origin".to_string()),
                    commit_id: None,
                    file_path: None,
                    media_type: None,
                    size_bytes: None,
                    file_hash_blake3: None,
                    synced_at_unix_ms: None,
                    priority: Some(520),
                    restorable: false,
                },
                StoredMirrorRecord {
                    backend: "github".to_string(),
                    url: "https://github.com/acme/repo/blob/main/a.txt".to_string(),
                    download_url: Some(
                        "https://raw.githubusercontent.com/acme/repo/main/a.txt".to_string(),
                    ),
                    repository: Some("acme/repo".to_string()),
                    artifact_path: Some("a.txt".to_string()),
                    remote: Some("backup".to_string()),
                    commit_id: None,
                    file_path: None,
                    media_type: None,
                    size_bytes: None,
                    file_hash_blake3: None,
                    synced_at_unix_ms: None,
                    priority: Some(860),
                    restorable: true,
                },
            ],
            preferred,
        );

        assert_eq!(ordered[0].backend, "github");
        assert!(ordered[0].restorable);
    }

    #[test]
    fn gitlab_blob_urls_become_raw_urls() {
        let url = "https://gitlab.com/acme/repo/-/blob/main/forge-mirror/file.txt";
        let resolved = resolve_download_url(url).expect("gitlab raw url");
        assert_eq!(
            resolved,
            "https://gitlab.com/acme/repo/-/raw/main/forge-mirror/file.txt"
        );
    }

    #[test]
    fn structured_r2_records_are_restorable_without_public_urls() {
        let target = MirrorTarget::R2 {
            bucket: "forge-bucket".to_string(),
            key: "forge-mirror/file.bin".to_string(),
        };
        let record = make_record(MirrorRecordInput {
            backend: "r2",
            target: &target,
            preferred_remote: "origin",
            commit_id: "deadbeef",
            file_path: "assets/file.bin",
            media_type: &MediaType::Archive,
            size_bytes: 64,
            file_hash_blake3: "abc123",
            synced_at_unix_ms: 1234,
        });

        assert!(record.download_url.is_none());
        assert!(record.restorable);
        assert!(can_restore_record(&record));
    }
}
