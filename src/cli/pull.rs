use std::collections::BTreeSet;
use std::fs;
use std::path::PathBuf;

use anyhow::{anyhow, bail, Context, Result};
use object_store::{aws::AmazonS3Builder, path::Path as ObjectPath, ObjectStore};

use crate::core::repository::Repository;
use crate::db::metadata::MetadataDb;
use crate::jobs::{
    load_job, queue_job, update_job_status, JobCheckpoint, JobKind, JobStatus, QueueJobRequest,
    RetryPolicy,
};
use crate::mirror::auth::AuthStore;
use crate::mirror::{can_restore_record, decode_records, ordered_pull_records, StoredMirrorRecord};

pub fn run(remote: &str) -> Result<()> {
    let cwd = std::env::current_dir().context("get cwd")?;
    let repo = Repository::discover(&cwd)?;
    run_for_repo(&repo, remote)
}

pub(crate) fn run_for_repo(repo: &Repository, remote: &str) -> Result<()> {
    let db = MetadataDb::open(&repo.metadata_db_path())?;
    let job = queue_job(
        &db,
        QueueJobRequest {
            kind: JobKind::MirrorPull,
            description: format!("pull mirrored files from remote '{remote}'"),
            remote: Some(remote.to_string()),
            commit_id: None,
            retry_policy: RetryPolicy::default(),
            checkpoint: Some(JobCheckpoint {
                stage: "queued".to_string(),
                cursor: None,
                completed_actions: Vec::new(),
                metrics: Some(pull_job_metrics(remote, None)),
            }),
        },
    )?;
    run_with_job(repo, remote, &job.id)
}

pub(crate) fn run_with_job(repo: &Repository, remote: &str, job_id: &str) -> Result<()> {
    let db = MetadataDb::open(&repo.metadata_db_path())?;
    let (completed_actions, metrics) = load_pull_job_progress_snapshot(&db, job_id)?;
    let _ = update_job_status(
        &db,
        job_id,
        JobStatus::Running,
        Some(JobCheckpoint {
            stage: "preparing".to_string(),
            cursor: None,
            completed_actions,
            metrics: Some(pull_job_metrics(remote, metrics)),
        }),
        None,
    );

    let result = execute_pull(repo, &db, remote, job_id);
    match result {
        Ok(summary) => {
            let (completed_actions, metrics) = load_pull_job_progress_snapshot(&db, job_id)?;
            let final_status = if summary.failed == 0 {
                JobStatus::Succeeded
            } else {
                JobStatus::Failed
            };
            let _ = update_job_status(
                &db,
                job_id,
                final_status,
                Some(JobCheckpoint {
                    stage: "completed".to_string(),
                    cursor: None,
                    completed_actions,
                    metrics: Some(pull_job_metrics(
                        remote,
                        merge_pull_metrics(
                            metrics,
                            Some(serde_json::json!({
                                "restored": summary.restored,
                                "failed": summary.failed,
                                "skipped": summary.skipped,
                            })),
                        ),
                    )),
                }),
                (summary.failed > 0)
                    .then(|| format!("{} mirrored file(s) failed to restore", summary.failed)),
            );
            Ok(())
        }
        Err(error) => {
            let (completed_actions, metrics) = load_pull_job_progress_snapshot(&db, job_id)?;
            let _ = update_job_status(
                &db,
                job_id,
                JobStatus::Failed,
                Some(JobCheckpoint {
                    stage: "failed".to_string(),
                    cursor: None,
                    completed_actions,
                    metrics: Some(pull_job_metrics(remote, metrics)),
                }),
                Some(error.to_string()),
            );
            Err(error)
        }
    }
}

struct PullSummary {
    restored: usize,
    failed: usize,
    skipped: usize,
}

fn pull_job_metrics(remote: &str, extra: Option<serde_json::Value>) -> serde_json::Value {
    let mut metrics = serde_json::Map::new();
    metrics.insert(
        "remote".to_string(),
        serde_json::Value::String(remote.to_string()),
    );
    if let Some(serde_json::Value::Object(extra_map)) = extra {
        metrics.extend(extra_map);
    }
    serde_json::Value::Object(metrics)
}

fn execute_pull(
    repo: &Repository,
    db: &MetadataDb,
    remote: &str,
    job_id: &str,
) -> Result<PullSummary> {
    let auth = AuthStore::open(&repo.forge_dir)?;
    let all_targets = db.get_all_mirror_targets()?;
    if all_targets.is_empty() {
        println!("No mirror targets found. Push first with `forge push --mirror all-free`.");
        return Ok(PullSummary {
            restored: 0,
            failed: 0,
            skipped: 0,
        });
    }

    let rt = tokio::runtime::Runtime::new().context("create tokio runtime")?;
    let client = reqwest::Client::new();

    let mut restored = 0usize;
    let mut failed = 0usize;
    let mut skipped = 0usize;
    let mut completed_paths = load_pull_completed_paths(db, job_id)?;

    println!(
        "Pulling {} mirrored file(s)... (remote: {remote})",
        all_targets.len()
    );

    for (file_path, json_bytes) in &all_targets {
        if completed_paths.contains(file_path) {
            println!(
                "  skip {} (already restored in previous attempt)",
                file_path
            );
            skipped += 1;
            continue;
        }

        let records = decode_records(json_bytes)
            .with_context(|| format!("parse mirror records for {file_path}"))?;
        let ordered = ordered_pull_records(records, Some(remote));
        let restorable: Vec<StoredMirrorRecord> =
            ordered.into_iter().filter(can_restore_record).collect();

        if restorable.is_empty() {
            eprintln!("  ! {} - no restorable mirrors recorded", file_path);
            skipped += 1;
            persist_pull_progress(
                db,
                job_id,
                remote,
                PullProgress {
                    restored,
                    failed,
                    skipped,
                    completed_paths: &completed_paths,
                    current_file: Some(file_path.as_str()),
                },
            )?;
            continue;
        }

        let mut success = false;
        let mut last_error = None;

        for record in &restorable {
            match rt.block_on(download_record(&client, &auth, record)) {
                Ok(data) => {
                    if let Some(expected_size) = record.size_bytes {
                        if data.len() as u64 != expected_size {
                            last_error = Some(format!(
                                "size mismatch from {}: expected {}, got {}",
                                restore_source_label(record),
                                expected_size,
                                data.len()
                            ));
                            continue;
                        }
                    }

                    if let Some(expected_hash) = record.file_hash_blake3.as_deref() {
                        let actual_hash = hex::encode(blake3::hash(&data).as_bytes());
                        if actual_hash != expected_hash {
                            last_error = Some(format!(
                                "hash mismatch from {}: expected {}, got {}",
                                restore_source_label(record),
                                expected_hash,
                                actual_hash
                            ));
                            continue;
                        }
                    }

                    let out_path = repo.root.join(file_path);
                    write_restored_file(out_path, &data)
                        .with_context(|| format!("restore {file_path}"))?;
                    println!("  ok {} <- {} ({})", file_path, record.backend, record.url);
                    restored += 1;
                    completed_paths.insert(file_path.clone());
                    persist_pull_progress(
                        db,
                        job_id,
                        remote,
                        PullProgress {
                            restored,
                            failed,
                            skipped,
                            completed_paths: &completed_paths,
                            current_file: Some(file_path.as_str()),
                        },
                    )?;
                    success = true;
                    break;
                }
                Err(error) => {
                    last_error = Some(error.to_string());
                }
            }
        }

        if !success {
            failed += 1;
            if let Some(error) = last_error {
                eprintln!("  ! {} - {}", file_path, error);
            } else {
                eprintln!(
                    "  ! {} - all {} mirror(s) failed",
                    file_path,
                    restorable.len()
                );
            }
            persist_pull_progress(
                db,
                job_id,
                remote,
                PullProgress {
                    restored,
                    failed,
                    skipped,
                    completed_paths: &completed_paths,
                    current_file: Some(file_path.as_str()),
                },
            )?;
        }
    }

    println!();
    if failed == 0 {
        println!("Pull complete: {restored} file(s) restored, {skipped} skipped.");
    } else {
        println!("Pull finished: {restored} restored, {failed} failed, {skipped} skipped.");
    }

    Ok(PullSummary {
        restored,
        failed,
        skipped,
    })
}

fn load_pull_completed_paths(db: &MetadataDb, job_id: &str) -> Result<BTreeSet<String>> {
    Ok(load_job(db, job_id)?
        .and_then(|job| job.checkpoint)
        .map(|checkpoint| checkpoint.completed_actions.into_iter().collect())
        .unwrap_or_default())
}

fn load_pull_job_progress_snapshot(
    db: &MetadataDb,
    job_id: &str,
) -> Result<(Vec<String>, Option<serde_json::Value>)> {
    let checkpoint = load_job(db, job_id)?.and_then(|job| job.checkpoint);
    Ok(match checkpoint {
        Some(checkpoint) => (checkpoint.completed_actions, checkpoint.metrics),
        None => (Vec::new(), None),
    })
}

fn merge_pull_metrics(
    base: Option<serde_json::Value>,
    extra: Option<serde_json::Value>,
) -> Option<serde_json::Value> {
    match (base, extra) {
        (
            Some(serde_json::Value::Object(mut base_map)),
            Some(serde_json::Value::Object(extra_map)),
        ) => {
            base_map.extend(extra_map);
            Some(serde_json::Value::Object(base_map))
        }
        (Some(value), None) | (None, Some(value)) => Some(value),
        (Some(_), Some(value)) => Some(value),
        (None, None) => None,
    }
}

struct PullProgress<'a> {
    restored: usize,
    failed: usize,
    skipped: usize,
    completed_paths: &'a BTreeSet<String>,
    current_file: Option<&'a str>,
}

fn persist_pull_progress(
    db: &MetadataDb,
    job_id: &str,
    remote: &str,
    progress: PullProgress<'_>,
) -> Result<()> {
    let _ = update_job_status(
        db,
        job_id,
        JobStatus::Running,
        Some(JobCheckpoint {
            stage: "restoring".to_string(),
            cursor: progress.current_file.map(str::to_string),
            completed_actions: progress.completed_paths.iter().cloned().collect(),
            metrics: Some(pull_job_metrics(
                remote,
                Some(serde_json::json!({
                    "restored": progress.restored,
                    "failed": progress.failed,
                    "skipped": progress.skipped,
                    "completed_files": progress.completed_paths.len(),
                    "current_file": progress.current_file,
                })),
            )),
        }),
        None,
    );
    Ok(())
}

async fn download_record(
    client: &reqwest::Client,
    auth: &AuthStore,
    record: &StoredMirrorRecord,
) -> Result<Vec<u8>> {
    let structured_restore = match record.backend.as_str() {
        "github" => Some(download_github(client, auth, record).await),
        "gitlab" => Some(download_gitlab(client, auth, record).await),
        "bitbucket" => Some(download_bitbucket(client, auth, record).await),
        "gdrive" => Some(download_gdrive(client, auth, record).await),
        "dropbox" => Some(download_dropbox(client, auth, record).await),
        "r2" => Some(download_r2(auth, record).await),
        _ => None,
    };

    match structured_restore {
        Some(Ok(data)) => Ok(data),
        Some(Err(error)) => {
            if let Some(download_url) = record.download_url.as_deref() {
                let error_text = error.to_string();
                try_download(client, download_url).await.with_context(|| {
                    format!(
                        "structured restore via {} failed: {}; fallback GET {} also failed",
                        record.backend, error_text, download_url
                    )
                })
            } else {
                Err(error)
            }
        }
        None => {
            let download_url = record.download_url.as_deref().ok_or_else(|| {
                anyhow!(
                    "{} mirror has no download URL or structured restore locator",
                    record.backend
                )
            })?;
            try_download(client, download_url).await
        }
    }
}

async fn try_download(client: &reqwest::Client, url: &str) -> Result<Vec<u8>> {
    let response = client
        .get(url)
        .header("User-Agent", "forge/0.1")
        .send()
        .await
        .context("HTTP request")?;

    if !response.status().is_success() {
        anyhow::bail!("HTTP {} from {}", response.status(), url);
    }

    let bytes = response.bytes().await.context("read body")?;
    Ok(bytes.to_vec())
}

async fn download_github(
    client: &reqwest::Client,
    auth: &AuthStore,
    record: &StoredMirrorRecord,
) -> Result<Vec<u8>> {
    let repository = required_record_field(record, "repository", record.repository.as_deref())?;
    let artifact_path =
        required_record_field(record, "artifact_path", record.artifact_path.as_deref())?;
    let bundle = auth
        .load("github")
        .context("load github auth")?
        .ok_or_else(|| anyhow!("github auth missing"))?;
    let api_url = format!("https://api.github.com/repos/{repository}/contents/{artifact_path}");
    let response = client
        .get(&api_url)
        .bearer_auth(&bundle.access_token)
        .header("User-Agent", "forge/0.1")
        .header("Accept", "application/vnd.github.raw")
        .send()
        .await
        .context("GitHub contents request")?;

    read_success_bytes(response, "GitHub restore")
        .await
        .with_context(|| format!("restore {artifact_path} from GitHub repo {repository}"))
}

async fn download_gitlab(
    client: &reqwest::Client,
    auth: &AuthStore,
    record: &StoredMirrorRecord,
) -> Result<Vec<u8>> {
    let repository = required_record_field(record, "repository", record.repository.as_deref())?;
    let artifact_path =
        required_record_field(record, "artifact_path", record.artifact_path.as_deref())?;
    let bundle = auth
        .load("gitlab")
        .context("load gitlab auth")?
        .ok_or_else(|| anyhow!("gitlab auth missing"))?;
    let api_url = format!(
        "https://gitlab.com/api/v4/projects/{}/repository/files/{}/raw?ref=main",
        encode_path_component(repository),
        encode_path_component(artifact_path)
    );
    let response = client
        .get(&api_url)
        .header("PRIVATE-TOKEN", &bundle.access_token)
        .send()
        .await
        .context("GitLab repository files request")?;

    read_success_bytes(response, "GitLab restore")
        .await
        .with_context(|| format!("restore {artifact_path} from GitLab project {repository}"))
}

async fn download_bitbucket(
    client: &reqwest::Client,
    auth: &AuthStore,
    record: &StoredMirrorRecord,
) -> Result<Vec<u8>> {
    let repository = required_record_field(record, "repository", record.repository.as_deref())?;
    let artifact_path =
        required_record_field(record, "artifact_path", record.artifact_path.as_deref())?;
    let bundle = auth
        .load("bitbucket")
        .context("load bitbucket auth")?
        .ok_or_else(|| anyhow!("bitbucket auth missing"))?;
    let api_url = format!(
        "https://api.bitbucket.org/2.0/repositories/{repository}/src/main/{}",
        encode_path_segments(artifact_path)
    );
    let response = client
        .get(&api_url)
        .bearer_auth(&bundle.access_token)
        .send()
        .await
        .context("Bitbucket source request")?;

    read_success_bytes(response, "Bitbucket restore")
        .await
        .with_context(|| format!("restore {artifact_path} from Bitbucket repository {repository}"))
}

async fn download_gdrive(
    client: &reqwest::Client,
    auth: &AuthStore,
    record: &StoredMirrorRecord,
) -> Result<Vec<u8>> {
    let file_id = required_record_field(record, "artifact_path", record.artifact_path.as_deref())?;
    let bundle = auth
        .load("gdrive")
        .context("load gdrive auth")?
        .ok_or_else(|| anyhow!("gdrive auth missing"))?;
    let api_url = format!("https://www.googleapis.com/drive/v3/files/{file_id}?alt=media");
    let response = client
        .get(&api_url)
        .bearer_auth(&bundle.access_token)
        .send()
        .await
        .context("Google Drive download request")?;

    read_success_bytes(response, "Google Drive restore")
        .await
        .with_context(|| format!("restore Google Drive file {file_id}"))
}

async fn download_dropbox(
    client: &reqwest::Client,
    auth: &AuthStore,
    record: &StoredMirrorRecord,
) -> Result<Vec<u8>> {
    let dropbox_path =
        required_record_field(record, "artifact_path", record.artifact_path.as_deref())?;
    let bundle = auth
        .load("dropbox")
        .context("load dropbox auth")?
        .ok_or_else(|| anyhow!("dropbox auth missing"))?;
    let api_arg = serde_json::to_string(&serde_json::json!({ "path": dropbox_path }))
        .context("serialize Dropbox API argument")?;
    let response = client
        .post("https://content.dropboxapi.com/2/files/download")
        .bearer_auth(&bundle.access_token)
        .header("Dropbox-API-Arg", api_arg)
        .send()
        .await
        .context("Dropbox download request")?;

    read_success_bytes(response, "Dropbox restore")
        .await
        .with_context(|| format!("restore Dropbox path {dropbox_path}"))
}

async fn download_r2(auth: &AuthStore, record: &StoredMirrorRecord) -> Result<Vec<u8>> {
    let artifact_path =
        required_record_field(record, "artifact_path", record.artifact_path.as_deref())?;
    let bundle = auth
        .load("r2")
        .context("load r2 auth")?
        .ok_or_else(|| anyhow!("r2 auth missing"))?;
    let bucket = record
        .repository
        .as_deref()
        .or_else(|| bundle.extra["bucket"].as_str())
        .ok_or_else(|| anyhow!("r2 mirror missing bucket"))?;
    let endpoint = bundle.extra["endpoint"]
        .as_str()
        .ok_or_else(|| anyhow!("r2 mirror missing endpoint"))?;
    let access_key = bundle.extra["access_key_id"]
        .as_str()
        .ok_or_else(|| anyhow!("r2 mirror missing access_key_id"))?;
    let secret_key = bundle.extra["secret_access_key"]
        .as_str()
        .ok_or_else(|| anyhow!("r2 mirror missing secret_access_key"))?;

    let store = AmazonS3Builder::new()
        .with_bucket_name(bucket)
        .with_endpoint(endpoint)
        .with_access_key_id(access_key)
        .with_secret_access_key(secret_key)
        .build()
        .context("build R2 object store")?;
    let object = store
        .get(&ObjectPath::from(artifact_path.to_string()))
        .await
        .with_context(|| format!("fetch r2://{bucket}/{artifact_path}"))?;
    let bytes = object
        .bytes()
        .await
        .with_context(|| format!("read r2://{bucket}/{artifact_path}"))?;
    Ok(bytes.to_vec())
}

async fn read_success_bytes(response: reqwest::Response, label: &str) -> Result<Vec<u8>> {
    let status = response.status();
    if !status.is_success() {
        let body = response.text().await.unwrap_or_default();
        bail!("{label} failed with HTTP {status}: {body}");
    }

    let bytes = response.bytes().await.context("read response body")?;
    Ok(bytes.to_vec())
}

fn restore_source_label(record: &StoredMirrorRecord) -> String {
    record
        .artifact_path
        .clone()
        .or_else(|| record.download_url.clone())
        .unwrap_or_else(|| record.url.clone())
}

fn required_record_field<'a>(
    record: &StoredMirrorRecord,
    field_name: &str,
    value: Option<&'a str>,
) -> Result<&'a str> {
    value.ok_or_else(|| anyhow!("{} mirror missing {}", record.backend, field_name))
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

fn encode_path_segments(path: &str) -> String {
    path.split('/')
        .map(encode_path_component)
        .collect::<Vec<_>>()
        .join("/")
}

fn write_restored_file(out_path: PathBuf, data: &[u8]) -> Result<()> {
    if let Some(parent) = out_path.parent() {
        fs::create_dir_all(parent).with_context(|| format!("create {}", parent.display()))?;
    }
    fs::write(&out_path, data).with_context(|| format!("write {}", out_path.display()))?;
    Ok(())
}
