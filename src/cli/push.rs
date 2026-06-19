use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::path::Path;
use std::sync::Arc;

use anyhow::{bail, Context, Result};
use chrono::Utc;

use crate::core::manifest::{deserialize_commit, FileEntry};
use crate::core::repository::Repository;
use crate::db::metadata::MetadataDb;
use crate::jobs::{
    load_job, queue_job, update_job_status, JobCheckpoint, JobKind, JobStatus, QueueJobRequest,
    RetryPolicy,
};
use crate::mirror::auth::AuthStore;
use crate::mirror::backends::{
    bitbucket::BitbucketBackend, dropbox::DropboxBackend, gdrive::GoogleDriveBackend,
    github::GitHubBackend, gitlab::GitLabBackend, mega::MegaBackend, pinterest::PinterestBackend,
    r2::R2Backend, sketchfab::SketchfabBackend, soundcloud::SoundCloudBackend,
    youtube::YouTubeBackend,
};
use crate::mirror::{
    decode_records, encode_records, make_record, media_type_label, MediaType, MirrorBackend,
    MirrorDispatcher, MirrorRecordInput, StoredMirrorFailure, StoredMirrorFile, StoredMirrorRecord,
    StoredMirrorRun,
};
use crate::store::cas::ChunkStore;
use crate::store::compression;
use crate::sync::{load_remote_registry, RemoteKind};

pub fn run(remote: &str, mirror: Option<&str>, pro: bool) -> Result<()> {
    let cwd = std::env::current_dir().context("get cwd")?;
    let repo = Repository::discover(&cwd)?;
    run_for_repo(&repo, remote, mirror, pro)
}

pub(crate) fn run_for_repo(
    repo: &Repository,
    remote: &str,
    mirror: Option<&str>,
    pro: bool,
) -> Result<()> {
    let mirror_mode = mirror.unwrap_or(if pro { "pro" } else { "all-free" });
    let db = MetadataDb::open(&repo.metadata_db_path())?;
    let head_id = repo
        .read_head()?
        .ok_or_else(|| anyhow::anyhow!("nothing to push - no commits yet"))?;
    let head_hex = hex::encode(head_id);
    let job = queue_job(
        &db,
        QueueJobRequest {
            kind: JobKind::MirrorPush,
            description: format!("push {head_hex} to remote '{remote}' via {mirror_mode}"),
            remote: Some(remote.to_string()),
            commit_id: Some(head_hex.clone()),
            retry_policy: RetryPolicy::default(),
            checkpoint: Some(JobCheckpoint {
                stage: "queued".to_string(),
                cursor: None,
                completed_actions: Vec::new(),
                metrics: Some(push_job_metrics(mirror_mode, pro, None)),
            }),
        },
    )?;
    run_with_job(repo, remote, Some(mirror_mode), pro, &job.id)
}

pub(crate) fn run_with_job(
    repo: &Repository,
    remote: &str,
    mirror: Option<&str>,
    pro: bool,
    job_id: &str,
) -> Result<()> {
    let rt = tokio::runtime::Runtime::new().context("create tokio runtime")?;
    let mirror_mode = mirror.unwrap_or(if pro { "pro" } else { "all-free" });
    let db = MetadataDb::open(&repo.metadata_db_path())?;
    let (completed_actions, metrics) = load_job_progress_snapshot(&db, job_id)?;
    let head_id = repo
        .read_head()?
        .ok_or_else(|| anyhow::anyhow!("nothing to push - no commits yet"))?;
    let head_hex = hex::encode(head_id);

    let _ = update_job_status(
        &db,
        job_id,
        JobStatus::Running,
        Some(JobCheckpoint {
            stage: "preparing".to_string(),
            cursor: Some(head_hex.clone()),
            completed_actions,
            metrics: Some(push_job_metrics(mirror_mode, pro, metrics)),
        }),
        None,
    );

    let result = execute_push(PushExecution {
        repo,
        db: &db,
        runtime: &rt,
        remote,
        mirror_mode,
        head_hex: &head_hex,
        job_id,
        pro,
    });
    match result {
        Ok(summary) => {
            let (completed_actions, metrics) = load_job_progress_snapshot(&db, job_id)?;
            let final_status = if summary.total_err == 0 {
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
                    cursor: Some(head_hex),
                    completed_actions,
                    metrics: Some(push_job_metrics(
                        mirror_mode,
                        pro,
                        merge_metric_objects(
                            metrics,
                            Some(serde_json::json!({
                                "success_count": summary.total_ok,
                                "failure_count": summary.total_err,
                                "file_count": summary.file_count,
                            })),
                        ),
                    )),
                }),
                (summary.total_err > 0)
                    .then(|| format!("{} mirror operation(s) failed", summary.total_err)),
            );
            Ok(())
        }
        Err(error) => {
            let (completed_actions, metrics) = load_job_progress_snapshot(&db, job_id)?;
            let _ = update_job_status(
                &db,
                job_id,
                JobStatus::Failed,
                Some(JobCheckpoint {
                    stage: "failed".to_string(),
                    cursor: Some(head_hex),
                    completed_actions,
                    metrics: Some(push_job_metrics(mirror_mode, pro, metrics)),
                }),
                Some(error.to_string()),
            );
            Err(error)
        }
    }
}

struct PushSummary {
    total_ok: usize,
    total_err: usize,
    file_count: usize,
}

struct PushExecution<'a> {
    repo: &'a Repository,
    db: &'a MetadataDb,
    runtime: &'a tokio::runtime::Runtime,
    remote: &'a str,
    mirror_mode: &'a str,
    head_hex: &'a str,
    job_id: &'a str,
    pro: bool,
}

fn push_job_metrics(
    mirror_mode: &str,
    pro: bool,
    extra: Option<serde_json::Value>,
) -> serde_json::Value {
    let mut metrics = serde_json::Map::new();
    metrics.insert(
        "mirror_mode".to_string(),
        serde_json::Value::String(mirror_mode.to_string()),
    );
    metrics.insert("pro".to_string(), serde_json::Value::Bool(pro));
    if let Some(serde_json::Value::Object(extra_map)) = extra {
        metrics.extend(extra_map);
    }
    serde_json::Value::Object(metrics)
}

fn execute_push(context: PushExecution<'_>) -> Result<PushSummary> {
    let PushExecution {
        repo,
        db,
        runtime,
        remote,
        mirror_mode,
        head_hex,
        job_id,
        pro,
    } = context;

    let manifest_path = repo.forge_dir.join("manifests").join(head_hex);
    let manifest_bytes = fs::read(&manifest_path)
        .with_context(|| format!("read manifest {}", manifest_path.display()))?;
    let commit = deserialize_commit(&manifest_bytes)?;

    println!(
        "Pushing commit {} ({} files) -> mirror: {mirror_mode}",
        &head_hex[..12],
        commit.files.len(),
    );

    let auth = Arc::new(AuthStore::open(&repo.forge_dir)?);
    let backends = build_backends(&auth, mirror_mode, repo, remote)?;

    if backends.is_empty() {
        bail!(
            "No mirror backends available for mode '{mirror_mode}'.\n\
             Run `forge auth all-free` first, or specify `--mirror <backend>`."
        );
    }

    let dispatcher = MirrorDispatcher::new(backends);
    let store = ChunkStore::new(repo.forge_dir.join("objects/chunks"));

    let mut resume = load_push_resume_state(db, remote, head_hex, job_id, mirror_mode)?;

    for entry in &commit.files {
        if resume.completed_paths.contains(&entry.path) {
            println!(
                "  skip {} (already completed in previous attempt)",
                entry.path
            );
            continue;
        }

        let data = reassemble_file_bytes(entry, &store)?;
        let file_path = Path::new(&entry.path);
        let media_type = MediaType::from_path(file_path);
        let file_hash_blake3 = hex::encode(entry.file_hash);
        let synced_at_unix_ms = Utc::now().timestamp_millis();
        let results = runtime.block_on(dispatcher.mirror(file_path, data));

        let mut records = Vec::new();
        let mut failures = Vec::new();
        for result in &results {
            match &result.target {
                Ok(target) => {
                    let record = make_record(MirrorRecordInput {
                        backend: result.backend,
                        target,
                        preferred_remote: remote,
                        commit_id: head_hex,
                        file_path: &entry.path,
                        media_type: &media_type,
                        size_bytes: entry.size,
                        file_hash_blake3: &file_hash_blake3,
                        synced_at_unix_ms,
                    });
                    let restore_note = if record.restorable {
                        "restorable"
                    } else {
                        "publish-only"
                    };
                    println!(
                        "  ok {} -> {} [{}]",
                        result.backend, record.url, restore_note
                    );
                    records.push(record);
                    resume.total_ok += 1;
                }
                Err(error) => {
                    eprintln!("  ! {} - {}", result.backend, error);
                    failures.push(StoredMirrorFailure {
                        backend: result.backend.to_string(),
                        error: error.to_string(),
                    });
                    resume.total_err += 1;
                }
            }
        }

        let stored_file = StoredMirrorFile {
            path: entry.path.clone(),
            media_type: media_type_label(&media_type).to_string(),
            size_bytes: entry.size,
            file_hash_blake3: file_hash_blake3.clone(),
            mirrors: records.clone(),
            failures,
        };
        upsert_run_file(&mut resume.run_files, stored_file);
        if !records.is_empty() {
            merge_remote_records(&mut resume.target_records, &entry.path, remote, records);
        }
        if resume
            .run_files
            .iter()
            .find(|file| file.path == entry.path)
            .map(|file| file.failures.is_empty())
            .unwrap_or(false)
        {
            resume.completed_paths.insert(entry.path.clone());
        }

        persist_push_progress(PushProgress {
            repo,
            db,
            remote,
            mirror_mode,
            head_hex,
            job_id,
            pro,
            resume: &resume,
            current_file: Some(entry.path.as_str()),
        })?;
    }

    let run = StoredMirrorRun {
        version: 1,
        commit_id: head_hex.to_string(),
        remote: remote.to_string(),
        mirror_mode: mirror_mode.to_string(),
        created_at_unix_ms: Utc::now().timestamp_millis(),
        success_count: resume.total_ok,
        failure_count: resume.total_err,
        files: resume.run_files.clone(),
    };
    let run_json = serde_json::to_vec_pretty(&run).context("serialize mirror run")?;
    let encoded_targets = encode_target_record_map(&resume.target_records)?;
    db.replace_mirror_targets(&encoded_targets)?;
    write_mirror_run(repo, db, remote, head_hex, &run_json)?;

    println!();
    if resume.total_err == 0 {
        println!(
            "Push complete: {} mirror(s) across {} files. No errors.",
            resume.total_ok,
            commit.files.len()
        );
    } else {
        println!(
            "Push finished: {} succeeded, {} failed.",
            resume.total_ok, resume.total_err
        );
    }

    if remote != "origin" {
        println!("Recorded mirror run under remote '{remote}'.");
    }

    Ok(PushSummary {
        total_ok: resume.total_ok,
        total_err: resume.total_err,
        file_count: commit.files.len(),
    })
}

struct PushResumeState {
    completed_paths: BTreeSet<String>,
    target_records: BTreeMap<String, Vec<StoredMirrorRecord>>,
    run_files: Vec<StoredMirrorFile>,
    total_ok: usize,
    total_err: usize,
}

fn load_push_resume_state(
    db: &MetadataDb,
    remote: &str,
    head_hex: &str,
    job_id: &str,
    mirror_mode: &str,
) -> Result<PushResumeState> {
    let completed_paths = load_job_completed_paths(db, job_id)?;
    let target_records = decode_target_record_map(db)?;
    let partial_run = load_stored_mirror_run(db, remote, head_hex)?;
    let (run_files, total_ok, total_err) = match partial_run {
        Some(run) if run.remote == remote && run.mirror_mode == mirror_mode => {
            (run.files, run.success_count, run.failure_count)
        }
        _ => (Vec::new(), 0, 0),
    };
    Ok(PushResumeState {
        completed_paths,
        target_records,
        run_files,
        total_ok,
        total_err,
    })
}

fn load_job_completed_paths(db: &MetadataDb, job_id: &str) -> Result<BTreeSet<String>> {
    Ok(load_job(db, job_id)?
        .and_then(|job| job.checkpoint)
        .map(|checkpoint| checkpoint.completed_actions.into_iter().collect())
        .unwrap_or_default())
}

fn load_job_progress_snapshot(
    db: &MetadataDb,
    job_id: &str,
) -> Result<(Vec<String>, Option<serde_json::Value>)> {
    let checkpoint = load_job(db, job_id)?.and_then(|job| job.checkpoint);
    Ok(match checkpoint {
        Some(checkpoint) => (checkpoint.completed_actions, checkpoint.metrics),
        None => (Vec::new(), None),
    })
}

fn merge_metric_objects(
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

fn decode_target_record_map(db: &MetadataDb) -> Result<BTreeMap<String, Vec<StoredMirrorRecord>>> {
    let mut map = BTreeMap::new();
    for (path, bytes) in db.get_all_mirror_targets()? {
        map.insert(path, decode_records(&bytes)?);
    }
    Ok(map)
}

fn merge_remote_records(
    target_records: &mut BTreeMap<String, Vec<StoredMirrorRecord>>,
    file_path: &str,
    remote: &str,
    new_records: Vec<StoredMirrorRecord>,
) {
    let mut merged = target_records.remove(file_path).unwrap_or_default();
    merged.retain(|record| record.remote.as_deref() != Some(remote));
    merged.extend(new_records);
    target_records.insert(file_path.to_string(), merged);
}

fn encode_target_record_map(
    target_records: &BTreeMap<String, Vec<StoredMirrorRecord>>,
) -> Result<Vec<(String, Vec<u8>)>> {
    target_records
        .iter()
        .map(|(path, records)| Ok((path.clone(), encode_records(records)?)))
        .collect()
}

fn upsert_run_file(run_files: &mut Vec<StoredMirrorFile>, file: StoredMirrorFile) {
    run_files.retain(|existing| existing.path != file.path);
    run_files.push(file);
    run_files.sort_by(|left, right| left.path.cmp(&right.path));
}

struct PushProgress<'a> {
    repo: &'a Repository,
    db: &'a MetadataDb,
    remote: &'a str,
    mirror_mode: &'a str,
    head_hex: &'a str,
    job_id: &'a str,
    pro: bool,
    resume: &'a PushResumeState,
    current_file: Option<&'a str>,
}

fn persist_push_progress(progress: PushProgress<'_>) -> Result<()> {
    let run = StoredMirrorRun {
        version: 1,
        commit_id: progress.head_hex.to_string(),
        remote: progress.remote.to_string(),
        mirror_mode: progress.mirror_mode.to_string(),
        created_at_unix_ms: Utc::now().timestamp_millis(),
        success_count: progress.resume.total_ok,
        failure_count: progress.resume.total_err,
        files: progress.resume.run_files.clone(),
    };
    let run_json = serde_json::to_vec_pretty(&run).context("serialize partial mirror run")?;
    let encoded_targets = encode_target_record_map(&progress.resume.target_records)?;
    progress.db.replace_mirror_targets(&encoded_targets)?;
    write_mirror_run(
        progress.repo,
        progress.db,
        progress.remote,
        progress.head_hex,
        &run_json,
    )?;
    let _ = update_job_status(
        progress.db,
        progress.job_id,
        JobStatus::Running,
        Some(JobCheckpoint {
            stage: "mirroring".to_string(),
            cursor: progress.current_file.map(str::to_string),
            completed_actions: progress.resume.completed_paths.iter().cloned().collect(),
            metrics: Some(push_job_metrics(
                progress.mirror_mode,
                progress.pro,
                Some(serde_json::json!({
                    "success_count": progress.resume.total_ok,
                    "failure_count": progress.resume.total_err,
                    "completed_files": progress.resume.completed_paths.len(),
                    "current_file": progress.current_file,
                })),
            )),
        }),
        None,
    );
    Ok(())
}

fn write_mirror_run(
    repo: &Repository,
    db: &MetadataDb,
    remote: &str,
    head_hex: &str,
    run_json: &[u8],
) -> Result<()> {
    fs::create_dir_all(repo.mirrors_dir())
        .with_context(|| format!("create {}", repo.mirrors_dir().display()))?;
    let run_path = repo.mirror_run_path_for_remote(remote, head_hex);
    fs::write(&run_path, run_json)
        .with_context(|| format!("write mirror run summary {}", run_path.display()))?;
    db.store_mirror_run(&mirror_run_key(remote, head_hex), run_json)?;
    Ok(())
}

fn load_stored_mirror_run(
    db: &MetadataDb,
    remote: &str,
    head_hex: &str,
) -> Result<Option<StoredMirrorRun>> {
    let preferred = mirror_run_key(remote, head_hex);
    db.get_mirror_run(&preferred)?
        .or_else(|| db.get_mirror_run(head_hex).ok().flatten())
        .map(|bytes| serde_json::from_slice(&bytes).context("decode stored mirror run"))
        .transpose()
}

fn mirror_run_key(remote: &str, head_hex: &str) -> String {
    format!("{remote}:{head_hex}")
}

fn reassemble_file_bytes(entry: &FileEntry, store: &ChunkStore) -> Result<Vec<u8>> {
    let mut data = Vec::with_capacity(entry.size as usize);
    for chunk_ref in &entry.chunks {
        let hash = blake3::Hash::from(chunk_ref.hash);
        let compressed = store.read(&hash)?;
        let raw = compression::decompress(&compressed)?;
        data.extend_from_slice(&raw);
    }
    Ok(data)
}

/// Assemble the list of backends based on mode string.
fn build_backends(
    auth: &Arc<AuthStore>,
    mode: &str,
    repo: &Repository,
    remote_name: &str,
) -> Result<Vec<Arc<dyn MirrorBackend>>> {
    let mut out: Vec<Arc<dyn MirrorBackend>> = Vec::new();
    let has_auth = |name: &str| -> bool { auth.load(name).ok().flatten().is_some() };

    match mode {
        "all-free" => {
            if has_auth("youtube") {
                out.push(Arc::new(YouTubeBackend::new(Arc::clone(auth))));
            }
            if has_auth("pinterest") {
                out.push(Arc::new(PinterestBackend::new(Arc::clone(auth))));
            }
            if has_auth("soundcloud") {
                out.push(Arc::new(SoundCloudBackend::new(Arc::clone(auth))));
            }
            if has_auth("sketchfab") {
                out.push(Arc::new(SketchfabBackend::new(Arc::clone(auth))));
            }
            if has_auth("github") {
                let github_repo = read_github_repo(repo, auth.as_ref(), remote_name)?;
                out.push(Arc::new(GitHubBackend::new(Arc::clone(auth), github_repo)));
            }
            if has_auth("gitlab") {
                let gitlab_project = read_gitlab_project(repo, auth.as_ref(), remote_name)?;
                out.push(Arc::new(GitLabBackend::new(
                    Arc::clone(auth),
                    gitlab_project,
                )));
            }
            if has_auth("bitbucket") {
                let (workspace, repository) =
                    read_bitbucket_repo(repo, auth.as_ref(), remote_name)?;
                out.push(Arc::new(BitbucketBackend::new(
                    Arc::clone(auth),
                    workspace,
                    repository,
                )));
            }
        }
        "pro" => {
            if has_auth("r2") {
                let r2_info = auth.load("r2")?.unwrap();
                let bucket = r2_info.extra["bucket"]
                    .as_str()
                    .unwrap_or("forge")
                    .to_string();
                let endpoint = r2_info.extra["endpoint"].as_str().unwrap_or("").to_string();
                out.push(Arc::new(R2Backend::new(Arc::clone(auth), bucket, endpoint)));
            }
            if has_auth("gdrive") {
                out.push(Arc::new(GoogleDriveBackend::new(Arc::clone(auth))));
            }
            if has_auth("dropbox") {
                out.push(Arc::new(DropboxBackend::new(Arc::clone(auth))));
            }
            if has_auth("mega") {
                out.push(Arc::new(MegaBackend::new(Arc::clone(auth))));
            }
            if has_auth("youtube") {
                out.push(Arc::new(YouTubeBackend::new(Arc::clone(auth))));
            }
            if has_auth("pinterest") {
                out.push(Arc::new(PinterestBackend::new(Arc::clone(auth))));
            }
            if has_auth("soundcloud") {
                out.push(Arc::new(SoundCloudBackend::new(Arc::clone(auth))));
            }
            if has_auth("sketchfab") {
                out.push(Arc::new(SketchfabBackend::new(Arc::clone(auth))));
            }
            if has_auth("github") {
                let github_repo = read_github_repo(repo, auth.as_ref(), remote_name)?;
                out.push(Arc::new(GitHubBackend::new(Arc::clone(auth), github_repo)));
            }
            if has_auth("gitlab") {
                let gitlab_project = read_gitlab_project(repo, auth.as_ref(), remote_name)?;
                out.push(Arc::new(GitLabBackend::new(
                    Arc::clone(auth),
                    gitlab_project,
                )));
            }
            if has_auth("bitbucket") {
                let (workspace, repository) =
                    read_bitbucket_repo(repo, auth.as_ref(), remote_name)?;
                out.push(Arc::new(BitbucketBackend::new(
                    Arc::clone(auth),
                    workspace,
                    repository,
                )));
            }
        }
        single => match single {
            "youtube" if has_auth("youtube") => {
                out.push(Arc::new(YouTubeBackend::new(Arc::clone(auth))))
            }
            "pinterest" if has_auth("pinterest") => {
                out.push(Arc::new(PinterestBackend::new(Arc::clone(auth))))
            }
            "soundcloud" if has_auth("soundcloud") => {
                out.push(Arc::new(SoundCloudBackend::new(Arc::clone(auth))))
            }
            "sketchfab" if has_auth("sketchfab") => {
                out.push(Arc::new(SketchfabBackend::new(Arc::clone(auth))))
            }
            "github" if has_auth("github") => {
                let github_repo = read_github_repo(repo, auth.as_ref(), remote_name)?;
                out.push(Arc::new(GitHubBackend::new(Arc::clone(auth), github_repo)));
            }
            "gitlab" if has_auth("gitlab") => {
                let gitlab_project = read_gitlab_project(repo, auth.as_ref(), remote_name)?;
                out.push(Arc::new(GitLabBackend::new(
                    Arc::clone(auth),
                    gitlab_project,
                )));
            }
            "bitbucket" if has_auth("bitbucket") => {
                let (workspace, repository) =
                    read_bitbucket_repo(repo, auth.as_ref(), remote_name)?;
                out.push(Arc::new(BitbucketBackend::new(
                    Arc::clone(auth),
                    workspace,
                    repository,
                )));
            }
            "gdrive" if has_auth("gdrive") => {
                out.push(Arc::new(GoogleDriveBackend::new(Arc::clone(auth))))
            }
            "dropbox" if has_auth("dropbox") => {
                out.push(Arc::new(DropboxBackend::new(Arc::clone(auth))))
            }
            "mega" if has_auth("mega") => out.push(Arc::new(MegaBackend::new(Arc::clone(auth)))),
            "r2" if has_auth("r2") => {
                let r2_info = auth.load("r2")?.unwrap();
                let bucket = r2_info.extra["bucket"]
                    .as_str()
                    .unwrap_or("forge")
                    .to_string();
                let endpoint = r2_info.extra["endpoint"].as_str().unwrap_or("").to_string();
                out.push(Arc::new(R2Backend::new(Arc::clone(auth), bucket, endpoint)));
            }
            name => {
                if !has_auth(name) {
                    bail!("Backend '{name}' is not authenticated. Run: forge auth {name}");
                }
                bail!("Unknown backend: '{name}'");
            }
        },
    }

    Ok(out)
}

/// Read the github mirror repo from config.toml or default from `forge auth` extra field.
fn read_github_repo(repo: &Repository, auth: &AuthStore, remote_name: &str) -> Result<String> {
    if let Some(repo_path) =
        read_registered_remote_path(repo, auth, remote_name, RemoteKind::GitHub)?
            .or_else(|| read_remote_path(repo, "github.com").ok().flatten())
    {
        let mut segments = repo_path.split('/').filter(|segment| !segment.is_empty());
        if let (Some(owner), Some(name)) = (segments.next(), segments.next()) {
            return Ok(format!("{owner}/{name}"));
        }
    }
    Ok(format!("{}/forge-mirror", default_remote_owner()))
}

fn read_gitlab_project(repo: &Repository, auth: &AuthStore, remote_name: &str) -> Result<String> {
    if let Some(project) = read_registered_remote_path(repo, auth, remote_name, RemoteKind::GitLab)?
        .or_else(|| read_remote_path(repo, "gitlab.com").ok().flatten())
    {
        return Ok(project);
    }
    Ok(format!("{}/forge-mirror", default_remote_owner()))
}

fn read_bitbucket_repo(
    repo: &Repository,
    auth: &AuthStore,
    remote_name: &str,
) -> Result<(String, String)> {
    if let Some(repo_path) =
        read_registered_remote_path(repo, auth, remote_name, RemoteKind::Bitbucket)?
            .or_else(|| read_remote_path(repo, "bitbucket.org").ok().flatten())
    {
        let mut segments = repo_path.split('/').filter(|segment| !segment.is_empty());
        if let (Some(workspace), Some(name)) = (segments.next(), segments.next()) {
            return Ok((workspace.to_string(), name.to_string()));
        }
    }
    Ok((default_remote_owner(), "forge-mirror".to_string()))
}

fn read_registered_remote_path(
    repo: &Repository,
    auth: &AuthStore,
    remote_name: &str,
    kind: RemoteKind,
) -> Result<Option<String>> {
    let registry = load_remote_registry(repo, auth)?;
    Ok(registry
        .remotes
        .into_iter()
        .find(|remote| remote.name == remote_name && remote.kind == kind && remote.enabled)
        .and_then(|remote| remote_path_for_kind(&remote.locator, &kind)))
}

fn read_remote_path(repo: &Repository, host: &str) -> Result<Option<String>> {
    let cfg = repo.read_config()?;
    Ok(cfg
        .remote_url
        .as_deref()
        .and_then(|url| remote_path_for_host(url, host)))
}

fn remote_path_for_host(url: &str, host: &str) -> Option<String> {
    let trimmed = url.trim();
    let prefixes = [
        format!("https://{host}/"),
        format!("http://{host}/"),
        format!("ssh://git@{host}/"),
        format!("ssh://{host}/"),
        format!("git@{host}:"),
    ];

    for prefix in prefixes {
        if let Some(path) = trimmed.strip_prefix(&prefix) {
            return normalize_remote_path(path);
        }
    }

    None
}

fn normalize_remote_path(path: &str) -> Option<String> {
    let normalized = path
        .split(['?', '#'])
        .next()
        .unwrap_or(path)
        .trim()
        .trim_matches('/')
        .trim_end_matches(".git")
        .to_string();
    if normalized.is_empty() {
        None
    } else {
        Some(normalized)
    }
}

fn remote_path_for_kind(locator: &str, kind: &RemoteKind) -> Option<String> {
    match kind {
        RemoteKind::GitHub => remote_path_for_host(locator, "github.com"),
        RemoteKind::GitLab => remote_path_for_host(locator, "gitlab.com"),
        RemoteKind::Bitbucket => remote_path_for_host(locator, "bitbucket.org"),
        _ => None,
    }
}

fn default_remote_owner() -> String {
    std::env::var("GIT_AUTHOR_NAME")
        .or_else(|_| std::env::var("USER"))
        .or_else(|_| std::env::var("USERNAME"))
        .unwrap_or_else(|_| "forge-user".to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_record(remote: &str, backend: &str) -> StoredMirrorRecord {
        StoredMirrorRecord {
            backend: backend.to_string(),
            url: format!("https://example.com/{remote}/{backend}"),
            download_url: Some(format!("https://download.example.com/{remote}/{backend}")),
            repository: None,
            artifact_path: Some(format!("{remote}/{backend}.bin")),
            remote: Some(remote.to_string()),
            commit_id: Some("deadbeef".to_string()),
            file_path: Some("assets/demo.bin".to_string()),
            media_type: Some("archive".to_string()),
            size_bytes: Some(64),
            file_hash_blake3: Some("abc123".to_string()),
            synced_at_unix_ms: Some(1),
            priority: Some(100),
            restorable: true,
        }
    }

    #[test]
    fn parses_https_github_remote_paths() {
        let parsed = remote_path_for_host("https://github.com/acme/project.git", "github.com");
        assert_eq!(parsed.as_deref(), Some("acme/project"));
    }

    #[test]
    fn parses_ssh_gitlab_remote_paths() {
        let parsed =
            remote_path_for_host("git@gitlab.com:group/subgroup/project.git", "gitlab.com");
        assert_eq!(parsed.as_deref(), Some("group/subgroup/project"));
    }

    #[test]
    fn parses_bitbucket_remote_paths() {
        let parsed = remote_path_for_host(
            "ssh://git@bitbucket.org/workspace/repo.git",
            "bitbucket.org",
        );
        assert_eq!(parsed.as_deref(), Some("workspace/repo"));
    }

    #[test]
    fn merge_remote_records_preserves_other_remotes() {
        let mut targets = BTreeMap::from([(
            "assets/demo.bin".to_string(),
            vec![
                test_record("origin", "github"),
                test_record("backup", "gitlab"),
            ],
        )]);

        merge_remote_records(
            &mut targets,
            "assets/demo.bin",
            "origin",
            vec![test_record("origin", "bitbucket")],
        );

        let merged = targets.get("assets/demo.bin").expect("merged records");
        assert_eq!(merged.len(), 2);
        assert!(merged.iter().any(|record| {
            record.remote.as_deref() == Some("backup") && record.backend == "gitlab"
        }));
        assert!(merged.iter().any(|record| {
            record.remote.as_deref() == Some("origin") && record.backend == "bitbucket"
        }));
        assert!(!merged.iter().any(|record| {
            record.remote.as_deref() == Some("origin") && record.backend == "github"
        }));
    }

    #[test]
    fn mirror_run_key_includes_remote() {
        assert_eq!(mirror_run_key("origin", "abc123"), "origin:abc123");
    }
}
