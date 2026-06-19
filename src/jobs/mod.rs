use anyhow::{anyhow, Context, Result};
use chrono::Utc;
use serde::{Deserialize, Serialize};

use crate::db::metadata::MetadataDb;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum JobKind {
    MirrorPush,
    MirrorPull,
    SyncPlan,
    SyncRun,
    Verify,
    Cleanup,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum JobStatus {
    Queued,
    Running,
    Succeeded,
    Failed,
    Cancelled,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RetryPolicy {
    pub max_attempts: u32,
    pub backoff_seconds: u64,
}

impl Default for RetryPolicy {
    fn default() -> Self {
        Self {
            max_attempts: 3,
            backoff_seconds: 30,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct JobCheckpoint {
    pub stage: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cursor: Option<String>,
    #[serde(default)]
    pub completed_actions: Vec<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub metrics: Option<serde_json::Value>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StoredJob {
    pub id: String,
    pub kind: JobKind,
    pub status: JobStatus,
    pub description: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub remote: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub commit_id: Option<String>,
    pub created_at_unix_ms: i64,
    pub updated_at_unix_ms: i64,
    pub attempts: u32,
    pub retry_policy: RetryPolicy,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub checkpoint: Option<JobCheckpoint>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_error: Option<String>,
}

#[derive(Debug, Clone)]
pub struct QueueJobRequest {
    pub kind: JobKind,
    pub description: String,
    pub remote: Option<String>,
    pub commit_id: Option<String>,
    pub retry_policy: RetryPolicy,
    pub checkpoint: Option<JobCheckpoint>,
}

pub fn queue_job(db: &MetadataDb, request: QueueJobRequest) -> Result<StoredJob> {
    let now = Utc::now().timestamp_millis();
    let seed = format!(
        "{:?}|{}|{}|{}|{}",
        request.kind,
        request.description,
        request.remote.as_deref().unwrap_or(""),
        request.commit_id.as_deref().unwrap_or(""),
        now
    );
    let hash = blake3::hash(seed.as_bytes()).to_hex().to_string();
    let job = StoredJob {
        id: format!("job-{now}-{}", &hash[..12]),
        kind: request.kind,
        status: JobStatus::Queued,
        description: request.description,
        remote: request.remote,
        commit_id: request.commit_id,
        created_at_unix_ms: now,
        updated_at_unix_ms: now,
        attempts: 0,
        retry_policy: request.retry_policy,
        checkpoint: request.checkpoint,
        last_error: None,
    };
    persist_job(db, &job)?;
    Ok(job)
}

pub fn load_job(db: &MetadataDb, job_id: &str) -> Result<Option<StoredJob>> {
    db.get_job(job_id)?
        .map(|bytes| serde_json::from_slice(&bytes).context("decode stored job"))
        .transpose()
}

pub fn list_jobs(db: &MetadataDb) -> Result<Vec<StoredJob>> {
    let mut jobs: Vec<StoredJob> = db
        .list_jobs()?
        .into_iter()
        .map(|(_job_id, bytes)| serde_json::from_slice(&bytes).context("decode stored job"))
        .collect::<Result<Vec<_>>>()?;
    jobs.sort_by(|left, right| right.updated_at_unix_ms.cmp(&left.updated_at_unix_ms));
    Ok(jobs)
}

pub fn update_job_status(
    db: &MetadataDb,
    job_id: &str,
    status: JobStatus,
    checkpoint: Option<JobCheckpoint>,
    last_error: Option<String>,
) -> Result<StoredJob> {
    let mut job = load_job(db, job_id)?.ok_or_else(|| anyhow!("job '{}' not found", job_id))?;

    if matches!(status, JobStatus::Running) && !matches!(job.status, JobStatus::Running) {
        job.attempts = job.attempts.saturating_add(1);
    }
    if checkpoint.is_some() {
        job.checkpoint = checkpoint;
    }
    if matches!(status, JobStatus::Succeeded) {
        job.last_error = None;
    } else if last_error.is_some() {
        job.last_error = last_error;
    }

    job.status = status;
    job.updated_at_unix_ms = Utc::now().timestamp_millis();
    persist_job(db, &job)?;
    Ok(job)
}

pub fn persist_job(db: &MetadataDb, job: &StoredJob) -> Result<()> {
    let bytes = serde_json::to_vec_pretty(job).context("serialize stored job")?;
    db.store_job(&job.id, &bytes)
}

pub fn can_retry_kind(kind: &JobKind) -> bool {
    matches!(
        kind,
        JobKind::MirrorPush | JobKind::MirrorPull | JobKind::SyncRun
    )
}

pub fn is_retryable(job: &StoredJob) -> bool {
    can_retry_kind(&job.kind)
        && matches!(job.status, JobStatus::Failed | JobStatus::Cancelled)
        && job.attempts < job.retry_policy.max_attempts
}

pub fn next_retry_at_unix_ms(job: &StoredJob) -> Option<i64> {
    if !is_retryable(job) {
        return None;
    }
    Some(
        job.updated_at_unix_ms
            .saturating_add((job.retry_policy.backoff_seconds as i64).saturating_mul(1000)),
    )
}

pub fn can_retry_now(job: &StoredJob, now_unix_ms: i64) -> bool {
    is_retryable(job)
        && next_retry_at_unix_ms(job)
            .map(|retry_at| now_unix_ms >= retry_at)
            .unwrap_or(false)
}

pub fn retry_wait_remaining_ms(job: &StoredJob, now_unix_ms: i64) -> Option<i64> {
    next_retry_at_unix_ms(job).map(|retry_at| retry_at.saturating_sub(now_unix_ms).max(0))
}

pub fn remaining_attempts(job: &StoredJob) -> u32 {
    job.retry_policy.max_attempts.saturating_sub(job.attempts)
}

#[cfg(test)]
mod tests {
    use tempfile::tempdir;

    use super::*;
    use crate::core::repository::Repository;

    #[test]
    fn queue_and_load_job_roundtrip() {
        let dir = tempdir().expect("tempdir");
        let repo = Repository::init(dir.path()).expect("init repo");
        let db = MetadataDb::open(&repo.metadata_db_path()).expect("open db");

        let queued = queue_job(
            &db,
            QueueJobRequest {
                kind: JobKind::MirrorPush,
                description: "mirror push".to_string(),
                remote: Some("origin".to_string()),
                commit_id: Some("abc123".to_string()),
                retry_policy: RetryPolicy::default(),
                checkpoint: None,
            },
        )
        .expect("queue job");

        let loaded = load_job(&db, &queued.id)
            .expect("load job")
            .expect("job present");
        assert_eq!(loaded.description, "mirror push");
        assert_eq!(loaded.status, JobStatus::Queued);
    }

    #[test]
    fn running_status_increments_attempts() {
        let dir = tempdir().expect("tempdir");
        let repo = Repository::init(dir.path()).expect("init repo");
        let db = MetadataDb::open(&repo.metadata_db_path()).expect("open db");

        let queued = queue_job(
            &db,
            QueueJobRequest {
                kind: JobKind::MirrorPull,
                description: "mirror pull".to_string(),
                remote: Some("origin".to_string()),
                commit_id: None,
                retry_policy: RetryPolicy::default(),
                checkpoint: None,
            },
        )
        .expect("queue job");

        let running = update_job_status(
            &db,
            &queued.id,
            JobStatus::Running,
            Some(JobCheckpoint {
                stage: "downloading".to_string(),
                cursor: None,
                completed_actions: Vec::new(),
                metrics: None,
            }),
            None,
        )
        .expect("update job");

        assert_eq!(running.attempts, 1);
        assert_eq!(running.status, JobStatus::Running);
        assert_eq!(
            running
                .checkpoint
                .as_ref()
                .map(|checkpoint| checkpoint.stage.as_str()),
            Some("downloading")
        );
    }

    #[test]
    fn repeated_running_updates_do_not_increment_attempts_twice() {
        let dir = tempdir().expect("tempdir");
        let repo = Repository::init(dir.path()).expect("init repo");
        let db = MetadataDb::open(&repo.metadata_db_path()).expect("open db");

        let queued = queue_job(
            &db,
            QueueJobRequest {
                kind: JobKind::SyncRun,
                description: "sync run".to_string(),
                remote: Some("origin".to_string()),
                commit_id: None,
                retry_policy: RetryPolicy::default(),
                checkpoint: None,
            },
        )
        .expect("queue job");

        let running = update_job_status(
            &db,
            &queued.id,
            JobStatus::Running,
            Some(JobCheckpoint {
                stage: "preparing".to_string(),
                cursor: None,
                completed_actions: Vec::new(),
                metrics: None,
            }),
            None,
        )
        .expect("first running update");
        let still_running = update_job_status(
            &db,
            &queued.id,
            JobStatus::Running,
            Some(JobCheckpoint {
                stage: "streaming".to_string(),
                cursor: None,
                completed_actions: Vec::new(),
                metrics: None,
            }),
            None,
        )
        .expect("second running update");

        assert_eq!(running.attempts, 1);
        assert_eq!(still_running.attempts, 1);
    }

    #[test]
    fn retry_backoff_uses_updated_timestamp() {
        let job = StoredJob {
            id: "job-1".to_string(),
            kind: JobKind::SyncRun,
            status: JobStatus::Failed,
            description: "sync".to_string(),
            remote: Some("origin".to_string()),
            commit_id: None,
            created_at_unix_ms: 1_000,
            updated_at_unix_ms: 2_000,
            attempts: 1,
            retry_policy: RetryPolicy {
                max_attempts: 3,
                backoff_seconds: 30,
            },
            checkpoint: None,
            last_error: Some("boom".to_string()),
        };

        assert_eq!(next_retry_at_unix_ms(&job), Some(32_000));
        assert!(!can_retry_now(&job, 31_999));
        assert!(can_retry_now(&job, 32_000));
        assert_eq!(retry_wait_remaining_ms(&job, 31_000), Some(1_000));
        assert_eq!(retry_wait_remaining_ms(&job, 32_500), Some(0));
    }
}
