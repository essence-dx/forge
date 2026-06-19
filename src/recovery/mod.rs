use anyhow::{anyhow, bail, Context, Result};
use chrono::Utc;

use crate::cli;
use crate::core::repository::Repository;
use crate::db::metadata::MetadataDb;
use crate::jobs::{
    can_retry_now, is_retryable, load_job, next_retry_at_unix_ms, remaining_attempts,
    retry_wait_remaining_ms, JobKind, JobStatus,
};
use crate::sync::execute_sync_with_job;

#[derive(Debug, Clone)]
pub struct JobRetryOutcome {
    pub job_id: String,
    pub kind: JobKind,
    pub previous_status: JobStatus,
    pub new_attempts: u32,
    pub remaining_attempts: u32,
    pub retried_at_unix_ms: i64,
    pub summary: String,
}

pub fn retry_job(repo: &Repository, job_id: &str) -> Result<JobRetryOutcome> {
    retry_job_at(repo, job_id, Utc::now().timestamp_millis())
}

pub fn retry_job_at(repo: &Repository, job_id: &str, now_unix_ms: i64) -> Result<JobRetryOutcome> {
    let job = {
        let db = MetadataDb::open(&repo.metadata_db_path())?;
        load_job(&db, job_id)?.ok_or_else(|| anyhow!("job '{}' not found", job_id))?
    };
    if !is_retryable(&job) {
        bail!(
            "job '{}' is not retryable (status: {:?}, attempts: {}, max: {})",
            job.id,
            job.status,
            job.attempts,
            job.retry_policy.max_attempts
        );
    }
    if !can_retry_now(&job, now_unix_ms) {
        let retry_at = next_retry_at_unix_ms(&job).unwrap_or(job.updated_at_unix_ms);
        let remaining_ms = retry_wait_remaining_ms(&job, now_unix_ms).unwrap_or(0);
        bail!(
            "job '{}' is waiting for retry backoff until {} ({} ms remaining)",
            job.id,
            retry_at,
            remaining_ms
        );
    }

    let previous_status = job.status.clone();
    let job_kind = job.kind.clone();
    let summary = match &job_kind {
        JobKind::MirrorPush => retry_push(repo, &job.id, job.remote.as_deref(), &job)?,
        JobKind::MirrorPull => retry_pull(repo, &job.id, job.remote.as_deref())?,
        JobKind::SyncRun => retry_sync(repo, &job.id, job.remote.as_deref(), &job)?,
        JobKind::SyncPlan => bail!(
            "job kind {:?} does not have a retry execution path yet",
            job_kind
        ),
        JobKind::Verify | JobKind::Cleanup => bail!(
            "job kind {:?} does not have an executable retry path yet",
            job.kind
        ),
    };

    let refreshed = {
        let db = MetadataDb::open(&repo.metadata_db_path())?;
        load_job(&db, job_id)?.ok_or_else(|| anyhow!("job '{}' disappeared after retry", job_id))?
    };
    let remaining_attempts = remaining_attempts(&refreshed);
    Ok(JobRetryOutcome {
        job_id: refreshed.id,
        kind: refreshed.kind.clone(),
        previous_status,
        new_attempts: refreshed.attempts,
        remaining_attempts,
        retried_at_unix_ms: now_unix_ms,
        summary,
    })
}

fn retry_push(
    repo: &Repository,
    job_id: &str,
    remote: Option<&str>,
    job: &crate::jobs::StoredJob,
) -> Result<String> {
    let remote = remote.unwrap_or("origin");
    let metrics = job
        .checkpoint
        .as_ref()
        .and_then(|checkpoint| checkpoint.metrics.as_ref());
    let mirror_mode = metrics
        .and_then(|metrics| metrics.get("mirror_mode"))
        .and_then(|value| value.as_str())
        .map(str::to_string)
        .or_else(|| parse_push_mode_from_description(&job.description))
        .unwrap_or_else(|| "all-free".to_string());
    let pro = metrics
        .and_then(|metrics| metrics.get("pro"))
        .and_then(|value| value.as_bool())
        .unwrap_or_else(|| mirror_mode == "pro");

    cli::push::run_with_job(repo, remote, Some(&mirror_mode), pro, job_id)
        .with_context(|| format!("retry push job '{}'", job_id))?;
    Ok(format!(
        "retried push job '{}' to '{}' via {}",
        job_id, remote, mirror_mode
    ))
}

fn retry_pull(repo: &Repository, job_id: &str, remote: Option<&str>) -> Result<String> {
    let remote = remote.unwrap_or("origin");
    cli::pull::run_with_job(repo, remote, job_id)
        .with_context(|| format!("retry pull job '{}'", job_id))?;
    Ok(format!("retried pull job '{}' from '{}'", job_id, remote))
}

fn retry_sync(
    repo: &Repository,
    job_id: &str,
    remote: Option<&str>,
    job: &crate::jobs::StoredJob,
) -> Result<String> {
    let metrics = job
        .checkpoint
        .as_ref()
        .and_then(|checkpoint| checkpoint.metrics.as_ref());
    let force = metrics
        .and_then(|metrics| metrics.get("force"))
        .and_then(|value| value.as_bool())
        .unwrap_or(false);
    let allow_dirty = metrics
        .and_then(|metrics| metrics.get("allow_dirty"))
        .and_then(|value| value.as_bool())
        .unwrap_or(false);
    let requested_remote = metrics
        .and_then(|metrics| metrics.get("requested_remote"))
        .and_then(|value| value.as_str())
        .or(remote);

    execute_sync_with_job(repo, requested_remote, force, allow_dirty, Some(job_id))
        .with_context(|| format!("retry sync job '{}'", job_id))?;
    Ok(format!(
        "retried sync job '{}'{}",
        job_id,
        requested_remote
            .map(|remote| format!(" for '{}'", remote))
            .unwrap_or_default()
    ))
}

fn parse_push_mode_from_description(description: &str) -> Option<String> {
    description
        .split(" via ")
        .nth(1)
        .map(str::trim)
        .filter(|mode| !mode.is_empty())
        .map(str::to_string)
}
