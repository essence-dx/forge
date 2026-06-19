use std::path::Path;

use anyhow::{anyhow, Result};
use chrono::Utc;

use crate::core::repository::Repository;
use crate::db::metadata::MetadataDb;
use crate::jobs::{
    can_retry_now, is_retryable, list_jobs, load_job, next_retry_at_unix_ms, remaining_attempts,
    retry_wait_remaining_ms,
};
use crate::recovery::retry_job;

pub fn run_list() -> Result<()> {
    let repo = Repository::discover(Path::new("."))?;
    let db = MetadataDb::open(&repo.metadata_db_path())?;
    let jobs = list_jobs(&db)?;
    let now_unix_ms = Utc::now().timestamp_millis();

    if jobs.is_empty() {
        println!("No jobs recorded.");
        return Ok(());
    }

    println!("Recorded jobs:");
    for job in jobs {
        let retryable = is_retryable(&job);
        let retry_now = can_retry_now(&job, now_unix_ms);
        let next_retry = next_retry_at_unix_ms(&job)
            .map(|timestamp| timestamp.to_string())
            .unwrap_or_else(|| "-".to_string());
        let wait_ms = retry_wait_remaining_ms(&job, now_unix_ms)
            .map(|value| value.to_string())
            .unwrap_or_else(|| "-".to_string());
        println!(
            "  {} {:?} {:?} remote={} attempts={}/{} retryable={} retry_now={} next_retry={} wait_ms={} remaining={} updated={}",
            job.id,
            job.kind,
            job.status,
            job.remote.as_deref().unwrap_or("-"),
            job.attempts,
            job.retry_policy.max_attempts,
            retryable,
            retry_now,
            next_retry,
            wait_ms,
            remaining_attempts(&job),
            job.updated_at_unix_ms
        );
    }

    Ok(())
}

pub fn run_show(job_id: &str) -> Result<()> {
    let repo = Repository::discover(Path::new("."))?;
    let db = MetadataDb::open(&repo.metadata_db_path())?;
    let job = load_job(&db, job_id)?.ok_or_else(|| anyhow!("job '{}' not found", job_id))?;
    let now_unix_ms = Utc::now().timestamp_millis();
    println!(
        "{}",
        serde_json::to_string_pretty(&job).expect("serialize job for display")
    );
    println!(
        "retryable={} retry_now={} next_retry={} wait_ms={} remaining={}",
        is_retryable(&job),
        can_retry_now(&job, now_unix_ms),
        next_retry_at_unix_ms(&job)
            .map(|timestamp| timestamp.to_string())
            .unwrap_or_else(|| "-".to_string()),
        retry_wait_remaining_ms(&job, now_unix_ms)
            .map(|value| value.to_string())
            .unwrap_or_else(|| "-".to_string()),
        remaining_attempts(&job)
    );
    Ok(())
}

pub fn run_retry(job_id: &str) -> Result<()> {
    let repo = Repository::discover(Path::new("."))?;
    let outcome = retry_job(&repo, job_id)?;
    println!(
        "Retried job {} {:?} from {:?}; attempts={} remaining={} retried_at={}. {}",
        outcome.job_id,
        outcome.kind,
        outcome.previous_status,
        outcome.new_attempts,
        outcome.remaining_attempts,
        outcome.retried_at_unix_ms,
        outcome.summary
    );
    Ok(())
}
