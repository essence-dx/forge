use std::path::Path;

use anyhow::Result;

use crate::core::repository::Repository;
use crate::db::metadata::MetadataDb;
use crate::mirror::auth::AuthStore;
use crate::sync::{
    build_remote_health_report, build_sync_overview, execute_sync, plan_sync, SyncExecutionReport,
    SyncPlan,
};

pub fn run_plan(remote: Option<&str>) -> Result<()> {
    let repo = Repository::discover(Path::new("."))?;
    let auth = AuthStore::open(&repo.forge_dir)?;
    let plan = plan_sync(&repo, &auth, remote)?;
    print_plan(&repo, &plan);
    Ok(())
}

pub fn run_execute(remote: Option<&str>, force: bool, allow_dirty: bool) -> Result<()> {
    let repo = Repository::discover(Path::new("."))?;
    let report = execute_sync(&repo, remote, force, allow_dirty)?;
    print_report(&report);
    Ok(())
}

pub fn run_status() -> Result<()> {
    let repo = Repository::discover(Path::new("."))?;
    let auth = AuthStore::open(&repo.forge_dir)?;
    let db = MetadataDb::open(&repo.metadata_db_path())?;
    let overview = build_sync_overview(&repo, &db, &auth)?;

    println!("Sync status:");
    match &overview.primary_remote {
        Some(remote) => println!(
            "  primary remote: {} ({:?}) -> {}",
            remote.name,
            remote.kind,
            remote.locator.as_deref().unwrap_or("-")
        ),
        None => println!("  primary remote: none"),
    }

    if overview.authenticated_backends.is_empty() {
        println!("  authenticated backends: none");
    } else {
        println!(
            "  authenticated backends: {}",
            overview
                .authenticated_backends
                .iter()
                .map(|remote| remote.name.as_str())
                .collect::<Vec<_>>()
                .join(", ")
        );
    }

    if overview.recent_runs.is_empty() {
        println!("  recent mirror runs: none");
    } else {
        println!("  recent mirror runs:");
        for run in overview.recent_runs.iter().take(5) {
            println!(
                "    - {} {} ok={} failed={} files={}",
                run.remote, run.mirror_mode, run.success_count, run.failure_count, run.file_count
            );
        }
    }

    let health = build_remote_health_report(&repo, &db, &auth)?;
    if !health.is_empty() {
        println!("  remote health:");
        for remote in health {
            println!(
                "    - {} {:?} enabled={} auth={} last_job={:?} last_error={} last_mirror_failures={}",
                remote.name,
                remote.kind,
                remote.enabled,
                remote.authenticated,
                remote.last_job_status,
                remote.last_job_error.as_deref().unwrap_or("-"),
                remote.last_mirror_failure_count.unwrap_or(0)
            );
        }
    }

    Ok(())
}

pub(crate) fn print_plan(repo: &Repository, plan: &SyncPlan) {
    println!(
        "Sync plan: {} action(s), {} warning(s), {} conflict(s)",
        plan.actions.len(),
        plan.warnings.len(),
        plan.conflicts.len()
    );
    if let Some(branch) = &plan.current_branch {
        println!("Current branch: {}", branch);
    }
    if let Some(primary) = &plan.primary_remote {
        println!("Primary remote: {}", primary);
    }

    if !plan.actions.is_empty() {
        println!();
        println!("Actions:");
        for action in &plan.actions {
            println!("  {:?}: {}", action.kind, action.summary);
        }
    }

    if !plan.conflicts.is_empty() {
        println!();
        println!("Conflicts:");
        for conflict in &plan.conflicts {
            let remote = conflict.remote.as_deref().unwrap_or("global");
            let blocking = if conflict.blocking {
                "blocking"
            } else {
                "non-blocking"
            };
            println!("  - [{}] {}: {}", blocking, remote, conflict.summary);
        }
    }

    if !plan.warnings.is_empty() {
        println!();
        println!("Warnings:");
        for warning in &plan.warnings {
            println!("  - {}", warning);
        }
    }

    let registry_path = repo.remote_registry_path();
    println!();
    if registry_path.exists() {
        println!(
            "Registry source: {}",
            registry_path
                .canonicalize()
                .unwrap_or(registry_path)
                .display()
        );
    } else {
        println!("Registry source: inferred from config/auth state");
    }
}

fn print_report(report: &SyncExecutionReport) {
    println!(
        "Sync run: {} action result(s), {} warning(s), {} plan conflict(s)",
        report.results.len(),
        report.warnings.len(),
        report.plan.conflicts.len()
    );

    if !report.results.is_empty() {
        println!();
        println!("Results:");
        for result in &report.results {
            println!(
                "  {:?} {:?} {}: {}",
                result.state, result.kind, result.remote, result.summary
            );
        }
    }

    if !report.plan.conflicts.is_empty() {
        println!();
        println!("Plan conflicts:");
        for conflict in &report.plan.conflicts {
            let remote = conflict.remote.as_deref().unwrap_or("global");
            let blocking = if conflict.blocking {
                "blocking"
            } else {
                "non-blocking"
            };
            println!("  - [{}] {}: {}", blocking, remote, conflict.summary);
        }
    }

    if !report.warnings.is_empty() {
        println!();
        println!("Warnings:");
        for warning in &report.warnings {
            println!("  - {}", warning);
        }
    }
}
