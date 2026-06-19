use std::collections::BTreeSet;
use std::fs;
use std::io::Write;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};

use anyhow::{bail, Context, Result};
use chrono::Utc;
use serde::{Deserialize, Serialize};
use tokio::io::{duplex, AsyncWriteExt};

use crate::cli;
use crate::core::hash::hash_file;
use crate::core::manifest::{deserialize_commit, deserialize_file_entry, serialize_file_entry};
use crate::core::repository::Repository;
use crate::db::metadata::MetadataDb;
use crate::jobs::{
    queue_job, update_job_status, JobCheckpoint, JobKind, JobStatus, QueueJobRequest, RetryPolicy,
};
use crate::mirror::auth::AuthStore;
use crate::mirror::StoredMirrorRun;
use crate::store::cas::ChunkStore;
use crate::store::compression;
use crate::transport::quic::{connect_client, open_forge_stream, QuicClientConfig};
use crate::transport::repository::{
    pull_commit_from_transport, push_commit_to_transport, serve_transport_message,
};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum RemoteKind {
    ForgeTransport,
    GitHub,
    GitLab,
    Bitbucket,
    R2,
    GoogleDrive,
    Dropbox,
    Mega,
    YouTube,
    Pinterest,
    SoundCloud,
    Sketchfab,
    Unknown,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum BranchStrategy {
    MirrorOnly,
    TrackMain,
    Unknown,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum RemoteCapability {
    CodeMirror,
    MediaPublish,
    ByteRestore,
    AuthenticatedRestore,
    LargeFileUpload,
    BranchRefs,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum SyncDirection {
    Push,
    Pull,
    Bidirectional,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum SyncActionKind {
    VerifyAccess,
    PushBranch,
    PullBranch,
    MirrorCommit,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum SyncConflictKind {
    RequestedRemoteMissing,
    AuthRequired,
    BranchMismatch,
    DirtyWorktree,
    MissingRemoteRef,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SyncConflict {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub remote: Option<String>,
    pub kind: SyncConflictKind,
    pub summary: String,
    pub blocking: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum SyncActionState {
    Executed,
    Skipped,
    Blocked,
    Failed,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SyncActionResult {
    pub remote: String,
    pub kind: SyncActionKind,
    pub state: SyncActionState,
    pub summary: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConfiguredRemote {
    pub name: String,
    pub kind: RemoteKind,
    pub locator: Option<String>,
    pub authenticated: bool,
    pub inferred_from_config: bool,
    pub branch_strategy: BranchStrategy,
    pub capabilities: Vec<RemoteCapability>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BranchMapping {
    pub local: String,
    pub remote: String,
    pub direction: SyncDirection,
    pub enabled: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RemoteDefinition {
    pub name: String,
    pub kind: RemoteKind,
    pub locator: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub auth_backend: Option<String>,
    pub enabled: bool,
    pub priority: u16,
    pub branch_strategy: BranchStrategy,
    #[serde(default)]
    pub branch_mappings: Vec<BranchMapping>,
    #[serde(default)]
    pub capabilities: Vec<RemoteCapability>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub notes: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RemoteRegistry {
    pub version: u32,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub primary: Option<String>,
    #[serde(default)]
    pub remotes: Vec<RemoteDefinition>,
}

impl Default for RemoteRegistry {
    fn default() -> Self {
        Self {
            version: 1,
            primary: None,
            remotes: Vec::new(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MirrorRunSummary {
    pub commit_id: String,
    pub remote: String,
    pub mirror_mode: String,
    pub created_at_unix_ms: i64,
    pub success_count: usize,
    pub failure_count: usize,
    pub file_count: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SyncOverview {
    pub primary_remote: Option<ConfiguredRemote>,
    pub authenticated_backends: Vec<ConfiguredRemote>,
    pub recent_runs: Vec<MirrorRunSummary>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RemoteHealth {
    pub name: String,
    pub kind: RemoteKind,
    pub locator: String,
    pub enabled: bool,
    pub authenticated: bool,
    pub capabilities: Vec<RemoteCapability>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_job_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_job_status: Option<JobStatus>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_job_error: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_job_updated_at_unix_ms: Option<i64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_mirror_commit_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_mirror_at_unix_ms: Option<i64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_mirror_success_count: Option<usize>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_mirror_failure_count: Option<usize>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SyncAction {
    pub remote: String,
    pub kind: SyncActionKind,
    pub summary: String,
    pub requires_auth: bool,
    pub destructive: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub branch_mapping: Option<BranchMapping>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SyncPlan {
    pub generated_at_unix_ms: i64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub current_branch: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub primary_remote: Option<String>,
    pub actions: Vec<SyncAction>,
    pub warnings: Vec<String>,
    #[serde(default)]
    pub conflicts: Vec<SyncConflict>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SyncExecutionReport {
    pub started_at_unix_ms: i64,
    pub finished_at_unix_ms: i64,
    pub plan: SyncPlan,
    pub results: Vec<SyncActionResult>,
    pub warnings: Vec<String>,
}

pub fn build_sync_overview(
    repo: &Repository,
    db: &MetadataDb,
    auth: &AuthStore,
) -> Result<SyncOverview> {
    Ok(SyncOverview {
        primary_remote: infer_primary_remote(repo, auth)?,
        authenticated_backends: discover_authenticated_backends(auth)?,
        recent_runs: load_recent_mirror_runs(db)?,
    })
}

pub fn infer_primary_remote(
    repo: &Repository,
    auth: &AuthStore,
) -> Result<Option<ConfiguredRemote>> {
    let config = repo.read_config()?;
    let Some(remote_url) = config.remote_url.as_deref() else {
        return Ok(None);
    };

    let kind = infer_remote_kind_from_url(remote_url);
    let backend_name = backend_name_for_kind(&kind);
    let authenticated = match backend_name {
        Some(name) => auth.load(name)?.is_some(),
        None => false,
    };

    Ok(Some(ConfiguredRemote {
        name: "origin".to_string(),
        kind: kind.clone(),
        locator: Some(remote_url.to_string()),
        authenticated,
        inferred_from_config: true,
        branch_strategy: branch_strategy_for_kind(&kind),
        capabilities: capabilities_for_kind(&kind),
    }))
}

pub fn discover_authenticated_backends(auth: &AuthStore) -> Result<Vec<ConfiguredRemote>> {
    let mut remotes = auth
        .list_backends()?
        .into_iter()
        .map(|backend| configured_backend(&backend, true))
        .collect::<Vec<_>>();
    remotes.sort_by(|left, right| left.name.cmp(&right.name));
    Ok(remotes)
}

pub fn load_recent_mirror_runs(db: &MetadataDb) -> Result<Vec<MirrorRunSummary>> {
    let mut runs = db
        .list_mirror_runs()?
        .into_iter()
        .map(|(_commit_id, bytes)| {
            let run: StoredMirrorRun =
                serde_json::from_slice(&bytes).context("decode stored mirror run")?;
            Ok(MirrorRunSummary {
                commit_id: run.commit_id,
                remote: run.remote,
                mirror_mode: run.mirror_mode,
                created_at_unix_ms: run.created_at_unix_ms,
                success_count: run.success_count,
                failure_count: run.failure_count,
                file_count: run.files.len(),
            })
        })
        .collect::<Result<Vec<_>>>()?;
    runs.sort_by(|left, right| right.created_at_unix_ms.cmp(&left.created_at_unix_ms));
    Ok(runs)
}

pub fn build_remote_health_report(
    repo: &Repository,
    db: &MetadataDb,
    auth: &AuthStore,
) -> Result<Vec<RemoteHealth>> {
    let registry = load_remote_registry(repo, auth)?;
    let jobs = crate::jobs::list_jobs(db)?;
    let runs = load_recent_mirror_runs(db)?;

    registry
        .remotes
        .into_iter()
        .map(|remote| {
            let auth_backend = remote
                .auth_backend
                .clone()
                .or_else(|| backend_name_for_kind(&remote.kind).map(str::to_string));
            let authenticated = auth_backend
                .as_deref()
                .map(|backend| auth.load(backend).ok().flatten().is_some())
                .unwrap_or(true);
            let last_job = jobs
                .iter()
                .find(|job| job.remote.as_deref() == Some(remote.name.as_str()));
            let last_run = runs.iter().find(|run| run.remote == remote.name);

            Ok(RemoteHealth {
                name: remote.name,
                kind: remote.kind,
                locator: remote.locator,
                enabled: remote.enabled,
                authenticated,
                capabilities: remote.capabilities,
                last_job_id: last_job.map(|job| job.id.clone()),
                last_job_status: last_job.map(|job| job.status.clone()),
                last_job_error: last_job.and_then(|job| job.last_error.clone()),
                last_job_updated_at_unix_ms: last_job.map(|job| job.updated_at_unix_ms),
                last_mirror_commit_id: last_run.map(|run| run.commit_id.clone()),
                last_mirror_at_unix_ms: last_run.map(|run| run.created_at_unix_ms),
                last_mirror_success_count: last_run.map(|run| run.success_count),
                last_mirror_failure_count: last_run.map(|run| run.failure_count),
            })
        })
        .collect()
}

pub fn load_remote_registry(repo: &Repository, auth: &AuthStore) -> Result<RemoteRegistry> {
    let inferred = inferred_remote_registry(repo, auth)?;
    let path = repo.remote_registry_path();
    if !path.exists() {
        return Ok(inferred);
    }

    let raw = fs::read(&path).with_context(|| format!("read {}", path.display()))?;
    let stored: RemoteRegistry = serde_json::from_slice(&raw).context("decode remote registry")?;
    Ok(merge_remote_registries(inferred, stored))
}

pub fn save_remote_registry(repo: &Repository, registry: &RemoteRegistry) -> Result<()> {
    let raw = serde_json::to_vec_pretty(registry).context("serialize remote registry")?;
    fs::write(repo.remote_registry_path(), raw)
        .with_context(|| format!("write {}", repo.remote_registry_path().display()))?;
    Ok(())
}

pub fn upsert_remote(
    repo: &Repository,
    auth: &AuthStore,
    remote: RemoteDefinition,
    make_primary: bool,
) -> Result<RemoteRegistry> {
    let mut registry = load_remote_registry(repo, auth)?;
    registry
        .remotes
        .retain(|existing| existing.name != remote.name);
    registry.remotes.push(remote.clone());
    if make_primary || registry.primary.is_none() {
        registry.primary = Some(remote.name.clone());
    }
    sort_registry(&mut registry);
    save_remote_registry(repo, &registry)?;
    Ok(registry)
}

pub fn remove_remote(repo: &Repository, auth: &AuthStore, name: &str) -> Result<RemoteRegistry> {
    let mut registry = load_remote_registry(repo, auth)?;
    let before = registry.remotes.len();
    registry.remotes.retain(|remote| remote.name != name);
    if registry.remotes.len() == before {
        bail!("remote '{}' not found", name);
    }
    if registry.primary.as_deref() == Some(name) {
        registry.primary = registry.remotes.first().map(|remote| remote.name.clone());
    }
    sort_registry(&mut registry);
    save_remote_registry(repo, &registry)?;
    Ok(registry)
}

pub fn plan_sync(
    repo: &Repository,
    auth: &AuthStore,
    requested_remote: Option<&str>,
) -> Result<SyncPlan> {
    let registry = load_remote_registry(repo, auth)?;
    plan_sync_with_registry(repo, auth, &registry, requested_remote)
}

pub fn plan_sync_with_registry(
    repo: &Repository,
    auth: &AuthStore,
    registry: &RemoteRegistry,
    requested_remote: Option<&str>,
) -> Result<SyncPlan> {
    let current_branch = repo.current_branch_name()?;
    let mut actions = Vec::new();
    let mut warnings = Vec::new();

    let mut selected = registry
        .remotes
        .iter()
        .filter(|remote| remote.enabled)
        .collect::<Vec<_>>();
    if let Some(requested) = requested_remote {
        selected.retain(|remote| remote.name == requested);
        if selected.is_empty() {
            warnings.push(format!(
                "remote '{}' is not configured or not enabled",
                requested
            ));
        }
    } else if let Some(primary) = registry.primary.as_deref() {
        selected.sort_by(|left, right| {
            (right.name == primary)
                .cmp(&(left.name == primary))
                .then_with(|| right.priority.cmp(&left.priority))
                .then_with(|| left.name.cmp(&right.name))
        });
    } else {
        selected.sort_by(|left, right| {
            right
                .priority
                .cmp(&left.priority)
                .then_with(|| left.name.cmp(&right.name))
        });
    }

    for remote in selected {
        let auth_backend = remote
            .auth_backend
            .clone()
            .or_else(|| backend_name_for_kind(&remote.kind).map(str::to_string));
        let requires_auth = auth_backend.is_some();
        let authenticated = auth_backend
            .as_deref()
            .map(|backend| auth.load(backend).ok().flatten().is_some())
            .unwrap_or(true);

        if requires_auth {
            actions.push(SyncAction {
                remote: remote.name.clone(),
                kind: SyncActionKind::VerifyAccess,
                summary: format!("Verify access to {}", remote.locator),
                requires_auth: true,
                destructive: false,
                branch_mapping: None,
            });
            if !authenticated {
                warnings.push(format!(
                    "remote '{}' requires '{}' authentication before live sync",
                    remote.name,
                    auth_backend.as_deref().unwrap_or("unknown")
                ));
            }
        }

        if remote.capabilities.contains(&RemoteCapability::BranchRefs) {
            let mappings = if remote.branch_mappings.is_empty() {
                default_branch_mappings(remote, current_branch.as_deref())
            } else {
                remote.branch_mappings.clone()
            };

            if mappings.is_empty() {
                warnings.push(format!(
                    "remote '{}' has no active branch mappings; only mirror-style actions are available",
                    remote.name
                ));
                actions.push(SyncAction {
                    remote: remote.name.clone(),
                    kind: SyncActionKind::MirrorCommit,
                    summary: format!("Mirror the current commit to {}", remote.name),
                    requires_auth,
                    destructive: false,
                    branch_mapping: None,
                });
                continue;
            }

            let active_mappings = mappings
                .into_iter()
                .filter(|mapping| mapping.enabled)
                .collect::<Vec<_>>();

            for mapping in &active_mappings {
                match mapping.direction {
                    SyncDirection::Push => actions.push(branch_action(
                        remote,
                        mapping,
                        SyncActionKind::PushBranch,
                        requires_auth,
                    )),
                    SyncDirection::Pull => actions.push(branch_action(
                        remote,
                        mapping,
                        SyncActionKind::PullBranch,
                        requires_auth,
                    )),
                    SyncDirection::Bidirectional => {
                        actions.push(branch_action(
                            remote,
                            mapping,
                            SyncActionKind::PushBranch,
                            requires_auth,
                        ));
                        actions.push(branch_action(
                            remote,
                            mapping,
                            SyncActionKind::PullBranch,
                            requires_auth,
                        ));
                    }
                }
            }

            if let Some(branch) = current_branch.as_deref() {
                let has_current_mapping = active_mappings
                    .iter()
                    .any(|mapping| mapping.local == branch);
                if !has_current_mapping && branch != "main" {
                    warnings.push(format!(
                        "current branch '{}' is not explicitly mapped for remote '{}'",
                        branch, remote.name
                    ));
                }
            }
        } else {
            actions.push(SyncAction {
                remote: remote.name.clone(),
                kind: SyncActionKind::MirrorCommit,
                summary: format!("Mirror the current commit to {}", remote.name),
                requires_auth,
                destructive: false,
                branch_mapping: None,
            });
        }
    }

    if actions.is_empty() {
        warnings.push("no sync actions were planned".to_string());
    }

    let conflicts = detect_sync_conflicts(repo, auth, registry, requested_remote, &actions)?;

    Ok(SyncPlan {
        generated_at_unix_ms: Utc::now().timestamp_millis(),
        current_branch,
        primary_remote: registry.primary.clone(),
        actions,
        warnings,
        conflicts,
    })
}

pub fn remote_definition(
    name: &str,
    kind: RemoteKind,
    locator: &str,
    auth_backend: Option<String>,
    branch_mappings: Vec<BranchMapping>,
    make_primary_hint: bool,
) -> RemoteDefinition {
    let capabilities = capabilities_for_kind(&kind);
    let priority = if make_primary_hint {
        1000
    } else {
        remote_priority(&kind)
    };
    let branch_strategy = branch_strategy_for_kind(&kind);
    let branch_mappings =
        if branch_mappings.is_empty() && capabilities.contains(&RemoteCapability::BranchRefs) {
            vec![BranchMapping {
                local: "main".to_string(),
                remote: "main".to_string(),
                direction: SyncDirection::Bidirectional,
                enabled: true,
            }]
        } else {
            branch_mappings
        };

    RemoteDefinition {
        name: name.to_string(),
        kind,
        locator: locator.to_string(),
        auth_backend,
        enabled: true,
        priority,
        branch_strategy,
        branch_mappings,
        capabilities,
        notes: None,
    }
}

pub fn parse_remote_kind(value: &str) -> Result<RemoteKind> {
    match value {
        "forge" | "transport" | "forge-transport" | "quic" => Ok(RemoteKind::ForgeTransport),
        "github" => Ok(RemoteKind::GitHub),
        "gitlab" => Ok(RemoteKind::GitLab),
        "bitbucket" => Ok(RemoteKind::Bitbucket),
        "r2" => Ok(RemoteKind::R2),
        "gdrive" | "google-drive" => Ok(RemoteKind::GoogleDrive),
        "dropbox" => Ok(RemoteKind::Dropbox),
        "mega" => Ok(RemoteKind::Mega),
        "youtube" => Ok(RemoteKind::YouTube),
        "pinterest" => Ok(RemoteKind::Pinterest),
        "soundcloud" => Ok(RemoteKind::SoundCloud),
        "sketchfab" => Ok(RemoteKind::Sketchfab),
        "unknown" => Ok(RemoteKind::Unknown),
        other => bail!("unknown remote kind '{}'", other),
    }
}

pub fn parse_sync_direction(value: &str) -> Result<SyncDirection> {
    match value {
        "push" => Ok(SyncDirection::Push),
        "pull" => Ok(SyncDirection::Pull),
        "both" | "bidirectional" => Ok(SyncDirection::Bidirectional),
        other => bail!("unknown sync direction '{}'", other),
    }
}

pub fn parse_branch_mapping(spec: &str) -> Result<BranchMapping> {
    let parts = spec.split(':').collect::<Vec<_>>();
    match parts.as_slice() {
        [local, remote] => Ok(BranchMapping {
            local: (*local).to_string(),
            remote: (*remote).to_string(),
            direction: SyncDirection::Bidirectional,
            enabled: true,
        }),
        [local, remote, direction] => Ok(BranchMapping {
            local: (*local).to_string(),
            remote: (*remote).to_string(),
            direction: parse_sync_direction(direction)?,
            enabled: true,
        }),
        _ => bail!(
            "invalid branch mapping '{}'; expected local:remote[:direction]",
            spec
        ),
    }
}

pub fn execute_sync(
    repo: &Repository,
    requested_remote: Option<&str>,
    force: bool,
    allow_dirty: bool,
) -> Result<SyncExecutionReport> {
    execute_sync_with_job(repo, requested_remote, force, allow_dirty, None)
}

pub(crate) fn execute_sync_with_job(
    repo: &Repository,
    requested_remote: Option<&str>,
    force: bool,
    allow_dirty: bool,
    existing_job_id: Option<&str>,
) -> Result<SyncExecutionReport> {
    let auth = AuthStore::open(&repo.forge_dir)?;
    let registry = load_remote_registry(repo, &auth)?;
    let plan = plan_sync_with_registry(repo, &auth, &registry, requested_remote)?;
    let started_at_unix_ms = Utc::now().timestamp_millis();
    let job_id = match existing_job_id {
        Some(job_id) => job_id.to_string(),
        None => {
            let db = MetadataDb::open(&repo.metadata_db_path())?;
            queue_job(
                &db,
                QueueJobRequest {
                    kind: JobKind::SyncRun,
                    description: format!(
                        "execute sync plan{}",
                        requested_remote
                            .map(|remote| format!(" for '{}'", remote))
                            .unwrap_or_default()
                    ),
                    remote: requested_remote.map(str::to_string),
                    commit_id: repo.read_head()?.map(hex::encode),
                    retry_policy: RetryPolicy::default(),
                    checkpoint: Some(JobCheckpoint {
                        stage: "queued".to_string(),
                        cursor: None,
                        completed_actions: Vec::new(),
                        metrics: Some(sync_job_metrics(requested_remote, force, allow_dirty, None)),
                    }),
                },
            )?
            .id
        }
    };
    {
        let db = MetadataDb::open(&repo.metadata_db_path())?;
        let _ = update_job_status(
            &db,
            &job_id,
            JobStatus::Running,
            Some(JobCheckpoint {
                stage: "executing".to_string(),
                cursor: None,
                completed_actions: Vec::new(),
                metrics: Some(sync_job_metrics(requested_remote, force, allow_dirty, None)),
            }),
            None,
        );
    }

    let mut results = Vec::new();
    let mut runtime_warnings = plan.warnings.clone();
    let effective_conflicts = plan
        .conflicts
        .iter()
        .filter(|conflict| !is_ignored_conflict(conflict, force, allow_dirty))
        .cloned()
        .collect::<Vec<_>>();

    if plan.actions.is_empty() {
        if !effective_conflicts.is_empty() {
            runtime_warnings.push(format!(
                "sync execution blocked by {} unresolved conflict(s)",
                effective_conflicts.len()
            ));
        }
        let blocked = effective_conflicts.len();
        let final_status = if blocked > 0 {
            JobStatus::Cancelled
        } else {
            JobStatus::Succeeded
        };
        let report = SyncExecutionReport {
            started_at_unix_ms,
            finished_at_unix_ms: Utc::now().timestamp_millis(),
            plan,
            results,
            warnings: runtime_warnings,
        };
        {
            let db = MetadataDb::open(&repo.metadata_db_path())?;
            let _ = update_job_status(
                &db,
                &job_id,
                final_status,
                Some(JobCheckpoint {
                    stage: "completed".to_string(),
                    cursor: None,
                    completed_actions: Vec::new(),
                    metrics: Some(sync_job_metrics(
                        requested_remote,
                        force,
                        allow_dirty,
                        Some(serde_json::json!({
                            "executed": 0,
                            "blocked": blocked,
                            "failed": 0,
                        })),
                    )),
                }),
                None,
            );
        }
        return Ok(report);
    }

    let current_head = repo.read_head()?;
    for action in &plan.actions {
        let action_conflicts = effective_conflicts
            .iter()
            .filter(|conflict| conflict_matches_action(conflict, action))
            .cloned()
            .collect::<Vec<_>>();

        if !action_conflicts.is_empty() {
            let summary = action_conflicts
                .iter()
                .map(|conflict| conflict.summary.as_str())
                .collect::<Vec<_>>()
                .join("; ");
            results.push(SyncActionResult {
                remote: action.remote.clone(),
                kind: action.kind.clone(),
                state: SyncActionState::Blocked,
                summary,
            });
            continue;
        }

        match execute_action(repo, &registry, action, current_head, &mut runtime_warnings) {
            Ok(result) => results.push(result),
            Err(error) => results.push(SyncActionResult {
                remote: action.remote.clone(),
                kind: action.kind.clone(),
                state: SyncActionState::Failed,
                summary: error.to_string(),
            }),
        }
    }

    let failed = results
        .iter()
        .filter(|result| matches!(result.state, SyncActionState::Failed))
        .count();
    let blocked = results
        .iter()
        .filter(|result| matches!(result.state, SyncActionState::Blocked))
        .count();
    let executed = results
        .iter()
        .filter(|result| matches!(result.state, SyncActionState::Executed))
        .count();
    let final_status = if failed > 0 {
        JobStatus::Failed
    } else if blocked > 0 {
        JobStatus::Cancelled
    } else {
        JobStatus::Succeeded
    };
    {
        let db = MetadataDb::open(&repo.metadata_db_path())?;
        let _ = update_job_status(
            &db,
            &job_id,
            final_status,
            Some(JobCheckpoint {
                stage: "completed".to_string(),
                cursor: None,
                completed_actions: results
                    .iter()
                    .filter(|result| matches!(result.state, SyncActionState::Executed))
                    .map(|result| result.summary.clone())
                    .collect(),
                metrics: Some(sync_job_metrics(
                    requested_remote,
                    force,
                    allow_dirty,
                    Some(serde_json::json!({
                        "executed": executed,
                        "blocked": blocked,
                        "failed": failed,
                    })),
                )),
            }),
            (failed > 0).then(|| format!("{} sync action(s) failed", failed)),
        );
    }

    Ok(SyncExecutionReport {
        started_at_unix_ms,
        finished_at_unix_ms: Utc::now().timestamp_millis(),
        plan,
        results,
        warnings: runtime_warnings,
    })
}

fn sync_job_metrics(
    requested_remote: Option<&str>,
    force: bool,
    allow_dirty: bool,
    extra: Option<serde_json::Value>,
) -> serde_json::Value {
    let mut metrics = serde_json::Map::new();
    metrics.insert("force".to_string(), serde_json::Value::Bool(force));
    metrics.insert(
        "allow_dirty".to_string(),
        serde_json::Value::Bool(allow_dirty),
    );
    if let Some(remote) = requested_remote {
        metrics.insert(
            "requested_remote".to_string(),
            serde_json::Value::String(remote.to_string()),
        );
    }
    if let Some(serde_json::Value::Object(extra_map)) = extra {
        metrics.extend(extra_map);
    }
    serde_json::Value::Object(metrics)
}

fn detect_sync_conflicts(
    repo: &Repository,
    auth: &AuthStore,
    registry: &RemoteRegistry,
    requested_remote: Option<&str>,
    actions: &[SyncAction],
) -> Result<Vec<SyncConflict>> {
    let db = MetadataDb::open(&repo.metadata_db_path())?;
    let working_tree_dirty = is_working_tree_dirty(repo, &db)?;
    let current_branch = repo.current_branch_name()?;
    let mut conflicts = Vec::new();

    if let Some(requested) = requested_remote {
        if !registry
            .remotes
            .iter()
            .any(|remote| remote.enabled && remote.name == requested)
        {
            push_unique_conflict(
                &mut conflicts,
                SyncConflict {
                    remote: Some(requested.to_string()),
                    kind: SyncConflictKind::RequestedRemoteMissing,
                    summary: format!("requested remote '{}' is not configured", requested),
                    blocking: true,
                },
            );
        }
    }

    for action in actions {
        if action.requires_auth {
            let remote = registry
                .remotes
                .iter()
                .find(|remote| remote.name == action.remote);
            let auth_backend = remote.and_then(|remote| {
                remote
                    .auth_backend
                    .clone()
                    .or_else(|| backend_name_for_kind(&remote.kind).map(str::to_string))
            });
            if let Some(backend) = auth_backend {
                if auth.load(&backend)?.is_none() {
                    push_unique_conflict(
                        &mut conflicts,
                        SyncConflict {
                            remote: Some(action.remote.clone()),
                            kind: SyncConflictKind::AuthRequired,
                            summary: format!(
                                "remote '{}' requires '{}' authentication before execution",
                                action.remote, backend
                            ),
                            blocking: true,
                        },
                    );
                }
            }
        }

        if let Some(mapping) = action.branch_mapping.as_ref() {
            match action.kind {
                SyncActionKind::PushBranch => {
                    if current_branch.as_deref() != Some(mapping.local.as_str()) {
                        push_unique_conflict(&mut conflicts, SyncConflict {
                            remote: Some(action.remote.clone()),
                            kind: SyncConflictKind::BranchMismatch,
                            summary: format!(
                                "push for remote '{}' expects local branch '{}', but current branch is '{}'",
                                action.remote,
                                mapping.local,
                                current_branch.as_deref().unwrap_or("detached")
                            ),
                            blocking: true,
                        });
                    }
                }
                SyncActionKind::PullBranch => {
                    if working_tree_dirty {
                        push_unique_conflict(
                            &mut conflicts,
                            SyncConflict {
                                remote: Some(action.remote.clone()),
                                kind: SyncConflictKind::DirtyWorktree,
                                summary: format!(
                                    "pull for remote '{}' would overwrite a dirty working tree",
                                    action.remote
                                ),
                                blocking: true,
                            },
                        );
                    }
                    if repo
                        .read_remote_ref(&action.remote, &mapping.remote)?
                        .is_none()
                    {
                        push_unique_conflict(
                            &mut conflicts,
                            SyncConflict {
                                remote: Some(action.remote.clone()),
                                kind: SyncConflictKind::MissingRemoteRef,
                                summary: format!(
                                    "remote '{}' has no tracked branch ref for '{}'",
                                    action.remote, mapping.remote
                                ),
                                blocking: true,
                            },
                        );
                    }
                }
                _ => {}
            }
        }
    }

    Ok(conflicts)
}

fn push_unique_conflict(conflicts: &mut Vec<SyncConflict>, conflict: SyncConflict) {
    let exists = conflicts.iter().any(|existing| {
        existing.remote == conflict.remote
            && existing.kind == conflict.kind
            && existing.summary == conflict.summary
            && existing.blocking == conflict.blocking
    });
    if !exists {
        conflicts.push(conflict);
    }
}

fn is_ignored_conflict(conflict: &SyncConflict, force: bool, allow_dirty: bool) -> bool {
    match conflict.kind {
        SyncConflictKind::DirtyWorktree => allow_dirty || force,
        SyncConflictKind::BranchMismatch => force,
        _ => false,
    }
}

fn conflict_matches_action(conflict: &SyncConflict, action: &SyncAction) -> bool {
    match conflict.kind {
        SyncConflictKind::RequestedRemoteMissing => true,
        SyncConflictKind::AuthRequired => {
            conflict.remote.as_deref() == Some(action.remote.as_str())
        }
        SyncConflictKind::BranchMismatch => {
            conflict.remote.as_deref() == Some(action.remote.as_str())
                && matches!(
                    action.kind,
                    SyncActionKind::PushBranch | SyncActionKind::PullBranch
                )
        }
        SyncConflictKind::DirtyWorktree | SyncConflictKind::MissingRemoteRef => {
            conflict.remote.as_deref() == Some(action.remote.as_str())
                && matches!(action.kind, SyncActionKind::PullBranch)
        }
    }
}

fn execute_action(
    repo: &Repository,
    registry: &RemoteRegistry,
    action: &SyncAction,
    current_head: Option<[u8; 32]>,
    runtime_warnings: &mut Vec<String>,
) -> Result<SyncActionResult> {
    let remote = registry
        .remotes
        .iter()
        .find(|remote| remote.name == action.remote)
        .ok_or_else(|| anyhow::anyhow!("remote '{}' not found in registry", action.remote))?;

    match action.kind {
        SyncActionKind::VerifyAccess => Ok(SyncActionResult {
            remote: action.remote.clone(),
            kind: action.kind.clone(),
            state: SyncActionState::Executed,
            summary: format!("Verified configuration for remote '{}'", action.remote),
        }),
        SyncActionKind::MirrorCommit => {
            if matches!(remote.kind, RemoteKind::ForgeTransport) {
                let commit_id =
                    current_head.ok_or_else(|| anyhow::anyhow!("no HEAD commit to mirror"))?;
                let commit_hex = hex::encode(commit_id);
                execute_transport_push(repo, remote, &commit_hex)?;
            } else {
                let backend = backend_name_for_kind(&remote.kind).ok_or_else(|| {
                    anyhow::anyhow!("remote '{}' has no executable backend", remote.name)
                })?;
                cli::push::run_for_repo(repo, &remote.name, Some(backend), false)?;
            }
            Ok(SyncActionResult {
                remote: action.remote.clone(),
                kind: action.kind.clone(),
                state: SyncActionState::Executed,
                summary: format!("Mirrored current commit to '{}'", action.remote),
            })
        }
        SyncActionKind::PushBranch => {
            let mapping = action
                .branch_mapping
                .as_ref()
                .ok_or_else(|| anyhow::anyhow!("push action missing branch mapping"))?;
            let commit_id =
                current_head.ok_or_else(|| anyhow::anyhow!("no HEAD commit to push"))?;
            let commit_hex = hex::encode(commit_id);
            if matches!(remote.kind, RemoteKind::ForgeTransport) {
                execute_transport_push(repo, remote, &commit_hex)?;
            } else {
                let backend = backend_name_for_kind(&remote.kind).ok_or_else(|| {
                    anyhow::anyhow!("remote '{}' has no code backend", remote.name)
                })?;
                cli::push::run_for_repo(repo, &remote.name, Some(backend), false)?;
            }
            repo.write_remote_ref(&remote.name, &mapping.remote, &commit_id)?;
            Ok(SyncActionResult {
                remote: action.remote.clone(),
                kind: action.kind.clone(),
                state: SyncActionState::Executed,
                summary: format!(
                    "Pushed branch '{}' to '{}:{}'",
                    mapping.local, action.remote, mapping.remote
                ),
            })
        }
        SyncActionKind::PullBranch => {
            let mapping = action
                .branch_mapping
                .as_ref()
                .ok_or_else(|| anyhow::anyhow!("pull action missing branch mapping"))?;
            let remote_commit = repo
                .read_remote_ref(&action.remote, &mapping.remote)?
                .ok_or_else(|| {
                    anyhow::anyhow!(
                        "remote ref '{}:{}' not found",
                        action.remote,
                        mapping.remote
                    )
                })?;
            let remote_commit_hex = hex::encode(remote_commit);
            if matches!(remote.kind, RemoteKind::ForgeTransport) {
                execute_transport_pull(repo, remote, &remote_commit_hex)?;
            } else {
                cli::pull::run_for_repo(repo, &remote.name)?;
            }
            if repo
                .forge_dir
                .join("manifests")
                .join(&remote_commit_hex)
                .exists()
            {
                let db = MetadataDb::open(&repo.metadata_db_path())?;
                restore_commit_into_branch(repo, &db, remote_commit, &mapping.local)?;
            } else {
                runtime_warnings.push(format!(
                    "remote '{}' restored files, but commit '{}' is not available locally for branch attach",
                    action.remote, remote_commit_hex
                ));
            }

            Ok(SyncActionResult {
                remote: action.remote.clone(),
                kind: action.kind.clone(),
                state: SyncActionState::Executed,
                summary: format!(
                    "Pulled branch '{}:{}' into local branch '{}'",
                    action.remote, mapping.remote, mapping.local
                ),
            })
        }
    }
}

#[derive(Debug, Clone)]
enum TransportLocator {
    LocalRepo {
        path: PathBuf,
    },
    Quic {
        remote_addr: SocketAddr,
        bind_addr: String,
        ca_certificate_path: PathBuf,
        server_name: String,
        keep_alive: bool,
    },
}

fn execute_transport_push(
    repo: &Repository,
    remote: &RemoteDefinition,
    commit_id: &str,
) -> Result<()> {
    let locator = parse_transport_locator(&remote.locator)?;
    let runtime = tokio::runtime::Runtime::new().context("create transport runtime")?;

    let report = match locator {
        TransportLocator::LocalRepo { path } => {
            let remote_repo = Repository::discover(Path::new(&path))
                .with_context(|| format!("discover transport repo {}", path.display()))?;
            runtime.block_on(async move {
                let (mut client_io, mut server_io) = duplex(1 << 20);
                let server_repo = remote_repo.clone();
                let server_task = tokio::spawn(async move {
                    while serve_transport_message(&server_repo, &mut server_io)
                        .await?
                        .is_some()
                    {}
                    Ok::<_, anyhow::Error>(())
                });

                let report = push_commit_to_transport(repo, &mut client_io, commit_id).await?;
                drop(client_io);
                server_task
                    .await
                    .context("join local transport push task")??;
                Ok::<_, anyhow::Error>(report)
            })?
        }
        TransportLocator::Quic {
            remote_addr,
            bind_addr,
            ca_certificate_path,
            server_name,
            keep_alive,
        } => runtime.block_on(async move {
            let mut session = connect_client(&QuicClientConfig {
                bind_addr,
                remote_addr,
                ca_certificate_path,
                server_name,
                keep_alive,
            })
            .await?;
            let mut stream = open_forge_stream(&mut session.connection).await?;
            let report = push_commit_to_transport(repo, &mut stream, commit_id).await?;
            let _ = stream.shutdown().await;
            Ok::<_, anyhow::Error>(report)
        })?,
    };

    if !report.complete {
        bail!(
            "transport push for remote '{}' did not complete commit '{}'",
            remote.name,
            commit_id
        );
    }
    Ok(())
}

fn execute_transport_pull(
    repo: &Repository,
    remote: &RemoteDefinition,
    commit_id: &str,
) -> Result<()> {
    let locator = parse_transport_locator(&remote.locator)?;
    let runtime = tokio::runtime::Runtime::new().context("create transport runtime")?;

    let report = match locator {
        TransportLocator::LocalRepo { path } => {
            let remote_repo = Repository::discover(Path::new(&path))
                .with_context(|| format!("discover transport repo {}", path.display()))?;
            runtime.block_on(async move {
                let (mut client_io, mut server_io) = duplex(1 << 20);
                let server_repo = remote_repo.clone();
                let server_task = tokio::spawn(async move {
                    while serve_transport_message(&server_repo, &mut server_io)
                        .await?
                        .is_some()
                    {}
                    Ok::<_, anyhow::Error>(())
                });

                let report = pull_commit_from_transport(repo, &mut client_io, commit_id).await?;
                drop(client_io);
                server_task
                    .await
                    .context("join local transport pull task")??;
                Ok::<_, anyhow::Error>(report)
            })?
        }
        TransportLocator::Quic {
            remote_addr,
            bind_addr,
            ca_certificate_path,
            server_name,
            keep_alive,
        } => runtime.block_on(async move {
            let mut session = connect_client(&QuicClientConfig {
                bind_addr,
                remote_addr,
                ca_certificate_path,
                server_name,
                keep_alive,
            })
            .await?;
            let mut stream = open_forge_stream(&mut session.connection).await?;
            let report = pull_commit_from_transport(repo, &mut stream, commit_id).await?;
            let _ = stream.shutdown().await;
            Ok::<_, anyhow::Error>(report)
        })?,
    };

    if !report.complete {
        bail!(
            "transport pull for remote '{}' did not complete commit '{}'",
            remote.name,
            commit_id
        );
    }
    Ok(())
}

fn parse_transport_locator(locator: &str) -> Result<TransportLocator> {
    let trimmed = locator.trim();
    if let Some(rest) = trimmed
        .strip_prefix("forge+local://")
        .or_else(|| trimmed.strip_prefix("local://"))
        .or_else(|| trimmed.strip_prefix("repo://"))
    {
        return Ok(TransportLocator::LocalRepo {
            path: PathBuf::from(rest),
        });
    }
    if let Some(rest) = trimmed
        .strip_prefix("forge+quic://")
        .or_else(|| trimmed.strip_prefix("quic://"))
    {
        return parse_quic_locator(rest);
    }
    if looks_like_local_repo_path(trimmed) {
        return Ok(TransportLocator::LocalRepo {
            path: PathBuf::from(trimmed),
        });
    }

    bail!(
        "unsupported transport locator '{}'; expected forge+local://<path> or forge+quic://<addr>?ca=<path>",
        locator
    )
}

fn parse_quic_locator(rest: &str) -> Result<TransportLocator> {
    let (addr_part, query) = rest.split_once('?').unwrap_or((rest, ""));
    let remote_addr: SocketAddr = addr_part
        .parse()
        .with_context(|| format!("parse QUIC remote address '{addr_part}'"))?;

    let mut bind_addr = "0.0.0.0:0".to_string();
    let mut ca_certificate_path = None::<PathBuf>;
    let mut server_name = "localhost".to_string();
    let mut keep_alive = true;

    for pair in query.split('&').filter(|pair| !pair.is_empty()) {
        let (key, value) = pair.split_once('=').unwrap_or((pair, ""));
        match key {
            "bind" => bind_addr = value.to_string(),
            "ca" | "cert" | "ca_cert" => ca_certificate_path = Some(PathBuf::from(value)),
            "server_name" | "name" => server_name = value.to_string(),
            "keep_alive" => keep_alive = !matches!(value, "0" | "false" | "no"),
            _ => {}
        }
    }

    let ca_certificate_path = ca_certificate_path.ok_or_else(|| {
        anyhow::anyhow!(
            "QUIC transport locator '{}' is missing a ca/cert query parameter",
            rest
        )
    })?;

    Ok(TransportLocator::Quic {
        remote_addr,
        bind_addr,
        ca_certificate_path,
        server_name,
        keep_alive,
    })
}

fn looks_like_local_repo_path(value: &str) -> bool {
    value.starts_with('/')
        || value.starts_with('.')
        || value.contains(":\\")
        || value.contains(":/")
}

fn is_working_tree_dirty(repo: &Repository, db: &MetadataDb) -> Result<bool> {
    if !db.get_staged_files()?.is_empty() {
        return Ok(true);
    }

    for (path, bytes) in db.get_all_tracked_files()? {
        let entry = deserialize_file_entry(&bytes)?;
        let abs = repo.root.join(&path);
        let metadata = match fs::metadata(&abs) {
            Ok(metadata) => metadata,
            Err(_) => return Ok(true),
        };
        if metadata.len() != entry.size {
            return Ok(true);
        }
        let modified_ns = metadata
            .modified()
            .ok()
            .and_then(|mtime| mtime.duration_since(std::time::UNIX_EPOCH).ok())
            .map(|duration| duration.as_nanos() as i64)
            .unwrap_or(0);
        if modified_ns != entry.mtime_ns {
            let hash = hash_file(&abs)?;
            if hash.as_bytes() != &entry.file_hash {
                return Ok(true);
            }
        }
    }

    Ok(false)
}

fn restore_commit_into_branch(
    repo: &Repository,
    db: &MetadataDb,
    commit_id: [u8; 32],
    branch: &str,
) -> Result<()> {
    let commit_hex = hex::encode(commit_id);
    let manifest_path = repo.forge_dir.join("manifests").join(&commit_hex);
    let bytes = fs::read(&manifest_path)
        .with_context(|| format!("read manifest {}", manifest_path.display()))?;
    let commit = deserialize_commit(&bytes)?;
    let store = ChunkStore::new(repo.forge_dir.join("objects/chunks"));

    let target_paths: BTreeSet<String> = commit
        .files
        .iter()
        .map(|entry| entry.path.clone())
        .collect();
    for (path, _) in db.get_all_tracked_files()? {
        if target_paths.contains(&path) {
            continue;
        }
        let abs_path = repo.root.join(&path);
        if abs_path.exists() {
            fs::remove_file(&abs_path)
                .with_context(|| format!("remove stale file {}", abs_path.display()))?;
        }
    }

    let mut tracked_entries = Vec::with_capacity(commit.files.len());
    for entry in &commit.files {
        let out_path = repo.root.join(&entry.path);
        if let Some(parent) = out_path.parent() {
            fs::create_dir_all(parent)
                .with_context(|| format!("create parent dirs {}", parent.display()))?;
        }

        let mut file = fs::File::create(&out_path)
            .with_context(|| format!("create output file {}", out_path.display()))?;
        for chunk in &entry.chunks {
            let hash = blake3::Hash::from(chunk.hash);
            let compressed = store.read(&hash)?;
            let raw = compression::decompress(&compressed)?;
            file.write_all(&raw)
                .with_context(|| format!("write data to {}", out_path.display()))?;
        }

        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            fs::set_permissions(&out_path, fs::Permissions::from_mode(entry.mode))
                .with_context(|| format!("set mode on {}", out_path.display()))?;
        }

        tracked_entries.push((entry.path.clone(), serialize_file_entry(entry)?));
    }

    db.replace_tracked_files(&tracked_entries)?;
    db.clear_staging()?;
    repo.write_branch_ref(branch, &commit_id)?;
    repo.attach_head_to_branch(branch)?;
    Ok(())
}

fn configured_backend(name: &str, authenticated: bool) -> ConfiguredRemote {
    let kind = infer_remote_kind_from_backend(name);
    ConfiguredRemote {
        name: name.to_string(),
        kind: kind.clone(),
        locator: None,
        authenticated,
        inferred_from_config: false,
        branch_strategy: branch_strategy_for_kind(&kind),
        capabilities: capabilities_for_kind(&kind),
    }
}

fn inferred_remote_registry(repo: &Repository, auth: &AuthStore) -> Result<RemoteRegistry> {
    let mut registry = RemoteRegistry::default();
    if let Some(primary) = infer_primary_remote(repo, auth)? {
        registry.primary = Some(primary.name.clone());
        registry.remotes.push(RemoteDefinition {
            name: primary.name,
            kind: primary.kind.clone(),
            locator: primary.locator.unwrap_or_default(),
            auth_backend: backend_name_for_kind(&primary.kind).map(str::to_string),
            enabled: true,
            priority: 1000,
            branch_strategy: primary.branch_strategy,
            branch_mappings: vec![BranchMapping {
                local: "main".to_string(),
                remote: "main".to_string(),
                direction: SyncDirection::Bidirectional,
                enabled: true,
            }],
            capabilities: primary.capabilities,
            notes: Some("inferred from config.toml remote_url".to_string()),
        });
    }
    Ok(registry)
}

fn merge_remote_registries(
    mut inferred: RemoteRegistry,
    mut stored: RemoteRegistry,
) -> RemoteRegistry {
    for remote in stored.remotes.drain(..) {
        inferred
            .remotes
            .retain(|existing| existing.name != remote.name);
        inferred.remotes.push(remote);
    }
    if stored.primary.is_some() {
        inferred.primary = stored.primary;
    }
    sort_registry(&mut inferred);
    inferred
}

fn sort_registry(registry: &mut RemoteRegistry) {
    registry.remotes.sort_by(|left, right| {
        right
            .priority
            .cmp(&left.priority)
            .then_with(|| left.name.cmp(&right.name))
    });
}

fn infer_remote_kind_from_backend(name: &str) -> RemoteKind {
    match name {
        "forge" | "transport" => RemoteKind::ForgeTransport,
        "github" => RemoteKind::GitHub,
        "gitlab" => RemoteKind::GitLab,
        "bitbucket" => RemoteKind::Bitbucket,
        "r2" => RemoteKind::R2,
        "gdrive" => RemoteKind::GoogleDrive,
        "dropbox" => RemoteKind::Dropbox,
        "mega" => RemoteKind::Mega,
        "youtube" => RemoteKind::YouTube,
        "pinterest" => RemoteKind::Pinterest,
        "soundcloud" => RemoteKind::SoundCloud,
        "sketchfab" => RemoteKind::Sketchfab,
        _ => RemoteKind::Unknown,
    }
}

fn infer_remote_kind_from_url(url: &str) -> RemoteKind {
    if url.starts_with("forge+local://")
        || url.starts_with("forge+quic://")
        || url.starts_with("local://")
        || url.starts_with("repo://")
        || url.starts_with("quic://")
    {
        RemoteKind::ForgeTransport
    } else if url.contains("github.com") {
        RemoteKind::GitHub
    } else if url.contains("gitlab.com") {
        RemoteKind::GitLab
    } else if url.contains("bitbucket.org") {
        RemoteKind::Bitbucket
    } else {
        RemoteKind::Unknown
    }
}

fn backend_name_for_kind(kind: &RemoteKind) -> Option<&'static str> {
    match kind {
        RemoteKind::ForgeTransport => None,
        RemoteKind::GitHub => Some("github"),
        RemoteKind::GitLab => Some("gitlab"),
        RemoteKind::Bitbucket => Some("bitbucket"),
        RemoteKind::R2 => Some("r2"),
        RemoteKind::GoogleDrive => Some("gdrive"),
        RemoteKind::Dropbox => Some("dropbox"),
        RemoteKind::Mega => Some("mega"),
        RemoteKind::YouTube => Some("youtube"),
        RemoteKind::Pinterest => Some("pinterest"),
        RemoteKind::SoundCloud => Some("soundcloud"),
        RemoteKind::Sketchfab => Some("sketchfab"),
        RemoteKind::Unknown => None,
    }
}

fn branch_strategy_for_kind(kind: &RemoteKind) -> BranchStrategy {
    match kind {
        RemoteKind::ForgeTransport
        | RemoteKind::GitHub
        | RemoteKind::GitLab
        | RemoteKind::Bitbucket => BranchStrategy::TrackMain,
        RemoteKind::Unknown => BranchStrategy::Unknown,
        _ => BranchStrategy::MirrorOnly,
    }
}

fn capabilities_for_kind(kind: &RemoteKind) -> Vec<RemoteCapability> {
    match kind {
        RemoteKind::ForgeTransport => vec![
            RemoteCapability::CodeMirror,
            RemoteCapability::ByteRestore,
            RemoteCapability::BranchRefs,
        ],
        RemoteKind::GitHub | RemoteKind::GitLab | RemoteKind::Bitbucket => vec![
            RemoteCapability::CodeMirror,
            RemoteCapability::ByteRestore,
            RemoteCapability::AuthenticatedRestore,
            RemoteCapability::BranchRefs,
        ],
        RemoteKind::R2 => vec![
            RemoteCapability::ByteRestore,
            RemoteCapability::AuthenticatedRestore,
            RemoteCapability::LargeFileUpload,
        ],
        RemoteKind::GoogleDrive | RemoteKind::Dropbox | RemoteKind::Mega => vec![
            RemoteCapability::MediaPublish,
            RemoteCapability::ByteRestore,
            RemoteCapability::AuthenticatedRestore,
            RemoteCapability::LargeFileUpload,
        ],
        RemoteKind::YouTube
        | RemoteKind::Pinterest
        | RemoteKind::SoundCloud
        | RemoteKind::Sketchfab => vec![RemoteCapability::MediaPublish],
        RemoteKind::Unknown => Vec::new(),
    }
}

fn remote_priority(kind: &RemoteKind) -> u16 {
    match kind {
        RemoteKind::ForgeTransport => 940,
        RemoteKind::GitHub => 900,
        RemoteKind::GitLab => 880,
        RemoteKind::Bitbucket => 860,
        RemoteKind::R2 => 840,
        RemoteKind::GoogleDrive => 800,
        RemoteKind::Dropbox => 780,
        RemoteKind::Mega => 760,
        RemoteKind::Sketchfab => 520,
        RemoteKind::YouTube => 500,
        RemoteKind::SoundCloud => 480,
        RemoteKind::Pinterest => 460,
        RemoteKind::Unknown => 400,
    }
}

fn default_branch_mappings(
    remote: &RemoteDefinition,
    current_branch: Option<&str>,
) -> Vec<BranchMapping> {
    if remote.branch_strategy != BranchStrategy::TrackMain {
        return Vec::new();
    }

    let local_branch = current_branch.unwrap_or("main");
    let direction = if local_branch == "main" {
        SyncDirection::Bidirectional
    } else {
        SyncDirection::Push
    };

    vec![BranchMapping {
        local: local_branch.to_string(),
        remote: "main".to_string(),
        direction,
        enabled: true,
    }]
}

fn branch_action(
    remote: &RemoteDefinition,
    mapping: &BranchMapping,
    kind: SyncActionKind,
    requires_auth: bool,
) -> SyncAction {
    let summary = match kind {
        SyncActionKind::PushBranch => format!(
            "Push local branch '{}' to remote branch '{}' on {}",
            mapping.local, mapping.remote, remote.name
        ),
        SyncActionKind::PullBranch => format!(
            "Pull remote branch '{}' into local branch '{}' from {}",
            mapping.remote, mapping.local, remote.name
        ),
        _ => format!(
            "Sync branch mapping {}:{} on {}",
            mapping.local, mapping.remote, remote.name
        ),
    };

    SyncAction {
        remote: remote.name.clone(),
        kind,
        summary,
        requires_auth,
        destructive: false,
        branch_mapping: Some(mapping.clone()),
    }
}

#[cfg(test)]
mod tests {
    use tempfile::tempdir;

    use super::*;
    use crate::core::repository::Repository;

    #[test]
    fn infers_github_from_url() {
        assert_eq!(
            infer_remote_kind_from_url("https://github.com/acme/project.git"),
            RemoteKind::GitHub
        );
    }

    #[test]
    fn infers_gitlab_from_url() {
        assert_eq!(
            infer_remote_kind_from_url("git@gitlab.com:group/project.git"),
            RemoteKind::GitLab
        );
    }

    #[test]
    fn infers_bitbucket_from_backend() {
        assert_eq!(
            infer_remote_kind_from_backend("bitbucket"),
            RemoteKind::Bitbucket
        );
    }

    #[test]
    fn parses_transport_remote_kind_aliases() {
        assert_eq!(
            parse_remote_kind("forge").expect("parse kind"),
            RemoteKind::ForgeTransport
        );
        assert_eq!(
            infer_remote_kind_from_url("forge+local://F:/flow/remote"),
            RemoteKind::ForgeTransport
        );
    }

    #[test]
    fn parses_transport_quic_locator() {
        match parse_transport_locator(
            "forge+quic://127.0.0.1:4444?ca=certs/ca.pem&server_name=forge.local&bind=0.0.0.0:0&keep_alive=false",
        )
        .expect("parse locator")
        {
            TransportLocator::Quic {
                remote_addr,
                bind_addr,
                ca_certificate_path,
                server_name,
                keep_alive,
            } => {
                assert_eq!(
                    remote_addr,
                    "127.0.0.1:4444"
                        .parse::<std::net::SocketAddr>()
                        .expect("socket addr")
                );
                assert_eq!(bind_addr, "0.0.0.0:0");
                assert_eq!(ca_certificate_path, PathBuf::from("certs/ca.pem"));
                assert_eq!(server_name, "forge.local");
                assert!(!keep_alive);
            }
            other => panic!("unexpected locator: {other:?}"),
        }
    }

    #[test]
    fn parses_branch_mapping_with_direction() {
        let mapping = parse_branch_mapping("main:release:push").expect("parse branch mapping");
        assert_eq!(mapping.local, "main");
        assert_eq!(mapping.remote, "release");
        assert_eq!(mapping.direction, SyncDirection::Push);
    }

    #[test]
    fn upsert_remote_persists_registry() {
        let dir = tempdir().expect("tempdir");
        let repo = Repository::init(dir.path()).expect("init repo");
        let auth = AuthStore::open(&repo.forge_dir).expect("open auth");

        let registry = upsert_remote(
            &repo,
            &auth,
            remote_definition(
                "backup",
                RemoteKind::GitLab,
                "https://gitlab.com/acme/project.git",
                Some("gitlab".to_string()),
                vec![BranchMapping {
                    local: "main".to_string(),
                    remote: "main".to_string(),
                    direction: SyncDirection::Bidirectional,
                    enabled: true,
                }],
                true,
            ),
            true,
        )
        .expect("upsert remote");

        assert_eq!(registry.primary.as_deref(), Some("backup"));
        let loaded = load_remote_registry(&repo, &auth).expect("load registry");
        assert_eq!(loaded.remotes.len(), 1);
        assert_eq!(loaded.remotes[0].name, "backup");
    }

    #[test]
    fn sync_plan_includes_branch_actions() {
        let dir = tempdir().expect("tempdir");
        let repo = Repository::init(dir.path()).expect("init repo");
        let auth = AuthStore::open(&repo.forge_dir).expect("open auth");
        let registry = RemoteRegistry {
            version: 1,
            primary: Some("origin".to_string()),
            remotes: vec![remote_definition(
                "origin",
                RemoteKind::GitHub,
                "https://github.com/acme/project.git",
                Some("github".to_string()),
                vec![BranchMapping {
                    local: "main".to_string(),
                    remote: "main".to_string(),
                    direction: SyncDirection::Bidirectional,
                    enabled: true,
                }],
                true,
            )],
        };

        let plan = plan_sync_with_registry(&repo, &auth, &registry, None).expect("plan sync");
        assert!(plan
            .actions
            .iter()
            .any(|action| matches!(action.kind, SyncActionKind::PushBranch)));
        assert!(plan
            .actions
            .iter()
            .any(|action| matches!(action.kind, SyncActionKind::PullBranch)));
    }
}
