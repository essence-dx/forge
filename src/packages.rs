use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::path::{Component, Path, PathBuf};

use anyhow::{bail, Context, Result};
use chrono::Utc;
use serde::{Deserialize, Serialize};
use walkdir::WalkDir;

use crate::chunking::cdc::ChunkConfig;
use crate::chunking::chunk_file;
use crate::core::hash::hash_file;
use crate::core::manifest::FileType;
use crate::core::repository::Repository;

pub const PACKAGE_CONTRACT_FORMAT: u16 = 1;
pub const PACKAGE_MANIFEST_SCHEMA: &str = "forge.package_manifest";
pub const PACKAGE_LOCK_SCHEMA: &str = "forge.package_lock";
pub const PACKAGE_STATUS_SCHEMA: &str = "forge.package_status_receipt";
pub const PACKAGE_ADD_RECEIPT_SCHEMA: &str = "forge.package_add_receipt";

const LEGACY_PACKAGE_MANIFEST_SCHEMA: &str = "forge.package_manifest.v1";

fn package_contract_format() -> u16 {
    PACKAGE_CONTRACT_FORMAT
}

fn package_manifest_schema_supported(schema: &str) -> bool {
    schema == PACKAGE_MANIFEST_SCHEMA || schema == LEGACY_PACKAGE_MANIFEST_SCHEMA
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PackageManifest {
    pub schema: String,
    #[serde(default = "package_contract_format")]
    pub format: u16,
    #[serde(default)]
    pub packages: Vec<PackageSpec>,
    #[serde(default)]
    pub remotes: Vec<PackageRemote>,
    #[serde(default)]
    pub media: Vec<MediaAssetSpec>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PackageSpec {
    pub name: String,
    pub version: String,
    pub source: PackageSource,
    #[serde(default)]
    pub integrity_hash: Option<String>,
    #[serde(default)]
    pub dependencies: Vec<DependencyConstraint>,
    #[serde(default)]
    pub files: Vec<PackageFileSpec>,
    #[serde(default)]
    pub receipt_paths: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PackageSource {
    pub kind: String,
    pub locator: String,
    #[serde(default)]
    pub hash: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DependencyConstraint {
    pub name: String,
    pub constraint: String,
    #[serde(default)]
    pub boundary: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PackageFileSpec {
    pub path: String,
    #[serde(default, alias = "hash")]
    pub expected_hash: Option<String>,
    #[serde(default)]
    pub role: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PackageRemote {
    pub name: String,
    pub kind: String,
    pub locator: String,
    #[serde(default)]
    pub auth_ref: Option<String>,
    #[serde(default)]
    pub secret_policy: Option<String>,
    #[serde(default)]
    pub notes: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MediaAssetSpec {
    pub asset_id: String,
    pub path: String,
    #[serde(default)]
    pub media_type: Option<String>,
    #[serde(default, alias = "hash")]
    pub expected_hash: Option<String>,
    #[serde(default)]
    pub preview_receipt: Option<String>,
    #[serde(default)]
    pub restore_plan: Option<String>,
    #[serde(default)]
    pub metadata: BTreeMap<String, String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum PackageIntegrityState {
    Valid,
    Missing,
    Mismatch,
    Invalid,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PackageFileStatus {
    pub path: String,
    pub exists: bool,
    pub bytes: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub content_hash: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub expected_hash: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub hash_matches: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub role: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PackageStatus {
    pub name: String,
    pub version: String,
    pub source_kind: String,
    pub source_locator: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub source_hash: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub expected_source_hash: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub source_exists: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub source_hash_matches: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub expected_integrity_hash: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub integrity_hash: Option<String>,
    pub integrity_state: PackageIntegrityState,
    pub dependency_constraints: Vec<DependencyConstraint>,
    pub files: Vec<PackageFileStatus>,
    pub receipt_paths: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RemoteStatus {
    pub name: String,
    pub kind: String,
    pub locator: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub auth_ref: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub secret_policy: Option<String>,
    pub secrets_safe: bool,
    pub executable_now: bool,
    pub boundary: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub notes: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MediaChunkStatus {
    pub index: usize,
    pub offset: u64,
    pub length: u32,
    pub hash: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MediaAssetStatus {
    pub asset_id: String,
    pub path: String,
    pub exists: bool,
    pub bytes: u64,
    pub media_type: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub content_hash: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub expected_hash: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub hash_matches: Option<bool>,
    pub chunk_count: usize,
    pub chunk_map: Vec<MediaChunkStatus>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub preview_receipt: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub restore_plan: Option<String>,
    pub metadata: BTreeMap<String, String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PackageStatusSummary {
    pub package_count: usize,
    pub valid_packages: usize,
    pub missing_packages: usize,
    pub mismatched_packages: usize,
    pub dependency_constraints: usize,
    pub remote_count: usize,
    pub unsafe_remote_count: usize,
    pub media_asset_count: usize,
    pub tracked_media_assets: usize,
    pub media_chunk_count: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PackageStatusReceipt {
    pub schema: String,
    #[serde(default = "package_contract_format")]
    pub format: u16,
    pub generated_at_unix_ms: i64,
    pub manifest_path: PathBuf,
    pub lock_path: PathBuf,
    pub package_lock_present: bool,
    pub packages: Vec<PackageStatus>,
    pub remotes: Vec<RemoteStatus>,
    pub media: Vec<MediaAssetStatus>,
    pub summary: PackageStatusSummary,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PackageLock {
    pub schema: String,
    #[serde(default = "package_contract_format")]
    pub format: u16,
    pub generated_at_unix_ms: i64,
    pub packages: Vec<PackageLockPackage>,
    pub remotes: Vec<RemoteStatus>,
    pub media: Vec<PackageLockMediaAsset>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PackageLockPackage {
    pub name: String,
    pub version: String,
    pub source_kind: String,
    pub source_locator: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub source_hash: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub expected_source_hash: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub source_hash_matches: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub integrity_hash: Option<String>,
    pub integrity_state: PackageIntegrityState,
    pub dependency_constraints: Vec<DependencyConstraint>,
    pub files: Vec<PackageFileStatus>,
    pub receipt_paths: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PackageLockMediaAsset {
    pub asset_id: String,
    pub path: String,
    pub media_type: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub content_hash: Option<String>,
    pub chunk_count: usize,
    pub chunk_map: Vec<MediaChunkStatus>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub preview_receipt: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub restore_plan: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PackageAddReceipt {
    pub schema: String,
    #[serde(default = "package_contract_format")]
    pub format: u16,
    pub generated_at_unix_ms: i64,
    pub package: PackageLockPackage,
    pub manifest_path: String,
    pub lock_path: String,
    pub status_receipt_path: String,
    pub package_receipt_path: String,
    pub cache: PackageCacheReceipt,
    pub boundary: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PackageCacheReceipt {
    pub cache_path: String,
    pub cached_files: Vec<CachedPackageFile>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CachedPackageFile {
    pub path: String,
    pub cache_path: String,
    pub bytes: u64,
    pub content_hash: String,
}

pub fn load_package_manifest(repo: &Repository) -> Result<PackageManifest> {
    let path = repo.package_manifest_path();
    let bytes = fs::read(&path).with_context(|| {
        format!(
            "read package manifest {}; run `forge package lock --write` after creating one",
            path.display()
        )
    })?;
    let manifest: PackageManifest =
        serde_json::from_slice(&bytes).with_context(|| format!("parse {}", path.display()))?;
    if !package_manifest_schema_supported(&manifest.schema) {
        bail!(
            "unsupported package manifest schema `{}`; expected `{}`",
            manifest.schema,
            PACKAGE_MANIFEST_SCHEMA
        );
    }
    if manifest.format != PACKAGE_CONTRACT_FORMAT {
        bail!(
            "unsupported package manifest format `{}`; expected `{}`",
            manifest.format,
            PACKAGE_CONTRACT_FORMAT
        );
    }
    Ok(manifest)
}

pub fn load_or_create_package_manifest(repo: &Repository) -> Result<PackageManifest> {
    if repo.package_manifest_path().exists() {
        return load_package_manifest(repo);
    }

    Ok(PackageManifest {
        schema: PACKAGE_MANIFEST_SCHEMA.to_string(),
        format: PACKAGE_CONTRACT_FORMAT,
        packages: Vec::new(),
        remotes: Vec::new(),
        media: Vec::new(),
    })
}

pub fn add_local_slice_package(
    repo: &Repository,
    name: &str,
    version: &str,
    source: &str,
    files: &[String],
    dependencies: Vec<DependencyConstraint>,
) -> Result<PackageAddReceipt> {
    let source = normalize_path(source);
    let source_path = resolve_repo_relative_path(&repo.root, &source)?;
    if !source_path.exists() {
        bail!("package source `{source}` does not exist");
    }

    let package_files = collect_package_files(repo, &source, files)?;
    if package_files.is_empty() {
        bail!("package `{name}` does not contain any source files");
    }

    let source_hash = hash_package_path(repo, &source_path)?;
    let package_receipt_rel = format!(
        ".forge/receipts/packages/{}.json",
        sanitize_package_name(name)
    );
    let mut spec = PackageSpec {
        name: name.to_string(),
        version: version.to_string(),
        source: PackageSource {
            kind: "local-slice".to_string(),
            locator: source.clone(),
            hash: Some(source_hash),
        },
        integrity_hash: None,
        dependencies,
        files: package_files,
        receipt_paths: vec![
            package_receipt_rel,
            ".forge/receipts/package-status.json".to_string(),
        ],
    };
    validate_package_identity(&spec)?;
    fill_package_file_hashes(repo, &mut spec)?;

    let mut manifest = load_or_create_package_manifest(repo)?;
    manifest.packages.retain(|package| package.name != name);
    manifest.packages.push(spec);
    manifest.packages.sort_by(|left, right| {
        left.name
            .cmp(&right.name)
            .then_with(|| left.version.cmp(&right.version))
    });
    write_package_manifest(repo, &manifest)?;

    let status = build_package_status(repo)?;
    let integrity_hash = status
        .packages
        .iter()
        .find(|package| package.name == name)
        .and_then(|package| package.integrity_hash.clone())
        .context("package integrity hash was not available after add")?;

    let mut manifest = load_package_manifest(repo)?;
    let package = manifest
        .packages
        .iter_mut()
        .find(|package| package.name == name)
        .context("package manifest entry missing after add")?;
    package.integrity_hash = Some(integrity_hash);
    write_package_manifest(repo, &manifest)?;

    let status = build_package_status(repo)?;
    let lock = write_package_lock(repo, &status)?;
    write_package_status_receipt(repo, &status)?;

    let package = lock
        .packages
        .iter()
        .find(|package| package.name == name)
        .cloned()
        .context("package lock entry missing after add")?;
    let cache = write_package_cache(repo, &package)?;
    let receipt_path = repo
        .forge_dir
        .join("receipts/packages")
        .join(format!("{}.json", sanitize_package_name(name)));
    let receipt = PackageAddReceipt {
        schema: PACKAGE_ADD_RECEIPT_SCHEMA.to_string(),
        format: PACKAGE_CONTRACT_FORMAT,
        generated_at_unix_ms: Utc::now().timestamp_millis(),
        package,
        manifest_path: repo_relative_label(repo, &repo.package_manifest_path()),
        lock_path: repo_relative_label(repo, &repo.package_lock_path()),
        status_receipt_path: repo_relative_label(repo, &repo.package_status_receipt_path()),
        package_receipt_path: repo_relative_label(repo, &receipt_path),
        cache,
        boundary: "forge-owned source slice; no node_modules install performed".to_string(),
    };
    write_pretty_json(&receipt_path, &receipt)?;
    Ok(receipt)
}

pub fn build_package_status(repo: &Repository) -> Result<PackageStatusReceipt> {
    let manifest = load_package_manifest(repo)?;
    let config = repo.read_config()?;
    let chunk_config = ChunkConfig {
        min_size: config.chunk_min,
        avg_size: config.chunk_avg,
        max_size: config.chunk_max,
    };

    let packages = manifest
        .packages
        .iter()
        .map(|package| package_status(repo, package))
        .collect::<Result<Vec<_>>>()?;
    let remotes = manifest
        .remotes
        .iter()
        .map(remote_status)
        .collect::<Vec<_>>();
    let media = manifest
        .media
        .iter()
        .map(|asset| media_status(repo, asset, &chunk_config))
        .collect::<Result<Vec<_>>>()?;

    let summary = PackageStatusSummary {
        package_count: packages.len(),
        valid_packages: packages
            .iter()
            .filter(|package| package.integrity_state == PackageIntegrityState::Valid)
            .count(),
        missing_packages: packages
            .iter()
            .filter(|package| package.integrity_state == PackageIntegrityState::Missing)
            .count(),
        mismatched_packages: packages
            .iter()
            .filter(|package| package.integrity_state == PackageIntegrityState::Mismatch)
            .count(),
        dependency_constraints: packages
            .iter()
            .map(|package| package.dependency_constraints.len())
            .sum(),
        remote_count: remotes.len(),
        unsafe_remote_count: remotes.iter().filter(|remote| !remote.secrets_safe).count(),
        media_asset_count: media.len(),
        tracked_media_assets: media.iter().filter(|asset| asset.exists).count(),
        media_chunk_count: media.iter().map(|asset| asset.chunk_count).sum(),
    };

    Ok(PackageStatusReceipt {
        schema: PACKAGE_STATUS_SCHEMA.to_string(),
        format: PACKAGE_CONTRACT_FORMAT,
        generated_at_unix_ms: Utc::now().timestamp_millis(),
        manifest_path: repo.package_manifest_path(),
        lock_path: repo.package_lock_path(),
        package_lock_present: repo.package_lock_path().exists(),
        packages,
        remotes,
        media,
        summary,
    })
}

pub fn build_package_lock(status: &PackageStatusReceipt) -> PackageLock {
    PackageLock {
        schema: PACKAGE_LOCK_SCHEMA.to_string(),
        format: PACKAGE_CONTRACT_FORMAT,
        generated_at_unix_ms: Utc::now().timestamp_millis(),
        packages: status
            .packages
            .iter()
            .map(|package| PackageLockPackage {
                name: package.name.clone(),
                version: package.version.clone(),
                source_kind: package.source_kind.clone(),
                source_locator: package.source_locator.clone(),
                source_hash: package.source_hash.clone(),
                expected_source_hash: package.expected_source_hash.clone(),
                source_hash_matches: package.source_hash_matches,
                integrity_hash: package.integrity_hash.clone(),
                integrity_state: package.integrity_state,
                dependency_constraints: package.dependency_constraints.clone(),
                files: package.files.clone(),
                receipt_paths: package.receipt_paths.clone(),
            })
            .collect(),
        remotes: status.remotes.clone(),
        media: status
            .media
            .iter()
            .map(|asset| PackageLockMediaAsset {
                asset_id: asset.asset_id.clone(),
                path: asset.path.clone(),
                media_type: asset.media_type.clone(),
                content_hash: asset.content_hash.clone(),
                chunk_count: asset.chunk_count,
                chunk_map: asset.chunk_map.clone(),
                preview_receipt: asset.preview_receipt.clone(),
                restore_plan: asset.restore_plan.clone(),
            })
            .collect(),
    }
}

pub fn write_package_lock(repo: &Repository, status: &PackageStatusReceipt) -> Result<PackageLock> {
    let lock = build_package_lock(status);
    let path = repo.package_lock_path();
    write_pretty_json(&path, &lock)?;
    Ok(lock)
}

pub fn write_package_status_receipt(
    repo: &Repository,
    status: &PackageStatusReceipt,
) -> Result<PathBuf> {
    let path = repo.package_status_receipt_path();
    write_pretty_json(&path, status)?;
    Ok(path)
}

fn write_package_manifest(repo: &Repository, manifest: &PackageManifest) -> Result<()> {
    write_pretty_json(&repo.package_manifest_path(), manifest)
}

fn package_status(repo: &Repository, package: &PackageSpec) -> Result<PackageStatus> {
    validate_package_identity(package)?;

    let source = package_source_status(repo, &package.source)?;
    let files = package
        .files
        .iter()
        .map(|file| package_file_status(repo, file))
        .collect::<Result<Vec<_>>>()?;
    let integrity_hash = package_integrity_hash(&files, &package.dependencies);
    let missing = files.iter().any(|file| !file.exists);
    let mismatched_files = files.iter().any(|file| file.hash_matches == Some(false));
    let invalid_expected = package
        .integrity_hash
        .as_deref()
        .is_some_and(|hash| !is_blake3_hex(hash));
    let invalid_source_hash = package
        .source
        .hash
        .as_deref()
        .is_some_and(|hash| !is_blake3_hex(hash));
    let integrity_mismatch = package
        .integrity_hash
        .as_deref()
        .zip(integrity_hash.as_deref())
        .is_some_and(|(expected, actual)| expected != actual);
    let source_hash_mismatch = source.hash_matches == Some(false);

    let integrity_state = if invalid_expected || invalid_source_hash {
        PackageIntegrityState::Invalid
    } else if missing || source.exists == Some(false) {
        PackageIntegrityState::Missing
    } else if mismatched_files || integrity_mismatch || source_hash_mismatch {
        PackageIntegrityState::Mismatch
    } else {
        PackageIntegrityState::Valid
    };

    Ok(PackageStatus {
        name: package.name.clone(),
        version: package.version.clone(),
        source_kind: package.source.kind.clone(),
        source_locator: package.source.locator.clone(),
        source_hash: source.content_hash,
        expected_source_hash: package.source.hash.clone(),
        source_exists: source.exists,
        source_hash_matches: source.hash_matches,
        expected_integrity_hash: package.integrity_hash.clone(),
        integrity_hash,
        integrity_state,
        dependency_constraints: package.dependencies.clone(),
        files,
        receipt_paths: package.receipt_paths.clone(),
    })
}

fn package_file_status(repo: &Repository, file: &PackageFileSpec) -> Result<PackageFileStatus> {
    let path = resolve_repo_relative_path(&repo.root, &file.path)?;
    if !path.exists() {
        return Ok(PackageFileStatus {
            path: normalize_path(&file.path),
            exists: false,
            bytes: 0,
            content_hash: None,
            expected_hash: file.expected_hash.clone(),
            hash_matches: file.expected_hash.as_ref().map(|_| false),
            role: file.role.clone(),
        });
    }

    let metadata = fs::metadata(&path).with_context(|| format!("stat {}", path.display()))?;
    let content_hash = hash_package_path(repo, &path)?;
    let hash_matches = file
        .expected_hash
        .as_ref()
        .map(|expected| expected == &content_hash);

    Ok(PackageFileStatus {
        path: normalize_path(&file.path),
        exists: true,
        bytes: metadata.len(),
        content_hash: Some(content_hash),
        expected_hash: file.expected_hash.clone(),
        hash_matches,
        role: file.role.clone(),
    })
}

fn media_status(
    repo: &Repository,
    asset: &MediaAssetSpec,
    chunk_config: &ChunkConfig,
) -> Result<MediaAssetStatus> {
    let path = resolve_repo_relative_path(&repo.root, &asset.path)?;
    if !path.exists() {
        return Ok(MediaAssetStatus {
            asset_id: asset.asset_id.clone(),
            path: normalize_path(&asset.path),
            exists: false,
            bytes: 0,
            media_type: asset
                .media_type
                .clone()
                .unwrap_or_else(|| "unknown".to_string()),
            content_hash: None,
            expected_hash: asset.expected_hash.clone(),
            hash_matches: asset.expected_hash.as_ref().map(|_| false),
            chunk_count: 0,
            chunk_map: Vec::new(),
            preview_receipt: asset.preview_receipt.clone(),
            restore_plan: asset.restore_plan.clone(),
            metadata: asset.metadata.clone(),
        });
    }

    let bytes = fs::read(&path).with_context(|| format!("read media {}", path.display()))?;
    let metadata = fs::metadata(&path).with_context(|| format!("stat media {}", path.display()))?;
    let header_len = bytes.len().min(128);
    let file_type = FileType::detect(&path, &bytes[..header_len]);
    let content_hash = blake3::hash(&bytes).to_hex().to_string();
    let hash_matches = asset
        .expected_hash
        .as_ref()
        .map(|expected| expected == &content_hash);
    let chunk_map = chunk_file(&bytes, file_type, chunk_config)
        .into_iter()
        .enumerate()
        .map(|(index, chunk)| MediaChunkStatus {
            index,
            offset: chunk.offset as u64,
            length: chunk.length as u32,
            hash: chunk.hash.to_hex().to_string(),
        })
        .collect::<Vec<_>>();

    Ok(MediaAssetStatus {
        asset_id: asset.asset_id.clone(),
        path: normalize_path(&asset.path),
        exists: true,
        bytes: metadata.len(),
        media_type: asset
            .media_type
            .clone()
            .unwrap_or_else(|| format!("{file_type:?}").to_ascii_lowercase()),
        content_hash: Some(content_hash),
        expected_hash: asset.expected_hash.clone(),
        hash_matches,
        chunk_count: chunk_map.len(),
        chunk_map,
        preview_receipt: asset.preview_receipt.clone(),
        restore_plan: asset.restore_plan.clone(),
        metadata: asset.metadata.clone(),
    })
}

fn package_integrity_hash(
    files: &[PackageFileStatus],
    dependencies: &[DependencyConstraint],
) -> Option<String> {
    if files
        .iter()
        .any(|file| !file.exists || file.content_hash.is_none())
    {
        return None;
    }

    let mut hasher = blake3::Hasher::new();
    let mut sorted_files = files.iter().collect::<Vec<_>>();
    sorted_files.sort_by(|left, right| left.path.cmp(&right.path));
    for file in sorted_files {
        hasher.update(file.path.as_bytes());
        let hash = file.content_hash.as_ref()?;
        hasher.update(hash.as_bytes());
    }
    let mut sorted_dependencies = dependencies.iter().collect::<Vec<_>>();
    sorted_dependencies.sort_by(|left, right| {
        left.name
            .cmp(&right.name)
            .then_with(|| left.constraint.cmp(&right.constraint))
            .then_with(|| left.boundary.cmp(&right.boundary))
    });
    for dependency in sorted_dependencies {
        hasher.update(dependency.name.as_bytes());
        hasher.update(dependency.constraint.as_bytes());
        if let Some(boundary) = &dependency.boundary {
            hasher.update(boundary.as_bytes());
        }
    }
    Some(hasher.finalize().to_hex().to_string())
}

fn remote_status(remote: &PackageRemote) -> RemoteStatus {
    let secrets_safe = !has_plaintext_secret_marker(&remote.locator)
        && remote
            .auth_ref
            .as_deref()
            .is_none_or(|auth_ref| !has_plaintext_secret_marker(auth_ref))
        && remote
            .secret_policy
            .as_deref()
            .is_none_or(|policy| !has_plaintext_secret_marker(policy));
    let executable_now = matches!(
        remote.kind.as_str(),
        "local" | "local-filesystem" | "filesystem" | "forge-transport"
    );
    let boundary = if executable_now {
        "local-provider-ready".to_string()
    } else {
        "adapter-boundary-configured-not-executed".to_string()
    };

    RemoteStatus {
        name: remote.name.clone(),
        kind: remote.kind.clone(),
        locator: remote.locator.clone(),
        auth_ref: remote.auth_ref.clone(),
        secret_policy: remote.secret_policy.clone(),
        secrets_safe,
        executable_now,
        boundary,
        notes: remote.notes.clone(),
    }
}

fn validate_package_identity(package: &PackageSpec) -> Result<()> {
    if package.name.trim().is_empty() {
        bail!("package name must not be empty");
    }
    if package.version.trim().is_empty() {
        bail!("package `{}` must declare a version", package.name);
    }
    if package.source.kind.trim().is_empty() {
        bail!("package `{}` must declare a source kind", package.name);
    }
    if package.source.locator.trim().is_empty() {
        bail!("package `{}` must declare a source locator", package.name);
    }
    Ok(())
}

#[derive(Debug)]
struct PackageSourceStatus {
    exists: Option<bool>,
    content_hash: Option<String>,
    hash_matches: Option<bool>,
}

fn package_source_status(repo: &Repository, source: &PackageSource) -> Result<PackageSourceStatus> {
    if !is_local_package_source(&source.kind, &source.locator) {
        return Ok(PackageSourceStatus {
            exists: None,
            content_hash: None,
            hash_matches: None,
        });
    }

    let locator = source
        .locator
        .strip_prefix("file://")
        .unwrap_or(&source.locator);
    let path = resolve_repo_relative_path(&repo.root, locator)?;
    if !path.exists() {
        return Ok(PackageSourceStatus {
            exists: Some(false),
            content_hash: None,
            hash_matches: source.hash.as_ref().map(|_| false),
        });
    }

    let content_hash = hash_package_path(repo, &path)?;
    let hash_matches = source
        .hash
        .as_ref()
        .map(|expected| expected == &content_hash);
    Ok(PackageSourceStatus {
        exists: Some(true),
        content_hash: Some(content_hash),
        hash_matches,
    })
}

fn is_local_package_source(kind: &str, locator: &str) -> bool {
    let kind = kind.to_ascii_lowercase();
    kind.contains("local")
        || kind.contains("filesystem")
        || kind.contains("slice")
        || kind.contains("path")
        || locator.starts_with("file://")
}

fn fill_package_file_hashes(repo: &Repository, package: &mut PackageSpec) -> Result<()> {
    for file in &mut package.files {
        let path = resolve_repo_relative_path(&repo.root, &file.path)?;
        if !path.is_file() {
            bail!(
                "package file `{}` does not exist or is not a file",
                file.path
            );
        }
        file.path = normalize_path(&file.path);
        file.expected_hash = Some(hash_file(&path)?.to_hex().to_string());
        if file.role.is_none() {
            file.role = Some("source".to_string());
        }
    }
    Ok(())
}

fn collect_package_files(
    repo: &Repository,
    source: &str,
    explicit_files: &[String],
) -> Result<Vec<PackageFileSpec>> {
    let mut files = BTreeSet::new();
    if explicit_files.is_empty() {
        let source_path = resolve_repo_relative_path(&repo.root, source)?;
        if source_path.is_file() {
            files.insert(normalize_path(source));
        } else {
            collect_files_under(&repo.root, &source_path, &mut files)?;
        }
    } else {
        for file in explicit_files {
            let normalized = normalize_path(file);
            let path = resolve_repo_relative_path(&repo.root, &normalized)?;
            if !path.is_file() {
                bail!("package file `{normalized}` does not exist or is not a file");
            }
            files.insert(normalized);
        }
    }

    Ok(files
        .into_iter()
        .map(|path| PackageFileSpec {
            path,
            expected_hash: None,
            role: Some("source".to_string()),
        })
        .collect())
}

fn collect_files_under(root: &Path, dir: &Path, files: &mut BTreeSet<String>) -> Result<()> {
    for entry in WalkDir::new(dir).into_iter().filter_entry(|entry| {
        !path_has_component(entry.path(), ".forge")
            && !path_has_component(entry.path(), ".git")
            && !path_has_component(entry.path(), "node_modules")
    }) {
        let entry = entry.with_context(|| format!("walk package source {}", dir.display()))?;
        let path = entry.path();
        if !path.is_file() {
            continue;
        }
        let relative = path
            .strip_prefix(root)
            .with_context(|| format!("make package path relative {}", path.display()))?;
        files.insert(normalize_path(&relative.to_string_lossy()));
    }
    Ok(())
}

fn hash_package_path(repo: &Repository, path: &Path) -> Result<String> {
    if path.is_file() {
        return Ok(hash_file(path)?.to_hex().to_string());
    }
    if !path.is_dir() {
        bail!(
            "package source {} is not a file or directory",
            path.display()
        );
    }

    let mut files = BTreeSet::new();
    collect_files_under(&repo.root, path, &mut files)?;
    let mut hasher = blake3::Hasher::new();
    for file in files {
        let path = resolve_repo_relative_path(&repo.root, &file)?;
        let file_hash = hash_file(&path)?.to_hex().to_string();
        hasher.update(file.as_bytes());
        hasher.update(file_hash.as_bytes());
    }
    Ok(hasher.finalize().to_hex().to_string())
}

fn write_package_cache(
    repo: &Repository,
    package: &PackageLockPackage,
) -> Result<PackageCacheReceipt> {
    let cache_root = repo
        .forge_dir
        .join("packages/cache")
        .join(sanitize_package_name(&package.name))
        .join(&package.version);
    fs::create_dir_all(&cache_root)
        .with_context(|| format!("create package cache {}", cache_root.display()))?;

    let mut cached_files = Vec::new();
    for file in &package.files {
        if !file.exists {
            bail!("cannot cache missing package file `{}`", file.path);
        }
        let source = resolve_repo_relative_path(&repo.root, &file.path)?;
        let target = cache_root.join(Path::new(&file.path));
        if let Some(parent) = target.parent() {
            fs::create_dir_all(parent)
                .with_context(|| format!("create package cache dir {}", parent.display()))?;
        }
        fs::copy(&source, &target).with_context(|| {
            format!(
                "copy package file {} to cache {}",
                source.display(),
                target.display()
            )
        })?;
        cached_files.push(CachedPackageFile {
            path: file.path.clone(),
            cache_path: repo_relative_label(repo, &target),
            bytes: file.bytes,
            content_hash: file.content_hash.clone().unwrap_or_default(),
        });
    }

    let cache = PackageCacheReceipt {
        cache_path: repo_relative_label(repo, &cache_root),
        cached_files,
    };
    write_pretty_json(&cache_root.join("manifest.json"), &cache)?;
    Ok(cache)
}

fn sanitize_package_name(name: &str) -> String {
    let sanitized = name
        .chars()
        .map(|ch| {
            if ch.is_ascii_alphanumeric() || matches!(ch, '-' | '_' | '.') {
                ch
            } else {
                '-'
            }
        })
        .collect::<String>()
        .trim_matches('-')
        .to_string();
    if sanitized.is_empty() {
        "package".to_string()
    } else {
        sanitized
    }
}

fn repo_relative_label(repo: &Repository, path: &Path) -> String {
    path.strip_prefix(&repo.root)
        .map(|relative| normalize_path(&relative.to_string_lossy()))
        .unwrap_or_else(|_| normalize_path(&path.to_string_lossy()))
}

fn path_has_component(path: &Path, needle: &str) -> bool {
    path.components().any(|component| {
        component
            .as_os_str()
            .to_str()
            .is_some_and(|value| value.eq_ignore_ascii_case(needle))
    })
}

fn write_pretty_json(path: &Path, value: &impl Serialize) -> Result<()> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent).with_context(|| format!("create {}", parent.display()))?;
    }
    let bytes = serde_json::to_vec_pretty(value).context("serialize package JSON")?;
    fs::write(path, bytes).with_context(|| format!("write {}", path.display()))?;
    Ok(())
}

fn resolve_repo_relative_path(root: &Path, relative: &str) -> Result<PathBuf> {
    let path = Path::new(relative);
    if path.is_absolute() {
        bail!("Forge package paths must be project-relative: `{relative}`");
    }
    if path
        .components()
        .any(|component| matches!(component, Component::ParentDir))
    {
        bail!("Forge package paths must not escape the repo root: `{relative}`");
    }
    Ok(root.join(path))
}

fn normalize_path(path: &str) -> String {
    path.replace('\\', "/")
}

fn is_blake3_hex(value: &str) -> bool {
    value.len() == 64 && value.bytes().all(|byte| byte.is_ascii_hexdigit())
}

fn has_plaintext_secret_marker(value: &str) -> bool {
    let lower = value.to_ascii_lowercase();
    if let Some((scheme, rest)) = lower.split_once("://") {
        let path_start = rest.find('/').unwrap_or(rest.len());
        if scheme != "file" && rest[..path_start].contains('@') {
            return true;
        }
    }

    [
        "token=",
        "secret=",
        "password=",
        "passwd=",
        "access_key=",
        "access-key=",
        "api_key=",
        "apikey=",
        "client_secret=",
    ]
    .iter()
    .any(|marker| lower.contains(marker))
}
