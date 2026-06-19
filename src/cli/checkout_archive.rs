use std::fs;
use std::path::{Component, Path, PathBuf};

use anyhow::{bail, Context, Result};
use chrono::Utc;
use serde::{Deserialize, Serialize};

use crate::core::repository::Repository;
use crate::store::compression;

const CHECKOUT_ARCHIVE_FORMAT: u16 = 1;
const CHECKOUT_ARCHIVE_SCHEMA: &str = "forge.checkout_archive_receipt";
const CHECKOUT_ARCHIVE_RESTORE_SCHEMA: &str = "forge.checkout_archive_restore_receipt";
const LEGACY_CHECKOUT_ARCHIVE_SCHEMA: &str = "forge.checkout_archive_receipt.v1";

fn checkout_archive_format() -> u16 {
    CHECKOUT_ARCHIVE_FORMAT
}

fn checkout_archive_schema_supported(schema: &str) -> bool {
    schema == CHECKOUT_ARCHIVE_SCHEMA || schema == LEGACY_CHECKOUT_ARCHIVE_SCHEMA
}

pub fn run_list(json: bool) -> Result<()> {
    let repo = Repository::discover(&std::env::current_dir().context("get current dir")?)?;
    let receipts = list_archive_receipts(&repo)?;

    if json {
        println!("{}", serde_json::to_string_pretty(&receipts)?);
        return Ok(());
    }

    if receipts.is_empty() {
        println!("No checkout archive receipts found.");
        return Ok(());
    }

    println!("Checkout archive receipts:");
    for receipt in receipts {
        println!(
            "  {} archived={} target={}",
            receipt.receipt_path.display(),
            receipt.archived_count,
            receipt.target_commit
        );
    }
    Ok(())
}

pub fn run_restore(
    receipt: &str,
    path_filter: Option<&str>,
    overwrite: bool,
    json: bool,
) -> Result<()> {
    let repo = Repository::discover(&std::env::current_dir().context("get current dir")?)?;
    let receipt_path = resolve_archive_receipt_path(&repo, receipt)?;
    let archive_receipt = read_archive_receipt(&receipt_path)?;
    if !checkout_archive_schema_supported(&archive_receipt.schema) {
        bail!(
            "unsupported checkout archive receipt schema `{}`; expected `{}`",
            archive_receipt.schema,
            CHECKOUT_ARCHIVE_SCHEMA
        );
    }
    if archive_receipt.format != CHECKOUT_ARCHIVE_FORMAT {
        bail!(
            "unsupported checkout archive receipt format `{}`; expected `{}`",
            archive_receipt.format,
            CHECKOUT_ARCHIVE_FORMAT
        );
    }

    let mut restore_receipt =
        CheckoutArchiveRestoreReceipt::new(receipt_path.display().to_string(), &archive_receipt);
    for archived in &archive_receipt.archived_files {
        if path_filter.is_some_and(|filter| normalize_path(filter) != archived.path) {
            continue;
        }
        restore_receipt
            .restored_files
            .push(restore_archived_file(&repo, archived, overwrite)?);
    }

    if restore_receipt.restored_files.is_empty() {
        if let Some(filter) = path_filter {
            bail!(
                "checkout archive receipt {} does not contain `{}`",
                receipt_path.display(),
                filter
            );
        }
        bail!(
            "checkout archive receipt {} has no archived files to restore",
            receipt_path.display()
        );
    }

    let restore_path = write_restore_receipt(&repo, &mut restore_receipt)?;
    if json {
        println!("{}", serde_json::to_string_pretty(&restore_receipt)?);
    } else {
        println!(
            "Restored {} archived file(s); receipt: {}",
            restore_receipt.restored_count,
            restore_path.display()
        );
    }
    Ok(())
}

#[derive(Debug, Serialize)]
struct CheckoutArchiveReceiptListEntry {
    receipt_path: PathBuf,
    target_commit: String,
    archived_count: usize,
    generated_at_unix_ms: i64,
}

#[derive(Debug, Deserialize)]
struct CheckoutArchiveReceipt {
    schema: String,
    #[serde(default = "checkout_archive_format")]
    format: u16,
    generated_at_unix_ms: i64,
    target_commit: String,
    archived_count: usize,
    archived_files: Vec<CheckoutArchivedFile>,
}

#[derive(Debug, Deserialize)]
struct CheckoutArchivedFile {
    path: String,
    content_hash: String,
    original_size: u64,
    compressed_size: u64,
    compression: String,
    archive_path: String,
}

#[derive(Debug, Serialize)]
struct CheckoutArchiveRestoreReceipt {
    schema: &'static str,
    format: u16,
    generated_at_unix_ms: i64,
    source_receipt: String,
    target_commit: String,
    restored_count: usize,
    restored_files: Vec<RestoredCheckoutArchiveFile>,
}

impl CheckoutArchiveRestoreReceipt {
    fn new(source_receipt: String, archive_receipt: &CheckoutArchiveReceipt) -> Self {
        Self {
            schema: CHECKOUT_ARCHIVE_RESTORE_SCHEMA,
            format: CHECKOUT_ARCHIVE_FORMAT,
            generated_at_unix_ms: Utc::now().timestamp_millis(),
            source_receipt,
            target_commit: archive_receipt.target_commit.clone(),
            restored_count: 0,
            restored_files: Vec::new(),
        }
    }
}

#[derive(Debug, Serialize)]
struct RestoredCheckoutArchiveFile {
    path: String,
    content_hash: String,
    restored_size: u64,
    archive_path: String,
    compression: String,
}

fn list_archive_receipts(repo: &Repository) -> Result<Vec<CheckoutArchiveReceiptListEntry>> {
    let mut receipts = Vec::new();
    if !repo.checkout_receipt_dir().is_dir() {
        return Ok(receipts);
    }

    for entry in fs::read_dir(repo.checkout_receipt_dir()).with_context(|| {
        format!(
            "read checkout receipt dir {}",
            repo.checkout_receipt_dir().display()
        )
    })? {
        let entry = entry?;
        let path = entry.path();
        if !path.is_file() || path.extension().and_then(|ext| ext.to_str()) != Some("json") {
            continue;
        }
        let receipt = read_archive_receipt(&path)?;
        receipts.push(CheckoutArchiveReceiptListEntry {
            receipt_path: path,
            target_commit: receipt.target_commit,
            archived_count: receipt.archived_count,
            generated_at_unix_ms: receipt.generated_at_unix_ms,
        });
    }

    receipts.sort_by(|left, right| {
        right
            .generated_at_unix_ms
            .cmp(&left.generated_at_unix_ms)
            .then_with(|| left.receipt_path.cmp(&right.receipt_path))
    });
    Ok(receipts)
}

fn read_archive_receipt(path: &Path) -> Result<CheckoutArchiveReceipt> {
    let bytes = fs::read(path)
        .with_context(|| format!("read checkout archive receipt {}", path.display()))?;
    serde_json::from_slice(&bytes)
        .with_context(|| format!("parse checkout archive receipt {}", path.display()))
}

fn resolve_archive_receipt_path(repo: &Repository, receipt: &str) -> Result<PathBuf> {
    let path = Path::new(receipt);
    if path.is_absolute() {
        return Ok(path.to_path_buf());
    }

    let cwd_candidate = std::env::current_dir()
        .context("get current dir")?
        .join(path);
    if cwd_candidate.is_file() {
        return Ok(cwd_candidate);
    }

    let repo_candidate = repo.root.join(path);
    if repo_candidate.is_file() {
        return Ok(repo_candidate);
    }

    let receipt_dir_candidate = repo.checkout_receipt_dir().join(path);
    if receipt_dir_candidate.is_file() {
        return Ok(receipt_dir_candidate);
    }

    bail!("checkout archive receipt `{receipt}` was not found")
}

fn restore_archived_file(
    repo: &Repository,
    archived: &CheckoutArchivedFile,
    overwrite: bool,
) -> Result<RestoredCheckoutArchiveFile> {
    if archived.compression != "zstd" {
        bail!(
            "unsupported checkout archive compression `{}` for `{}`",
            archived.compression,
            archived.path
        );
    }

    let archive_path = resolve_repo_relative_path(&repo.root, &archived.archive_path)?;
    let out_path = resolve_repo_relative_path(&repo.root, &archived.path)?;
    if out_path.exists() && !overwrite {
        bail!(
            "refusing to overwrite existing restored path {}; pass --overwrite to replace it",
            out_path.display()
        );
    }

    let compressed = fs::read(&archive_path)
        .with_context(|| format!("read checkout archive {}", archive_path.display()))?;
    if compressed.len() as u64 != archived.compressed_size {
        bail!(
            "compressed archive size mismatch for `{}`: expected {}, got {}",
            archived.path,
            archived.compressed_size,
            compressed.len()
        );
    }
    let raw = compression::decompress(&compressed)
        .with_context(|| format!("decompress checkout archive {}", archive_path.display()))?;
    if raw.len() as u64 != archived.original_size {
        bail!(
            "restored size mismatch for `{}`: expected {}, got {}",
            archived.path,
            archived.original_size,
            raw.len()
        );
    }
    let content_hash = blake3::hash(&raw).to_hex().to_string();
    if content_hash != archived.content_hash {
        bail!(
            "restored hash mismatch for `{}`: expected {}, got {}",
            archived.path,
            archived.content_hash,
            content_hash
        );
    }

    if let Some(parent) = out_path.parent() {
        fs::create_dir_all(parent)
            .with_context(|| format!("create restore parent dir {}", parent.display()))?;
    }
    fs::write(&out_path, raw)
        .with_context(|| format!("write restored file {}", out_path.display()))?;

    Ok(RestoredCheckoutArchiveFile {
        path: archived.path.clone(),
        content_hash,
        restored_size: archived.original_size,
        archive_path: archived.archive_path.clone(),
        compression: archived.compression.clone(),
    })
}

fn write_restore_receipt(
    repo: &Repository,
    receipt: &mut CheckoutArchiveRestoreReceipt,
) -> Result<PathBuf> {
    fs::create_dir_all(repo.checkout_restore_receipt_dir()).with_context(|| {
        format!(
            "create checkout restore receipt dir {}",
            repo.checkout_restore_receipt_dir().display()
        )
    })?;
    receipt.restored_count = receipt.restored_files.len();
    let filename = format!(
        "{}-{}.json",
        &receipt.target_commit[..receipt.target_commit.len().min(12)],
        receipt.generated_at_unix_ms
    );
    let path = repo.checkout_restore_receipt_dir().join(filename);
    fs::write(&path, serde_json::to_vec_pretty(receipt)?)
        .with_context(|| format!("write checkout restore receipt {}", path.display()))?;
    Ok(path)
}

fn resolve_repo_relative_path(root: &Path, relative: &str) -> Result<PathBuf> {
    let path = Path::new(relative);
    if path.is_absolute()
        || path
            .components()
            .any(|component| matches!(component, Component::ParentDir))
    {
        bail!("checkout archive paths must be repo-relative: `{relative}`");
    }
    Ok(root.join(path))
}

fn normalize_path(path: &str) -> String {
    path.replace('\\', "/")
}
