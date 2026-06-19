use std::collections::BTreeSet;
use std::fs;
use std::io::Write;
use std::path::{Path, PathBuf};

use anyhow::{Context, Result};
use chrono::Utc;
use serde::Serialize;

use crate::core::manifest::{deserialize_commit, serialize_file_entry};
use crate::core::repository::Repository;
use crate::db::metadata::MetadataDb;
use crate::store::cas::ChunkStore;
use crate::store::compression;

const CHECKOUT_ARCHIVE_FORMAT: u16 = 1;
const CHECKOUT_ARCHIVE_SCHEMA: &str = "forge.checkout_archive_receipt";

pub fn run(commit_id_hex: &str) -> Result<()> {
    let cwd = std::env::current_dir().context("get current dir")?;
    let repo = Repository::discover(&cwd)?;
    let store = ChunkStore::new(repo.forge_dir.join("objects/chunks"));
    let db = MetadataDb::open(&repo.metadata_db_path())?;
    let config = repo.read_config()?;

    let manifest_path = repo.forge_dir.join("manifests").join(commit_id_hex);
    let bytes = fs::read(&manifest_path)
        .with_context(|| format!("read manifest {}", manifest_path.display()))?;
    let commit = deserialize_commit(&bytes)?;

    let target_paths: BTreeSet<String> = commit
        .files
        .iter()
        .map(|entry| entry.path.clone())
        .collect();
    let mut archive_receipt = CheckoutArchiveReceipt::new(commit_id_hex);
    for (path, _) in db.get_all_tracked_files()? {
        if target_paths.contains(&path) {
            continue;
        }
        let abs_path = repo.root.join(&path);
        if abs_path.exists() {
            let archived = archive_checkout_file(
                &repo,
                &path,
                &abs_path,
                commit_id_hex,
                config.compression_level,
            )?;
            archive_receipt.archived_files.push(archived);
            fs::remove_file(&abs_path)
                .with_context(|| format!("remove stale file {}", abs_path.display()))?;
        }
    }
    if !archive_receipt.archived_files.is_empty() {
        write_checkout_archive_receipt(&repo, &mut archive_receipt)?;
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
    fs::write(repo.head_path(), format!("{}\n", commit_id_hex)).context("write detached HEAD")?;
    println!(
        "Checked out {} ({} files)",
        &commit_id_hex[..commit_id_hex.len().min(12)],
        commit.files.len()
    );
    Ok(())
}

#[derive(Debug, Serialize)]
struct CheckoutArchiveReceipt {
    schema: &'static str,
    format: u16,
    generated_at_unix_ms: i64,
    target_commit: String,
    archived_count: usize,
    archived_files: Vec<CheckoutArchivedFile>,
}

impl CheckoutArchiveReceipt {
    fn new(target_commit: &str) -> Self {
        Self {
            schema: CHECKOUT_ARCHIVE_SCHEMA,
            format: CHECKOUT_ARCHIVE_FORMAT,
            generated_at_unix_ms: Utc::now().timestamp_millis(),
            target_commit: target_commit.to_string(),
            archived_count: 0,
            archived_files: Vec::new(),
        }
    }
}

#[derive(Debug, Serialize)]
struct CheckoutArchivedFile {
    path: String,
    content_hash: String,
    original_size: u64,
    compressed_size: u64,
    compression: &'static str,
    archive_path: String,
}

fn archive_checkout_file(
    repo: &Repository,
    path: &str,
    abs_path: &Path,
    target_commit: &str,
    compression_level: i32,
) -> Result<CheckoutArchivedFile> {
    let bytes =
        fs::read(abs_path).with_context(|| format!("read stale file {}", abs_path.display()))?;
    let content_hash = blake3::hash(&bytes).to_hex().to_string();
    let compressed = compression::compress(&bytes, compression_level)?;
    let short_commit = &target_commit[..target_commit.len().min(12)];
    let archive_file = format!(
        "{}-{}.zst",
        &content_hash[..16],
        sanitize_archive_path_component(path)
    );
    let archive_abs = repo
        .checkout_archive_dir()
        .join(short_commit)
        .join(&archive_file);
    if let Some(parent) = archive_abs.parent() {
        fs::create_dir_all(parent)
            .with_context(|| format!("create archive dir {}", parent.display()))?;
    }
    fs::write(&archive_abs, &compressed)
        .with_context(|| format!("write checkout archive {}", archive_abs.display()))?;

    Ok(CheckoutArchivedFile {
        path: normalize_receipt_path(path),
        content_hash,
        original_size: bytes.len() as u64,
        compressed_size: compressed.len() as u64,
        compression: "zstd",
        archive_path: normalize_receipt_path(&format!(
            ".forge/archives/checkouts/{short_commit}/{archive_file}"
        )),
    })
}

fn write_checkout_archive_receipt(
    repo: &Repository,
    receipt: &mut CheckoutArchiveReceipt,
) -> Result<PathBuf> {
    fs::create_dir_all(repo.checkout_receipt_dir()).with_context(|| {
        format!(
            "create checkout receipt dir {}",
            repo.checkout_receipt_dir().display()
        )
    })?;
    receipt.archived_count = receipt.archived_files.len();
    let filename = format!(
        "{}-{}.json",
        &receipt.target_commit[..receipt.target_commit.len().min(12)],
        receipt.generated_at_unix_ms
    );
    let path = repo.checkout_receipt_dir().join(filename);
    let bytes = serde_json::to_vec_pretty(receipt).context("serialize checkout archive receipt")?;
    fs::write(&path, bytes)
        .with_context(|| format!("write checkout archive receipt {}", path.display()))?;
    Ok(path)
}

fn sanitize_archive_path_component(path: &str) -> String {
    let sanitized = path
        .chars()
        .map(|ch| match ch {
            'a'..='z' | 'A'..='Z' | '0'..='9' | '-' | '_' | '.' => ch,
            _ => '_',
        })
        .collect::<String>();
    if sanitized.is_empty() {
        "file".to_string()
    } else {
        sanitized
    }
}

fn normalize_receipt_path(path: &str) -> String {
    path.replace('\\', "/")
}
