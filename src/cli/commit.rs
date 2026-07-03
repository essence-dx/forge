use std::collections::BTreeMap;
use std::fs;

use anyhow::{bail, Context, Result};
use chrono::Utc;

use crate::core::manifest::{deserialize_file_entry, serialize_commit, Commit};
use crate::core::repository::Repository;
use crate::db::metadata::MetadataDb;
use crate::util::human::short_hex;

pub fn run(message: &str) -> Result<()> {
    let cwd = std::env::current_dir().context("get current dir")?;
    let repo = Repository::discover(&cwd)?;
    let db = MetadataDb::open(&repo.metadata_db_path())?;

    let staged = db.get_staged_files()?;
    let mut tracked_entries = BTreeMap::new();
    for (path, bytes) in db.get_all_tracked_files()? {
        tracked_entries.insert(path, bytes);
    }

    let deleted_paths: Vec<String> = tracked_entries
        .keys()
        .filter(|path| !repo.root.join(path.as_str()).exists())
        .cloned()
        .collect();

    if staged.is_empty() && deleted_paths.is_empty() {
        bail!("Nothing staged");
    }

    for path in deleted_paths {
        tracked_entries.remove(&path);
    }

    for (path, bytes) in &staged {
        tracked_entries.insert(path.clone(), bytes.clone());
    }

    let mut tracked_pairs: Vec<(String, Vec<u8>)> = tracked_entries.into_iter().collect();
    tracked_pairs.sort_by(|left, right| left.0.cmp(&right.0));

    let mut files = Vec::with_capacity(tracked_pairs.len());
    for (_path, bytes) in &tracked_pairs {
        files.push(deserialize_file_entry(bytes)?);
    }

    let mut parents = Vec::new();
    if let Some(parent) = repo.read_head()? {
        parents.push(parent);
    }

    let author = std::env::var("GIT_AUTHOR_NAME")
        .or_else(|_| std::env::var("USER"))
        .unwrap_or_else(|_| "unknown".to_string());

    let author_sr = author.clone();

    let timestamp_ns = Utc::now()
        .timestamp_nanos_opt()
        .unwrap_or_else(|| Utc::now().timestamp() * 1_000_000_000);

    let draft = Commit {
        id: [0u8; 32],
        parents,
        files,
        message: message.to_string(),
        author,
        timestamp_ns,
    };

    let draft_bytes = serialize_commit(&draft)?;
    let commit_id = *blake3::hash(&draft_bytes).as_bytes();

    let commit = Commit {
        id: commit_id,
        ..draft
    };
    let commit_bytes = serialize_commit(&commit)?;
    let id_hex = hex::encode(commit_id);

    let manifest_path = repo.forge_dir.join("manifests").join(&id_hex);
    fs::write(&manifest_path, &commit_bytes)
        .with_context(|| format!("write manifest {}", manifest_path.display()))?;

    db.store_commit(&id_hex, &commit_bytes)?;
    db.replace_tracked_files(&tracked_pairs)?;
    repo.update_head(&commit_id)?;
    db.clear_staging()?;

    println!(
        "Committed {} - {} files, message: {}",
        short_hex(&commit_id),
        commit.files.len(),
        message
    );

    // Write .sr for serializer daemon to compile to .machine
    let dx = crate::dx_config::ForgeDxConfig::load();
    let sr_path = dx.sr_path("forge-commit");
    let _ = dx_config::write_sr_file(&sr_path, &[
        ("action", "commit"),
        ("commit_id", &id_hex),
        ("files_count", &commit.files.len().to_string()),
        ("message", message),
        ("author", &author_sr),
        ("timestamp_ns", &timestamp_ns.to_string()),
    ]);

    // Attempt fast .machine readback to verify the pipeline
    if let Some(status) = dx.read_status("forge-commit") {
        tracing::debug!(
            "forge-commit machine cache verified: {} entries",
            status.len()
        );
    }

    Ok(())
}
