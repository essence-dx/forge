use std::fs;
use std::path::{Path, PathBuf};

use anyhow::{bail, Context, Result};
use serde::{Deserialize, Serialize};

use crate::db::metadata::MetadataDb;

#[derive(Debug, Clone)]
pub struct Repository {
    pub root: PathBuf,
    pub forge_dir: PathBuf,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Config {
    pub chunk_min: u32,
    pub chunk_avg: u32,
    pub chunk_max: u32,
    pub compression_level: i32,
    pub dict_size: usize,
    pub remote_url: Option<String>,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            chunk_min: 64 * 1024,
            chunk_avg: 256 * 1024,
            chunk_max: 1024 * 1024,
            compression_level: 8,
            dict_size: 112_640,
            remote_url: None,
        }
    }
}

impl Repository {
    pub fn discover(start: &Path) -> Result<Self> {
        let mut current = start
            .canonicalize()
            .with_context(|| format!("unable to canonicalize {}", start.display()))?;

        loop {
            let forge_dir = current.join(".forge");
            if forge_dir.is_dir() {
                return Ok(Self {
                    root: current,
                    forge_dir,
                });
            }
            if !current.pop() {
                bail!("not inside a Forge repository (missing .forge directory)");
            }
        }
    }

    pub fn init(path: &Path) -> Result<Self> {
        fs::create_dir_all(path).with_context(|| format!("failed to create {}", path.display()))?;
        let root = path
            .canonicalize()
            .with_context(|| format!("failed to canonicalize {}", path.display()))?;
        let forge_dir = root.join(".forge");

        for rel in [
            "objects/chunks",
            "objects/packs",
            "refs/heads",
            "refs/remotes",
            "manifests",
            "archives/checkouts",
            "packages",
            "packages/cache",
            "receipts",
            "receipts/packages",
            "receipts/checkouts",
            "receipts/checkouts/restores",
            "dictionaries",
            "mirrors",
            "media/chunk-maps",
        ] {
            fs::create_dir_all(forge_dir.join(rel))
                .with_context(|| format!("failed to create .forge/{rel}"))?;
        }

        fs::write(forge_dir.join("HEAD"), b"ref: refs/heads/main\n")
            .context("failed to write HEAD")?;

        let config_toml = toml::to_string_pretty(&Config::default()).context("serialize config")?;
        fs::write(forge_dir.join("config.toml"), config_toml).context("write config.toml")?;

        let db_path = forge_dir.join("metadata.redb");
        MetadataDb::create(&db_path)?;

        Ok(Self { root, forge_dir })
    }

    pub fn objects_dir(&self) -> PathBuf {
        self.forge_dir.join("objects")
    }

    pub fn chunk_path(&self, hash: &[u8; 32]) -> PathBuf {
        let hex_hash = hex::encode(hash);
        self.forge_dir
            .join("objects/chunks")
            .join(&hex_hash[0..2])
            .join(&hex_hash[2..])
    }

    pub fn metadata_db_path(&self) -> PathBuf {
        self.forge_dir.join("metadata.redb")
    }

    pub fn config_path(&self) -> PathBuf {
        self.forge_dir.join("config.toml")
    }

    pub fn remote_registry_path(&self) -> PathBuf {
        self.forge_dir.join("remotes.json")
    }

    pub fn package_manifest_path(&self) -> PathBuf {
        self.forge_dir.join("packages/manifest.json")
    }

    pub fn package_lock_path(&self) -> PathBuf {
        self.forge_dir.join("packages/lock.json")
    }

    pub fn package_status_receipt_path(&self) -> PathBuf {
        self.forge_dir.join("receipts/package-status.json")
    }

    pub fn checkout_archive_dir(&self) -> PathBuf {
        self.forge_dir.join("archives/checkouts")
    }

    pub fn checkout_receipt_dir(&self) -> PathBuf {
        self.forge_dir.join("receipts/checkouts")
    }

    pub fn checkout_restore_receipt_dir(&self) -> PathBuf {
        self.forge_dir.join("receipts/checkouts/restores")
    }

    pub fn head_path(&self) -> PathBuf {
        self.forge_dir.join("HEAD")
    }

    pub fn branch_ref_path(&self, branch: &str) -> PathBuf {
        self.forge_dir.join("refs/heads").join(branch)
    }

    pub fn remote_ref_path(&self, remote: &str, branch: &str) -> PathBuf {
        self.forge_dir
            .join("refs/remotes")
            .join(remote)
            .join(branch)
    }

    pub fn mirrors_dir(&self) -> PathBuf {
        self.forge_dir.join("mirrors")
    }

    pub fn mirror_run_path(&self, commit_id_hex: &str) -> PathBuf {
        self.mirrors_dir().join(format!("{commit_id_hex}.json"))
    }

    pub fn mirror_run_path_for_remote(&self, remote: &str, commit_id_hex: &str) -> PathBuf {
        self.mirrors_dir().join(format!(
            "{}--{commit_id_hex}.json",
            sanitize_path_component(remote)
        ))
    }

    pub fn read_head(&self) -> Result<Option<[u8; 32]>> {
        let head = fs::read_to_string(self.head_path()).context("failed to read HEAD")?;
        if let Some(reference) = head.strip_prefix("ref: ") {
            let rel = reference.trim();
            let ref_path = self.forge_dir.join(rel);
            if !ref_path.exists() {
                return Ok(None);
            }
            let commit_hex = fs::read_to_string(&ref_path)
                .with_context(|| format!("failed to read ref {}", rel))?
                .trim()
                .to_string();
            if commit_hex.is_empty() {
                return Ok(None);
            }
            let bytes = hex::decode(&commit_hex).context("invalid commit id in ref")?;
            let id = bytes
                .try_into()
                .map_err(|_| anyhow::anyhow!("invalid commit id length in ref"))?;
            return Ok(Some(id));
        }

        let commit_hex = head.trim();
        if commit_hex.is_empty() {
            return Ok(None);
        }
        let bytes = hex::decode(commit_hex).context("invalid detached HEAD id")?;
        let id = bytes
            .try_into()
            .map_err(|_| anyhow::anyhow!("invalid detached HEAD length"))?;
        Ok(Some(id))
    }

    pub fn read_head_reference(&self) -> Result<Option<String>> {
        let head = fs::read_to_string(self.head_path()).context("failed to read HEAD")?;
        Ok(head
            .strip_prefix("ref: ")
            .map(|reference| reference.trim().to_string()))
    }

    pub fn current_branch_name(&self) -> Result<Option<String>> {
        Ok(self
            .read_head_reference()?
            .and_then(|reference| reference.strip_prefix("refs/heads/").map(str::to_string)))
    }

    pub fn read_branch_ref(&self, branch: &str) -> Result<Option<[u8; 32]>> {
        self.read_commit_ref(&self.branch_ref_path(branch))
    }

    pub fn write_branch_ref(&self, branch: &str, commit_id: &[u8; 32]) -> Result<()> {
        self.write_commit_ref(&self.branch_ref_path(branch), commit_id)
    }

    pub fn read_remote_ref(&self, remote: &str, branch: &str) -> Result<Option<[u8; 32]>> {
        self.read_commit_ref(&self.remote_ref_path(remote, branch))
    }

    pub fn write_remote_ref(&self, remote: &str, branch: &str, commit_id: &[u8; 32]) -> Result<()> {
        self.write_commit_ref(&self.remote_ref_path(remote, branch), commit_id)
    }

    pub fn attach_head_to_branch(&self, branch: &str) -> Result<()> {
        fs::write(self.head_path(), format!("ref: refs/heads/{branch}\n"))
            .context("write HEAD branch reference")?;
        Ok(())
    }

    pub fn update_head(&self, commit_id: &[u8; 32]) -> Result<()> {
        let head = fs::read_to_string(self.head_path()).context("failed to read HEAD")?;
        let hex_id = hex::encode(commit_id);
        if let Some(reference) = head.strip_prefix("ref: ") {
            let rel = reference.trim();
            let ref_path = self.forge_dir.join(rel);
            if let Some(parent) = ref_path.parent() {
                fs::create_dir_all(parent).with_context(|| {
                    format!("failed to create parent dirs for ref {}", parent.display())
                })?;
            }
            fs::write(ref_path, format!("{hex_id}\n")).context("failed to update branch ref")?;
        } else {
            fs::write(self.head_path(), format!("{hex_id}\n"))
                .context("failed to update detached HEAD")?;
        }
        Ok(())
    }

    pub fn read_config(&self) -> Result<Config> {
        let raw = fs::read_to_string(self.config_path()).context("failed to read config.toml")?;
        let cfg: Config = toml::from_str(&raw).context("failed to parse config.toml")?;
        Ok(cfg)
    }

    pub fn write_config(&self, config: &Config) -> Result<()> {
        let raw = toml::to_string_pretty(config).context("serialize config.toml")?;
        fs::write(self.config_path(), raw).context("write config.toml")?;
        Ok(())
    }

    fn read_commit_ref(&self, path: &Path) -> Result<Option<[u8; 32]>> {
        if !path.exists() {
            return Ok(None);
        }
        let commit_hex = fs::read_to_string(path)
            .with_context(|| format!("failed to read ref {}", path.display()))?
            .trim()
            .to_string();
        if commit_hex.is_empty() {
            return Ok(None);
        }
        let bytes = hex::decode(&commit_hex).context("invalid commit id in ref")?;
        let id = bytes
            .try_into()
            .map_err(|_| anyhow::anyhow!("invalid commit id length in ref"))?;
        Ok(Some(id))
    }

    fn write_commit_ref(&self, path: &Path, commit_id: &[u8; 32]) -> Result<()> {
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)
                .with_context(|| format!("create parent dirs for ref {}", parent.display()))?;
        }
        fs::write(path, format!("{}\n", hex::encode(commit_id)))
            .with_context(|| format!("write ref {}", path.display()))?;
        Ok(())
    }
}

fn sanitize_path_component(value: &str) -> String {
    let sanitized = value
        .chars()
        .map(|ch| match ch {
            'a'..='z' | 'A'..='Z' | '0'..='9' | '-' | '_' | '.' => ch,
            _ => '_',
        })
        .collect::<String>();
    if sanitized.is_empty() {
        "remote".to_string()
    } else {
        sanitized
    }
}
