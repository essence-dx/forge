use std::collections::HashMap;
use std::path::PathBuf;

/// Loaded DX workspace config with forge-specific helpers.
pub struct ForgeDxConfig {
    pub workspace_root: PathBuf,
    pub cache_dir: PathBuf,
    pub sr_dir: PathBuf,
    pub receipts_dir: PathBuf,
    pub global_cache_dir: PathBuf,
}

impl ForgeDxConfig {
    pub fn load() -> Self {
        let config = dx_config::DxConfig::load(
            &std::env::current_dir().unwrap_or_default(),
        )
        .unwrap_or_default();

        let ws = config.workspace.root.clone();
        let cache = config.paths.cache.clone();
        let sr = cache.parent().map(|p| p.join("serializer")).unwrap_or_else(|| ws.join(".dx").join("serializer"));
        let receipts = ws.join(".dx").join("receipts").join("forge");

        Self {
            workspace_root: ws,
            cache_dir: cache,
            sr_dir: sr,
            receipts_dir: receipts,
            global_cache_dir: config.global_cache_dir().to_path_buf(),
        }
    }

    pub fn sr_path(&self, name: &str) -> PathBuf {
        self.sr_dir.join(format!("{}.sr", name))
    }

    /// Get the `.machine` path for a named artifact.
    pub fn machine_path(&self, name: &str) -> PathBuf {
        self.sr_dir.join(format!("{}.machine", name))
    }

    /// Read tool status, trying `.machine` (fast) first, falling back to `.sr`.
    pub fn read_status(&self, name: &str) -> Option<HashMap<String, String>> {
        let sr_path = self.sr_path(name);
        dx_config::read_machine_or_sr(&sr_path)
    }

    pub fn receipt_path(&self, name: &str) -> PathBuf {
        self.receipts_dir.join(name)
    }

    pub fn global_sr_dir(&self) -> PathBuf {
        self.global_cache_dir.join("forge")
    }

    pub fn write_global_sr(&self, name: &str, entries: &[(&str, &str)]) {
        let dir = self.global_sr_dir();
        let path = dir.join(format!("{}.sr", name));
        let _ = dx_config::write_sr_file(&path, entries);
    }
}
