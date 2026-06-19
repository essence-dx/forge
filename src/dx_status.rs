use std::fs;
use std::path::{Path, PathBuf};

use anyhow::{Context, Result};
use chrono::Utc;
use serde::{Deserialize, Serialize};
use walkdir::WalkDir;

pub const DX_STATUS_SCHEMA: &str = "forge.dx_status";
const DX_STATUS_FORMAT: u16 = 1;
const SERIALIZER_DOCUMENT_MAGIC: &[u8; 4] = b"DXM1";
const TYPED_CACHE_MAGIC: &[u8; 8] = b"DXMCACH1";
const TYPED_CACHE_HEADER_LEN: usize = 256;
const MAX_MACHINE_INSPECTION_BYTES: u64 = 64 * 1024 * 1024;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum DxMachineKind {
    SerializerDocument,
    TypedCache,
    Unknown,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum DxArtifactState {
    Fresh,
    Stale,
    Missing,
    MissingSource,
    MissingMetadata,
    Unchecked,
    Invalid,
    TooLarge,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DxDocumentSummary {
    pub context_entries: usize,
    pub refs: usize,
    pub sections: usize,
    pub section_rows: usize,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub project_name: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DxMachineMetadata {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub schema: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cache_schema: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cache_version: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cache_kind: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub source_path: Option<PathBuf>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub source_bytes: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub source_blake3: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub source_hash_matches: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub machine_path: Option<PathBuf>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub machine_bytes: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub machine_blake3: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub machine_hash_matches: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub payload_bytes: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub payload_blake3: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub payload_hash_matches: Option<bool>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DxMachineStatus {
    pub kind: DxMachineKind,
    pub state: DxArtifactState,
    pub path: PathBuf,
    pub bytes: u64,
    pub blake3: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub metadata_path: Option<PathBuf>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub metadata_state: Option<DxArtifactState>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub metadata: Option<DxMachineMetadata>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub document_summary: Option<DxDocumentSummary>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub warnings: Vec<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DxStatusReport {
    pub schema: &'static str,
    pub format: u16,
    pub generated_at_unix_ms: i64,
    pub project_root: PathBuf,
    pub forge_repository_present: bool,
    pub dx_root_present: bool,
    pub package_manifest_configured: bool,
    pub serializer_machines: Vec<DxMachineStatus>,
    pub typed_caches: Vec<DxMachineStatus>,
    pub unknown_machines: Vec<DxMachineStatus>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub warnings: Vec<String>,
}

#[derive(Debug, Deserialize)]
struct GenericMetadata {
    schema: String,
    source: GenericMetadataFile,
    machine: GenericMetadataFile,
}

#[derive(Debug, Deserialize)]
struct GenericMetadataFile {
    path: String,
    bytes: u64,
    blake3: String,
}

#[derive(Debug, Deserialize)]
struct TypedCacheMetadata {
    cache_schema: String,
    cache_version: u32,
    cache_kind: u32,
    source_path: String,
    source_len: u64,
    source_blake3: String,
    machine_path: String,
    machine_payload_len: u64,
    machine_payload_blake3: String,
}

pub fn inspect_dx_status(project_root: &Path) -> Result<DxStatusReport> {
    let root = project_root
        .canonicalize()
        .with_context(|| format!("unable to canonicalize {}", project_root.display()))?;
    let forge_repository_present = root.join(".forge").is_dir();
    let dx_root = root.join(".dx");
    let dx_root_present = dx_root.is_dir();
    let package_manifest_configured = root.join(".forge/packages/manifest.json").is_file();

    let mut serializer_machines = Vec::new();
    let mut typed_caches = Vec::new();
    let mut unknown_machines = Vec::new();
    let mut warnings = Vec::new();

    for machine_path in machine_paths(&dx_root)? {
        let status = inspect_machine(&root, &machine_path)?;
        warnings.extend(status.warnings.iter().cloned());
        match status.kind {
            DxMachineKind::SerializerDocument => serializer_machines.push(status),
            DxMachineKind::TypedCache => typed_caches.push(status),
            DxMachineKind::Unknown => unknown_machines.push(status),
        }
    }

    Ok(DxStatusReport {
        schema: DX_STATUS_SCHEMA,
        format: DX_STATUS_FORMAT,
        generated_at_unix_ms: Utc::now().timestamp_millis(),
        project_root: root,
        forge_repository_present,
        dx_root_present,
        package_manifest_configured,
        serializer_machines,
        typed_caches,
        unknown_machines,
        warnings,
    })
}

fn machine_paths(dx_root: &Path) -> Result<Vec<PathBuf>> {
    if !dx_root.is_dir() {
        return Ok(Vec::new());
    }

    let mut paths = Vec::new();
    for entry in WalkDir::new(dx_root) {
        let entry = entry?;
        if entry.file_type().is_file()
            && entry
                .path()
                .file_name()
                .and_then(|name| name.to_str())
                .is_some_and(|name| name.ends_with(".machine"))
        {
            paths.push(entry.path().to_path_buf());
        }
    }
    paths.sort();
    Ok(paths)
}

fn inspect_machine(project_root: &Path, path: &Path) -> Result<DxMachineStatus> {
    let metadata = fs::metadata(path).with_context(|| format!("read {}", path.display()))?;
    let bytes_len = metadata.len();
    if bytes_len > MAX_MACHINE_INSPECTION_BYTES {
        return Ok(base_status(
            DxMachineKind::Unknown,
            DxArtifactState::TooLarge,
            path,
            bytes_len,
            None,
        ));
    }

    let bytes = fs::read(path).with_context(|| format!("read {}", path.display()))?;
    let hash = hash_hex(&bytes);
    if bytes.starts_with(TYPED_CACHE_MAGIC) {
        return Ok(inspect_typed_cache(
            project_root,
            path,
            bytes_len,
            hash,
            &bytes,
        ));
    }

    match inspect_serializer_document(project_root, path, bytes_len, hash, &bytes) {
        Ok(status) => Ok(status),
        Err(error) if bytes.starts_with(SERIALIZER_DOCUMENT_MAGIC) => {
            let mut status = base_status(
                DxMachineKind::SerializerDocument,
                DxArtifactState::Invalid,
                path,
                bytes_len,
                Some(hash_hex(&bytes)),
            );
            status.error = Some(error.to_string());
            status.warnings.push(format!(
                "invalid serializer machine cache: {}",
                path.display()
            ));
            Ok(status)
        }
        Err(error) => {
            let mut status = base_status(
                DxMachineKind::Unknown,
                DxArtifactState::Invalid,
                path,
                bytes_len,
                Some(hash_hex(&bytes)),
            );
            status.error = Some(error.to_string());
            Ok(status)
        }
    }
}

fn inspect_serializer_document(
    project_root: &Path,
    path: &Path,
    bytes_len: u64,
    hash: String,
    bytes: &[u8],
) -> Result<DxMachineStatus> {
    let document = serializer::machine_bytes_to_document(bytes)?;
    let metadata_path = metadata_path_for(path);
    let (metadata, metadata_state, state, warnings) =
        inspect_generic_metadata(project_root, &metadata_path, path, bytes)?;

    Ok(DxMachineStatus {
        kind: DxMachineKind::SerializerDocument,
        state,
        path: path.to_path_buf(),
        bytes: bytes_len,
        blake3: hash,
        metadata_path: Some(metadata_path),
        metadata_state: Some(metadata_state),
        metadata,
        document_summary: Some(DxDocumentSummary {
            context_entries: document.context.len(),
            refs: document.refs.len(),
            sections: document.sections.len(),
            section_rows: document
                .sections
                .values()
                .map(|section| section.rows.len())
                .sum(),
            project_name: document
                .get_path("project.name")
                .and_then(serializer::llm::DxLlmValue::as_str)
                .map(ToOwned::to_owned),
        }),
        warnings,
        error: None,
    })
}

fn inspect_typed_cache(
    project_root: &Path,
    path: &Path,
    bytes_len: u64,
    hash: String,
    bytes: &[u8],
) -> DxMachineStatus {
    let metadata_path = metadata_path_for(path);
    let (metadata, metadata_state, state, mut warnings) =
        inspect_typed_metadata(project_root, &metadata_path, bytes);

    if bytes.len() < TYPED_CACHE_HEADER_LEN {
        warnings.push(format!(
            "typed cache header is truncated: {}",
            path.display()
        ));
    }

    DxMachineStatus {
        kind: DxMachineKind::TypedCache,
        state,
        path: path.to_path_buf(),
        bytes: bytes_len,
        blake3: hash,
        metadata_path: Some(metadata_path),
        metadata_state: Some(metadata_state),
        metadata,
        document_summary: None,
        warnings,
        error: None,
    }
}

fn inspect_generic_metadata(
    project_root: &Path,
    metadata_path: &Path,
    machine_path: &Path,
    machine_bytes: &[u8],
) -> Result<(
    Option<DxMachineMetadata>,
    DxArtifactState,
    DxArtifactState,
    Vec<String>,
)> {
    if !metadata_path.is_file() {
        return Ok((
            None,
            DxArtifactState::MissingMetadata,
            DxArtifactState::Unchecked,
            vec![format!(
                "serializer machine metadata is missing: {}",
                metadata_path.display()
            )],
        ));
    }

    let text = fs::read_to_string(metadata_path)
        .with_context(|| format!("read {}", metadata_path.display()))?;
    let parsed: GenericMetadata = serde_json::from_str(&text)
        .with_context(|| format!("parse {}", metadata_path.display()))?;
    let source_path = resolve_recorded_path(project_root, &parsed.source.path);
    let machine_recorded_path = resolve_recorded_path(project_root, &parsed.machine.path);

    let mut metadata = DxMachineMetadata {
        schema: Some(parsed.schema),
        cache_schema: None,
        cache_version: None,
        cache_kind: None,
        source_path: Some(source_path.clone()),
        source_bytes: Some(parsed.source.bytes),
        source_blake3: Some(parsed.source.blake3.clone()),
        source_hash_matches: None,
        machine_path: Some(machine_recorded_path),
        machine_bytes: Some(parsed.machine.bytes),
        machine_blake3: Some(parsed.machine.blake3.clone()),
        machine_hash_matches: Some(parsed.machine.blake3 == hash_hex(machine_bytes)),
        payload_bytes: None,
        payload_blake3: None,
        payload_hash_matches: None,
    };

    if parsed.machine.bytes != machine_bytes.len() as u64
        || metadata.machine_hash_matches != Some(true)
    {
        return Ok((
            Some(metadata),
            DxArtifactState::Invalid,
            DxArtifactState::Invalid,
            vec![format!(
                "serializer machine metadata does not match machine bytes: {}",
                machine_path.display()
            )],
        ));
    }

    let source_bytes = match fs::read(&source_path) {
        Ok(bytes) => bytes,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            return Ok((
                Some(metadata),
                DxArtifactState::MissingSource,
                DxArtifactState::MissingSource,
                vec![format!(
                    "serializer machine source is missing: {}",
                    source_path.display()
                )],
            ));
        }
        Err(error) => return Err(error).with_context(|| format!("read {}", source_path.display())),
    };

    let source_hash_matches = parsed.source.blake3 == hash_hex(&source_bytes);
    metadata.source_hash_matches = Some(source_hash_matches);
    if !source_hash_matches || parsed.source.bytes != source_bytes.len() as u64 {
        return Ok((
            Some(metadata),
            DxArtifactState::Stale,
            DxArtifactState::Stale,
            vec![format!(
                "serializer machine cache is stale for source: {}",
                source_path.display()
            )],
        ));
    }

    Ok((
        Some(metadata),
        DxArtifactState::Fresh,
        DxArtifactState::Fresh,
        Vec::new(),
    ))
}

fn inspect_typed_metadata(
    project_root: &Path,
    metadata_path: &Path,
    machine_bytes: &[u8],
) -> (
    Option<DxMachineMetadata>,
    DxArtifactState,
    DxArtifactState,
    Vec<String>,
) {
    if !metadata_path.is_file() {
        return (
            None,
            DxArtifactState::MissingMetadata,
            DxArtifactState::Unchecked,
            vec![format!(
                "typed machine metadata is missing: {}",
                metadata_path.display()
            )],
        );
    }

    let text = match fs::read_to_string(metadata_path) {
        Ok(text) => text,
        Err(error) => {
            return (
                None,
                DxArtifactState::Invalid,
                DxArtifactState::Invalid,
                vec![format!(
                    "typed machine metadata could not be read: {} ({})",
                    metadata_path.display(),
                    error
                )],
            );
        }
    };
    let parsed: TypedCacheMetadata = match serde_json::from_str(&text) {
        Ok(parsed) => parsed,
        Err(error) => {
            return (
                None,
                DxArtifactState::Invalid,
                DxArtifactState::Invalid,
                vec![format!(
                    "typed machine metadata could not be parsed: {} ({})",
                    metadata_path.display(),
                    error
                )],
            );
        }
    };

    let source_path = resolve_recorded_path(project_root, &parsed.source_path);
    let payload = machine_bytes
        .get(TYPED_CACHE_HEADER_LEN..)
        .unwrap_or_default();
    let payload_hash = hash_hex(payload);
    let payload_hash_matches = payload_hash == parsed.machine_payload_blake3;

    let mut metadata = DxMachineMetadata {
        schema: None,
        cache_schema: Some(parsed.cache_schema),
        cache_version: Some(parsed.cache_version),
        cache_kind: Some(parsed.cache_kind),
        source_path: Some(source_path.clone()),
        source_bytes: Some(parsed.source_len),
        source_blake3: Some(parsed.source_blake3.clone()),
        source_hash_matches: None,
        machine_path: Some(resolve_recorded_path(project_root, &parsed.machine_path)),
        machine_bytes: Some(machine_bytes.len() as u64),
        machine_blake3: None,
        machine_hash_matches: None,
        payload_bytes: Some(parsed.machine_payload_len),
        payload_blake3: Some(parsed.machine_payload_blake3.clone()),
        payload_hash_matches: Some(payload_hash_matches),
    };

    if !payload_hash_matches || payload.len() as u64 != parsed.machine_payload_len {
        return (
            Some(metadata),
            DxArtifactState::Invalid,
            DxArtifactState::Invalid,
            vec![format!(
                "typed machine cache payload does not match metadata: {}",
                metadata_path.display()
            )],
        );
    }

    let source_bytes = match fs::read(&source_path) {
        Ok(bytes) => bytes,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            return (
                Some(metadata),
                DxArtifactState::MissingSource,
                DxArtifactState::MissingSource,
                vec![format!(
                    "typed machine cache source is missing: {}",
                    source_path.display()
                )],
            );
        }
        Err(error) => {
            return (
                Some(metadata),
                DxArtifactState::Invalid,
                DxArtifactState::Invalid,
                vec![format!(
                    "typed machine cache source could not be read: {} ({})",
                    source_path.display(),
                    error
                )],
            );
        }
    };

    let source_hash_matches = parsed.source_blake3 == hash_hex(&source_bytes);
    metadata.source_hash_matches = Some(source_hash_matches);
    if !source_hash_matches || source_bytes.len() as u64 != parsed.source_len {
        return (
            Some(metadata),
            DxArtifactState::Stale,
            DxArtifactState::Stale,
            vec![format!(
                "typed machine cache is stale for source: {}",
                source_path.display()
            )],
        );
    }

    (
        Some(metadata),
        DxArtifactState::Fresh,
        DxArtifactState::Fresh,
        Vec::new(),
    )
}

fn base_status(
    kind: DxMachineKind,
    state: DxArtifactState,
    path: &Path,
    bytes: u64,
    blake3: Option<String>,
) -> DxMachineStatus {
    DxMachineStatus {
        kind,
        state,
        path: path.to_path_buf(),
        bytes,
        blake3: blake3.unwrap_or_default(),
        metadata_path: None,
        metadata_state: None,
        metadata: None,
        document_summary: None,
        warnings: Vec::new(),
        error: None,
    }
}

fn metadata_path_for(machine_path: &Path) -> PathBuf {
    let Some(file_name) = machine_path.file_name().and_then(|name| name.to_str()) else {
        return machine_path.with_extension("machine.meta.json");
    };
    if let Some(stem) = file_name.strip_suffix(".machine") {
        return machine_path.with_file_name(format!("{stem}.machine.meta.json"));
    }
    machine_path.with_extension("machine.meta.json")
}

fn resolve_recorded_path(project_root: &Path, recorded: &str) -> PathBuf {
    let path = PathBuf::from(recorded);
    if path.is_absolute() {
        path
    } else {
        project_root.join(path)
    }
}

fn hash_hex(bytes: &[u8]) -> String {
    blake3::hash(bytes).to_hex().to_string()
}
