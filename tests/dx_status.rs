use std::fs;
use std::path::Path;

use forge::dx_status::{inspect_dx_status, DxArtifactState, DxMachineKind};
use forge::Repository;
use serializer::llm::convert::CompressionAlgorithm;
use serializer::machine::{
    paths_for_project_cache, source_fingerprint, write_typed_machine_cache, MachineCacheKind,
    MachineCacheSchema, MachineCacheWriteOptions,
};
use serializer::{SerializerOutput, SerializerOutputConfig};
use tempfile::tempdir;

#[derive(rkyv::Archive, rkyv::Serialize)]
struct PackageLaneVisibilityCache {
    lane_count: u32,
}

fn write_dx_source(root: &Path) {
    fs::write(
        root.join("dx"),
        r#"
project(name=dx-devtools version=0.1.0 kind=www-app)

tools[name command enabled output](
serializer "dx serializer" true .dx/serializer
forge "forge dx status --json" true .dx/forge
)
"#,
    )
    .expect("write dx source");
}

fn generate_serializer_machine(root: &Path) {
    let config = SerializerOutputConfig::new()
        .with_output_dir(root.join(".dx/serializer"))
        .with_llm(false)
        .with_machine(true)
        .with_metadata(true)
        .with_compression(CompressionAlgorithm::None);

    SerializerOutput::with_config(config)
        .process_file(&root.join("dx"))
        .expect("generate serializer machine");
}

fn write_forge_package_status_source(root: &Path) -> std::path::PathBuf {
    let source_path = root.join(".dx/forge/package-status.json");
    fs::create_dir_all(source_path.parent().expect("source parent")).expect("create source dir");
    fs::write(
        &source_path,
        serde_json::to_vec_pretty(&serde_json::json!({
            "schema": "dx.www_template.forge_package_status",
            "status": "lock-backed",
            "package_count": 1,
            "package_lane_visibility": [
                {
                    "lane": "ui-components",
                    "status": "present"
                }
            ]
        }))
        .expect("package status json"),
    )
    .expect("write package status source");
    source_path
}

fn generate_typed_forge_cache(root: &Path, source_path: &Path) {
    let paths =
        paths_for_project_cache(root, "www", "forge-package-status", source_path).expect("paths");
    let source = source_fingerprint(source_path).expect("source fingerprint");
    let payload = PackageLaneVisibilityCache { lane_count: 1 };
    let schema = MachineCacheSchema {
        name: "dx.www.forge_package_status",
        version: 2,
        kind: MachineCacheKind::Receipt,
    };

    write_typed_machine_cache(
        &payload,
        &source,
        &paths,
        schema,
        MachineCacheWriteOptions::default(),
    )
    .expect("write typed cache");
}

#[test]
fn dx_status_decodes_serializer_machine_and_validates_metadata() {
    let dir = tempdir().expect("tempdir");
    Repository::init(dir.path()).expect("init repo");
    write_dx_source(dir.path());
    generate_serializer_machine(dir.path());

    let report = inspect_dx_status(dir.path()).expect("inspect dx status");

    assert_eq!(report.schema, "forge.dx_status");
    assert!(!report.package_manifest_configured);
    assert_eq!(report.serializer_machines.len(), 1);

    let machine = &report.serializer_machines[0];
    assert_eq!(machine.kind, DxMachineKind::SerializerDocument);
    assert_eq!(machine.state, DxArtifactState::Fresh);
    assert_eq!(machine.metadata_state, Some(DxArtifactState::Fresh));
    assert!(machine.document_summary.is_some());
    assert_eq!(
        machine
            .metadata
            .as_ref()
            .and_then(|metadata| metadata.source_hash_matches),
        Some(true)
    );
    assert!(report.warnings.is_empty());
}

#[test]
fn dx_status_flags_stale_serializer_machine_metadata() {
    let dir = tempdir().expect("tempdir");
    Repository::init(dir.path()).expect("init repo");
    write_dx_source(dir.path());
    generate_serializer_machine(dir.path());
    fs::write(dir.path().join("dx"), "project(name=changed)\n").expect("stale source");

    let report = inspect_dx_status(dir.path()).expect("inspect dx status");
    let machine = &report.serializer_machines[0];

    assert_eq!(machine.kind, DxMachineKind::SerializerDocument);
    assert_eq!(machine.state, DxArtifactState::Stale);
    assert_eq!(machine.metadata_state, Some(DxArtifactState::Stale));
    assert!(report
        .warnings
        .iter()
        .any(|warning| warning.contains("stale")));
}

#[test]
fn dx_status_recognizes_typed_forge_machine_cache() {
    let dir = tempdir().expect("tempdir");
    Repository::init(dir.path()).expect("init repo");
    let source_path = write_forge_package_status_source(dir.path());
    generate_typed_forge_cache(dir.path(), &source_path);

    let report = inspect_dx_status(dir.path()).expect("inspect dx status");

    assert_eq!(report.typed_caches.len(), 1);
    let cache = &report.typed_caches[0];
    assert_eq!(cache.kind, DxMachineKind::TypedCache);
    assert_eq!(cache.state, DxArtifactState::Fresh);
    assert_eq!(cache.metadata_state, Some(DxArtifactState::Fresh));
    assert_eq!(
        cache
            .metadata
            .as_ref()
            .and_then(|metadata| metadata.cache_schema.as_deref()),
        Some("dx.www.forge_package_status")
    );
    assert_eq!(
        cache
            .metadata
            .as_ref()
            .and_then(|metadata| metadata.cache_version),
        Some(2)
    );
}

#[test]
fn dx_status_cli_prints_json_without_requiring_package_manifest() {
    let dir = tempdir().expect("tempdir");
    Repository::init(dir.path()).expect("init repo");
    write_dx_source(dir.path());
    generate_serializer_machine(dir.path());

    let output = assert_cmd::cargo::cargo_bin_cmd!("forge")
        .arg("--repo-dir")
        .arg(dir.path())
        .args(["dx", "status", "--json"])
        .assert()
        .success()
        .get_output()
        .stdout
        .clone();

    let report: serde_json::Value = serde_json::from_slice(&output).expect("json report");
    assert_eq!(report["schema"], "forge.dx_status");
    assert_eq!(report["package_manifest_configured"], false);
    assert_eq!(report["serializer_machines"].as_array().unwrap().len(), 1);
}
