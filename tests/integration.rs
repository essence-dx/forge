use std::fs;
use std::path::{Path, PathBuf};
use std::sync::{Mutex, OnceLock};

use forge::cli;
use forge::{
    build_package_status, build_remote_health_report, build_sync_overview, execute_sync,
    plan_sync_with_registry, remote_definition, retry_job, retry_job_at, upsert_remote,
    write_package_lock, write_package_status_receipt, AuthStore, BranchMapping, MetadataDb,
    PackageIntegrityState, RemoteKind, RemoteRegistry, Repository, SyncDirection, TokenBundle,
};
use tempfile::tempdir;

fn cwd_lock() -> &'static Mutex<()> {
    static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
    LOCK.get_or_init(|| Mutex::new(()))
}

struct CurrentDirGuard {
    original: PathBuf,
}

impl CurrentDirGuard {
    fn change_to(path: &Path) -> Self {
        let original = std::env::current_dir().expect("read current dir");
        std::env::set_current_dir(path).expect("set current dir");
        Self { original }
    }
}

impl Drop for CurrentDirGuard {
    fn drop(&mut self) {
        let _ = std::env::set_current_dir(&self.original);
    }
}

fn with_repo_dir<T>(path: &Path, run: impl FnOnce() -> T) -> T {
    let _guard = cwd_lock().lock().expect("lock cwd guard");
    let _cwd = CurrentDirGuard::change_to(path);
    run()
}

#[test]
fn init_creates_expected_layout() {
    let dir = tempdir().expect("tempdir");
    let repo = Repository::init(dir.path()).expect("init repo");

    assert!(repo.forge_dir.exists());
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
        assert!(repo.forge_dir.join(rel).exists(), "missing {}", rel);
    }

    let head = fs::read_to_string(repo.forge_dir.join("HEAD")).expect("read HEAD");
    assert_eq!(head, "ref: refs/heads/main\n");

    let db = MetadataDb::open(&repo.forge_dir.join("metadata.redb")).expect("open metadata db");
    assert!(db
        .get_all_tracked_files()
        .expect("tracked files")
        .is_empty());
}

#[test]
fn package_add_materializes_local_slice_lock_cache_and_receipts() {
    let dir = tempdir().expect("tempdir");
    let repo = Repository::init(dir.path()).expect("init repo");
    fs::create_dir_all(dir.path().join("components/dashboard-card")).expect("component dir");
    fs::write(
        dir.path().join("components/dashboard-card/index.tsx"),
        b"export function DashboardCard() { return 'forge package'; }\n",
    )
    .expect("write component");
    fs::write(
        dir.path().join("components/dashboard-card/styles.css"),
        b".dashboard-card { display: grid; }\n",
    )
    .expect("write styles");

    with_repo_dir(dir.path(), || {
        cli::package::run_add(
            "dx-www/dashboard-card",
            "0.2.0",
            "components/dashboard-card",
            &[],
            &["react=^19".to_string(), "lucide-react=^0.468".to_string()],
            "source-owned slice, no package-manager install",
            false,
        )
        .expect("package add");
    });

    assert!(repo.package_manifest_path().exists());
    assert!(repo.package_lock_path().exists());
    assert!(repo.package_status_receipt_path().exists());
    assert!(repo
        .forge_dir
        .join("receipts/packages/dx-www-dashboard-card.json")
        .exists());
    assert!(repo
        .forge_dir
        .join("packages/cache/dx-www-dashboard-card/0.2.0/components/dashboard-card/index.tsx")
        .exists());

    let status = build_package_status(&repo).expect("package status");
    assert!(status.package_lock_present);
    assert_eq!(status.summary.package_count, 1);
    assert_eq!(status.summary.valid_packages, 1);
    assert_eq!(status.summary.dependency_constraints, 2);
    assert_eq!(status.packages[0].name, "dx-www/dashboard-card");
    assert_eq!(status.packages[0].source_kind, "local-slice");
    assert_eq!(status.packages[0].source_hash_matches, Some(true));
    assert_eq!(status.packages[0].files.len(), 2);
    assert!(status.packages[0].integrity_hash.is_some());
    assert_eq!(
        status.packages[0].integrity_state,
        PackageIntegrityState::Valid
    );
    assert!(status.packages[0]
        .receipt_paths
        .iter()
        .any(|path| path == ".forge/receipts/packages/dx-www-dashboard-card.json"));

    let add_receipt: serde_json::Value = serde_json::from_slice(
        &fs::read(
            repo.forge_dir
                .join("receipts/packages/dx-www-dashboard-card.json"),
        )
        .expect("read add receipt"),
    )
    .expect("add receipt json");
    assert_eq!(add_receipt["schema"], forge::PACKAGE_ADD_RECEIPT_SCHEMA);
    assert_eq!(add_receipt["format"], forge::PACKAGE_CONTRACT_FORMAT);
    assert_eq!(add_receipt["package"]["name"], "dx-www/dashboard-card");
    assert_eq!(
        add_receipt["cache"]["cached_files"]
            .as_array()
            .unwrap()
            .len(),
        2
    );
    assert_eq!(
        add_receipt["boundary"],
        "forge-owned source slice; no node_modules install performed"
    );
}

#[test]
fn package_status_lock_and_media_receipt_are_real() {
    let dir = tempdir().expect("tempdir");
    let repo = Repository::init(dir.path()).expect("init repo");
    fs::create_dir_all(dir.path().join("components")).expect("components dir");
    fs::create_dir_all(dir.path().join("assets")).expect("assets dir");

    let source = b"export function Button() { return 'forge'; }\n";
    let source_hash = blake3::hash(source).to_hex().to_string();
    fs::write(dir.path().join("components/button.ts"), source).expect("write source");

    let media = [
        0, 0, 0, 16, b'f', b't', b'y', b'p', b'i', b's', b'o', b'm', 0, 0, 0, 0, 0, 0, 0, 18, b'm',
        b'd', b'a', b't', b'f', b'o', b'r', b'g', b'e', b'-',
    ];
    fs::write(dir.path().join("assets/preview.mp4"), media).expect("write media");

    let manifest = serde_json::json!({
        "schema": forge::PACKAGE_MANIFEST_SCHEMA,
        "format": forge::PACKAGE_CONTRACT_FORMAT,
        "packages": [
            {
                "name": "demo/ui-button",
                "version": "0.1.0",
                "source": {
                    "kind": "local-slice",
                    "locator": "components/button.ts",
                    "hash": source_hash
                },
                "dependencies": [
                    {
                        "name": "shadcn/ui/button",
                        "constraint": "0.1.x",
                        "boundary": "source-owned slice, no package-manager install"
                    }
                ],
                "files": [
                    {
                        "path": "components/button.ts",
                        "hash": source_hash,
                        "role": "source"
                    }
                ],
                "receipt_paths": [
                    ".forge/receipts/package-status.json"
                ]
            }
        ],
        "remotes": [
            {
                "name": "local-cache",
                "kind": "local-filesystem",
                "locator": "file://.forge/packages/cache",
                "auth_ref": "none",
                "secret_policy": "no-plaintext-secrets"
            },
            {
                "name": "r2-release",
                "kind": "s3-compatible",
                "locator": "s3://dx-forge/releases",
                "auth_ref": "env:CLOUDFLARE_R2_ACCESS_KEY_ID",
                "secret_policy": "env-only"
            }
        ],
        "media": [
            {
                "asset_id": "preview/video",
                "path": "assets/preview.mp4",
                "media_type": "mp4",
                "preview_receipt": ".forge/receipts/package-status.json",
                "restore_plan": "restore from content hash and chunk map"
            }
        ]
    });
    fs::write(
        repo.package_manifest_path(),
        serde_json::to_vec_pretty(&manifest).expect("manifest json"),
    )
    .expect("write package manifest");

    let status = build_package_status(&repo).expect("package status");
    assert_eq!(status.schema, forge::PACKAGE_STATUS_SCHEMA);
    assert_eq!(status.format, forge::PACKAGE_CONTRACT_FORMAT);
    assert!(!status.package_lock_present);
    assert_eq!(status.summary.package_count, 1);
    assert_eq!(status.summary.remote_count, 2);
    assert_eq!(status.summary.unsafe_remote_count, 0);
    assert_eq!(status.summary.media_asset_count, 1);
    assert!(status.summary.media_chunk_count >= 1);
    assert_eq!(
        status.packages[0].integrity_state,
        PackageIntegrityState::Valid
    );
    assert_eq!(status.packages[0].source_hash_matches, Some(true));
    assert_eq!(status.media[0].media_type, "mp4");
    assert!(status.media[0].content_hash.is_some());

    let lock = write_package_lock(&repo, &status).expect("write package lock");
    assert_eq!(lock.schema, forge::PACKAGE_LOCK_SCHEMA);
    assert_eq!(lock.format, forge::PACKAGE_CONTRACT_FORMAT);
    assert!(repo.package_lock_path().exists());

    let receipt = write_package_status_receipt(&repo, &status).expect("write receipt");
    assert!(receipt.exists());
}

#[test]
fn add_commit_and_checkout_roundtrip() {
    let dir = tempdir().expect("tempdir");
    Repository::init(dir.path()).expect("init repo");

    let file_path = dir.path().join("notes.txt");
    let original = b"forge roundtrip v1\n".to_vec();
    let updated = b"forge roundtrip v2\n".to_vec();
    fs::write(&file_path, &original).expect("write original file");

    let (first_commit, second_commit) = with_repo_dir(dir.path(), || {
        cli::add::run(&["notes.txt".to_string()], false).expect("stage original");
        cli::commit::run("initial snapshot").expect("commit original");

        let repo = Repository::discover(Path::new(".")).expect("discover repo");
        let first_commit = hex::encode(repo.read_head().expect("read head").expect("first head"));

        fs::write("notes.txt", &updated).expect("write updated file");
        cli::add::run(&["notes.txt".to_string()], false).expect("stage updated");
        cli::commit::run("updated snapshot").expect("commit updated");

        let second_commit = hex::encode(repo.read_head().expect("read head").expect("second head"));

        cli::checkout::run(&first_commit).expect("checkout first commit");
        assert_eq!(
            fs::read("notes.txt").expect("read after first checkout"),
            original
        );

        cli::checkout::run(&second_commit).expect("checkout second commit");
        assert_eq!(
            fs::read("notes.txt").expect("read after second checkout"),
            updated
        );

        (first_commit, second_commit)
    });

    assert_ne!(first_commit, second_commit);
}

#[test]
fn checkout_archives_stale_files_before_removing_them() {
    let dir = tempdir().expect("tempdir");
    let repo = Repository::init(dir.path()).expect("init repo");

    let first_commit = with_repo_dir(dir.path(), || {
        fs::write("kept.txt", b"kept v1\n").expect("write kept");
        cli::add::run(&["kept.txt".to_string()], false).expect("stage kept");
        cli::commit::run("kept only").expect("commit kept");

        let repo = Repository::discover(Path::new(".")).expect("discover repo");
        let first_commit = hex::encode(repo.read_head().expect("read head").expect("first head"));

        fs::write("extra.bin", b"non-code stale media payload\n").expect("write extra");
        cli::add::run(&["extra.bin".to_string()], false).expect("stage extra");
        cli::commit::run("add extra").expect("commit extra");

        cli::checkout::run(&first_commit).expect("checkout first commit");
        assert!(!Path::new("extra.bin").exists());
        first_commit
    });

    let receipt_dir = repo.forge_dir.join("receipts/checkouts");
    let receipts = fs::read_dir(&receipt_dir)
        .expect("checkout receipt dir")
        .filter(|entry| {
            entry
                .as_ref()
                .map(|entry| entry.path().is_file())
                .unwrap_or(false)
        })
        .collect::<Result<Vec<_>, _>>()
        .expect("checkout receipts");
    assert_eq!(receipts.len(), 1);

    let receipt: serde_json::Value = serde_json::from_slice(
        &fs::read(receipts[0].path()).expect("read checkout archive receipt"),
    )
    .expect("checkout archive receipt json");
    assert_eq!(receipt["schema"], "forge.checkout_archive_receipt");
    assert_eq!(receipt["format"], 1);
    assert_eq!(receipt["target_commit"], first_commit);
    assert_eq!(receipt["archived_count"], 1);
    assert_eq!(receipt["archived_files"][0]["path"], "extra.bin");
    assert_eq!(receipt["archived_files"][0]["compression"], "zstd");

    let archive_rel = receipt["archived_files"][0]["archive_path"]
        .as_str()
        .expect("archive path");
    let archive_path = dir.path().join(archive_rel);
    let archived =
        forge::store::compression::decompress(&fs::read(archive_path).expect("read archive"))
            .expect("decompress archive");
    assert_eq!(archived, b"non-code stale media payload\n");
}

#[test]
fn checkout_archive_restore_rehydrates_file_and_writes_receipt() {
    let dir = tempdir().expect("tempdir");
    let repo = Repository::init(dir.path()).expect("init repo");
    let payload = b"restore this media payload\n";

    let receipt_path = with_repo_dir(dir.path(), || {
        fs::write("kept.txt", b"kept v1\n").expect("write kept");
        cli::add::run(&["kept.txt".to_string()], false).expect("stage kept");
        cli::commit::run("kept only").expect("commit kept");

        let repo = Repository::discover(Path::new(".")).expect("discover repo");
        let first_commit = hex::encode(repo.read_head().expect("read head").expect("first head"));

        fs::write("extra.bin", payload).expect("write extra");
        cli::add::run(&["extra.bin".to_string()], false).expect("stage extra");
        cli::commit::run("add extra").expect("commit extra");

        cli::checkout::run(&first_commit).expect("checkout first commit");
        let receipt_path = fs::read_dir(repo.checkout_receipt_dir())
            .expect("checkout receipt dir")
            .find_map(|entry| {
                let path = entry.ok()?.path();
                path.is_file().then_some(path)
            })
            .expect("checkout archive receipt");

        cli::checkout_archive::run_restore(
            receipt_path.to_str().expect("receipt path"),
            Some("extra.bin"),
            true,
            false,
        )
        .expect("restore archive");
        assert_eq!(fs::read("extra.bin").expect("restored extra"), payload);

        receipt_path
    });

    let restore_dir = repo.forge_dir.join("receipts/checkouts/restores");
    let receipts = fs::read_dir(&restore_dir)
        .expect("restore receipt dir")
        .collect::<Result<Vec<_>, _>>()
        .expect("restore receipts");
    assert_eq!(receipts.len(), 1);

    let receipt: serde_json::Value =
        serde_json::from_slice(&fs::read(receipts[0].path()).expect("read restore receipt"))
            .expect("restore receipt json");
    assert_eq!(receipt["schema"], "forge.checkout_archive_restore_receipt");
    assert_eq!(receipt["format"], 1);
    assert_eq!(
        receipt["source_receipt"].as_str(),
        Some(receipt_path.to_string_lossy().as_ref())
    );
    assert_eq!(receipt["restored_count"], 1);
    assert_eq!(receipt["restored_files"][0]["path"], "extra.bin");
    let payload_hash = blake3::hash(payload).to_hex().to_string();
    assert_eq!(
        receipt["restored_files"][0]["content_hash"].as_str(),
        Some(payload_hash.as_str())
    );
}

#[test]
fn sync_overview_reports_primary_remote_and_auth_state() {
    let dir = tempdir().expect("tempdir");
    let repo = Repository::init(dir.path()).expect("init repo");

    let mut config = repo.read_config().expect("read config");
    config.remote_url = Some("https://github.com/acme/forge-demo.git".to_string());
    fs::write(
        repo.config_path(),
        toml::to_string_pretty(&config).expect("serialize config"),
    )
    .expect("write config");

    let auth = AuthStore::open(&repo.forge_dir).expect("open auth store");
    auth.save(
        "github",
        &TokenBundle {
            access_token: "token".to_string(),
            refresh_token: None,
            expires_at: None,
            extra: serde_json::Value::Null,
        },
    )
    .expect("save github token");

    let db = MetadataDb::open(&repo.metadata_db_path()).expect("open db");
    let overview = build_sync_overview(&repo, &db, &auth).expect("build sync overview");
    let primary = overview.primary_remote.expect("primary remote");

    assert_eq!(primary.name, "origin");
    assert!(primary.authenticated);
    assert_eq!(
        primary.locator.as_deref(),
        Some("https://github.com/acme/forge-demo.git")
    );
    assert_eq!(overview.authenticated_backends.len(), 1);
    assert_eq!(overview.authenticated_backends[0].name, "github");
}

#[test]
fn sync_plan_reports_missing_auth_once_per_remote() {
    let dir = tempdir().expect("tempdir");
    let repo = Repository::init(dir.path()).expect("init repo");
    let auth = AuthStore::open(&repo.forge_dir).expect("open auth");
    let registry = RemoteRegistry {
        version: 1,
        primary: Some("origin".to_string()),
        remotes: vec![remote_definition(
            "origin",
            RemoteKind::GitHub,
            "https://github.com/acme/forge-demo.git",
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

    let plan =
        plan_sync_with_registry(&repo, &auth, &registry, None).expect("plan sync with auth gap");
    let auth_conflicts = plan
        .conflicts
        .iter()
        .filter(|conflict| {
            conflict.remote.as_deref() == Some("origin")
                && conflict.summary.contains("authentication")
        })
        .count();
    assert_eq!(auth_conflicts, 1);
}

#[test]
fn sync_plan_reports_missing_remote_ref_for_pull() {
    let dir = tempdir().expect("tempdir");
    let repo = Repository::init(dir.path()).expect("init repo");
    let auth = AuthStore::open(&repo.forge_dir).expect("open auth");
    let registry = RemoteRegistry {
        version: 1,
        primary: Some("origin".to_string()),
        remotes: vec![remote_definition(
            "origin",
            RemoteKind::GitHub,
            "https://github.com/acme/forge-demo.git",
            None,
            vec![BranchMapping {
                local: "main".to_string(),
                remote: "main".to_string(),
                direction: SyncDirection::Pull,
                enabled: true,
            }],
            true,
        )],
    };

    let plan = plan_sync_with_registry(&repo, &auth, &registry, None).expect("plan pull sync");
    assert!(plan.conflicts.iter().any(|conflict| {
        conflict.summary.contains("has no tracked branch ref")
            && conflict.remote.as_deref() == Some("origin")
    }));
}

#[test]
fn execute_sync_cancels_when_requested_remote_is_missing() {
    let dir = tempdir().expect("tempdir");
    let repo = Repository::init(dir.path()).expect("init repo");

    let report = execute_sync(&repo, Some("missing"), false, false)
        .expect("execute sync with missing remote");
    assert!(report.results.is_empty());
    assert!(report
        .warnings
        .iter()
        .any(|warning| warning.contains("blocked by 1 unresolved conflict")));
    assert!(report
        .plan
        .conflicts
        .iter()
        .any(|conflict| conflict.summary.contains("requested remote 'missing'")));

    let db = MetadataDb::open(&repo.metadata_db_path()).expect("open db");
    let jobs = forge::list_jobs(&db).expect("list jobs");
    assert!(jobs.iter().any(|job| {
        job.description.contains("execute sync plan")
            && matches!(job.kind, forge::JobKind::SyncRun)
            && matches!(job.status, forge::JobStatus::Cancelled)
    }));
}

#[test]
fn retry_job_reuses_sync_job_and_increments_attempts() {
    let dir = tempdir().expect("tempdir");
    let repo = Repository::init(dir.path()).expect("init repo");

    let _ = execute_sync(&repo, Some("missing"), false, false).expect("initial sync run");
    let db = MetadataDb::open(&repo.metadata_db_path()).expect("open db");
    let first_job = forge::list_jobs(&db)
        .expect("list jobs")
        .into_iter()
        .find(|job| matches!(job.kind, forge::JobKind::SyncRun))
        .expect("sync job present");
    assert_eq!(first_job.attempts, 1);
    drop(db);

    let outcome = retry_job_at(&repo, &first_job.id, first_job.updated_at_unix_ms + 30_000)
        .expect("retry sync job");
    assert_eq!(outcome.job_id, first_job.id);
    assert_eq!(outcome.new_attempts, 2);

    let db = MetadataDb::open(&repo.metadata_db_path()).expect("reopen db");
    let refreshed = forge::load_job(&db, &first_job.id)
        .expect("reload job")
        .expect("job still present");
    assert_eq!(refreshed.attempts, 2);
    assert!(matches!(refreshed.status, forge::JobStatus::Cancelled));
}

#[test]
fn retry_job_blocks_until_backoff_window_elapses() {
    let dir = tempdir().expect("tempdir");
    let repo = Repository::init(dir.path()).expect("init repo");

    let _ = execute_sync(&repo, Some("missing"), false, false).expect("initial sync run");
    let db = MetadataDb::open(&repo.metadata_db_path()).expect("open db");
    let first_job = forge::list_jobs(&db)
        .expect("list jobs")
        .into_iter()
        .find(|job| matches!(job.kind, forge::JobKind::SyncRun))
        .expect("sync job present");
    drop(db);

    let error = retry_job(&repo, &first_job.id).expect_err("backoff should block immediate retry");
    assert!(error
        .to_string()
        .contains("waiting for retry backoff until"));
}

#[test]
fn remote_health_reports_auth_and_last_job_state() {
    let dir = tempdir().expect("tempdir");
    let repo = Repository::init(dir.path()).expect("init repo");
    let auth = AuthStore::open(&repo.forge_dir).expect("open auth");
    upsert_remote(
        &repo,
        &auth,
        remote_definition(
            "origin",
            RemoteKind::GitHub,
            "https://github.com/acme/forge-demo.git",
            Some("github".to_string()),
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
    drop(auth);

    let _ = execute_sync(&repo, Some("origin"), false, false).expect("execute sync");
    let db = MetadataDb::open(&repo.metadata_db_path()).expect("open db");
    let auth = AuthStore::open(&repo.forge_dir).expect("reopen auth");
    let health = build_remote_health_report(&repo, &db, &auth).expect("remote health");
    let origin = health
        .into_iter()
        .find(|remote| remote.name == "origin")
        .expect("origin health");

    assert!(!origin.authenticated);
    assert!(matches!(
        origin.last_job_status,
        Some(forge::JobStatus::Cancelled)
    ));
}

#[test]
fn sync_run_pushes_to_local_transport_remote() {
    let source_dir = tempdir().expect("source tempdir");
    let remote_dir = tempdir().expect("remote tempdir");
    let source_repo = Repository::init(source_dir.path()).expect("init source repo");
    let remote_repo = Repository::init(remote_dir.path()).expect("init remote repo");
    let auth = AuthStore::open(&source_repo.forge_dir).expect("open auth");

    upsert_remote(
        &source_repo,
        &auth,
        remote_definition(
            "lan",
            RemoteKind::ForgeTransport,
            &format!("forge+local://{}", remote_dir.path().display()),
            None,
            vec![BranchMapping {
                local: "main".to_string(),
                remote: "main".to_string(),
                direction: SyncDirection::Push,
                enabled: true,
            }],
            true,
        ),
        true,
    )
    .expect("configure transport remote");
    drop(auth);

    let commit_id = with_repo_dir(source_dir.path(), || {
        fs::write("notes.txt", b"forge transport sync\n").expect("write notes");
        cli::add::run(&["notes.txt".to_string()], false).expect("stage notes");
        cli::commit::run("transport sync fixture").expect("commit notes");
        let repo = Repository::discover(Path::new(".")).expect("discover source repo");
        hex::encode(repo.read_head().expect("read head").expect("head commit"))
    });

    let report = execute_sync(&source_repo, Some("lan"), false, false).expect("execute sync");
    assert!(report.results.iter().any(|result| result.remote == "lan"
        && matches!(result.kind, forge::SyncActionKind::PushBranch)
        && matches!(result.state, forge::SyncActionState::Executed)));

    assert!(remote_repo
        .forge_dir
        .join("manifests")
        .join(&commit_id)
        .exists());
    assert!(remote_repo.read_head().expect("read remote head").is_none());
    assert_eq!(
        source_repo
            .read_remote_ref("lan", "main")
            .expect("read remote ref")
            .map(hex::encode),
        Some(commit_id)
    );
}
