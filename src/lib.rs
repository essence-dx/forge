pub mod chunking;
pub mod cli;
pub mod core;
pub mod db;
pub mod dx_status;
pub mod jobs;
pub mod mirror;
pub mod packages;
pub mod recovery;
pub mod store;
pub mod sync;
pub mod transport;
pub mod util;

pub use core::manifest::{ChunkRef, Commit, FileEntry, Manifest};
pub use core::repository::{Config, Repository};
pub use db::metadata::MetadataDb;
pub use dx_status::{
    inspect_dx_status, DxArtifactState, DxDocumentSummary, DxMachineKind, DxMachineMetadata,
    DxMachineStatus, DxStatusReport, DX_STATUS_SCHEMA,
};
pub use jobs::{
    can_retry_kind, is_retryable, list_jobs, load_job, persist_job, queue_job, remaining_attempts,
    retry_wait_remaining_ms, update_job_status, JobCheckpoint, JobKind, JobStatus, QueueJobRequest,
    RetryPolicy, StoredJob,
};
pub use mirror::auth::{AuthStore, TokenBundle};
pub use mirror::{
    MediaType, MirrorTarget, StoredMirrorFailure, StoredMirrorFile, StoredMirrorRecord,
    StoredMirrorRun,
};
pub use packages::{
    add_local_slice_package, build_package_lock, build_package_status,
    load_or_create_package_manifest, load_package_manifest, write_package_lock,
    write_package_status_receipt, CachedPackageFile, DependencyConstraint, MediaAssetSpec,
    MediaAssetStatus, PackageAddReceipt, PackageCacheReceipt, PackageFileSpec, PackageFileStatus,
    PackageIntegrityState, PackageLock, PackageLockPackage, PackageManifest, PackageRemote,
    PackageSource, PackageSpec, PackageStatus, PackageStatusReceipt, PackageStatusSummary,
    RemoteStatus, PACKAGE_ADD_RECEIPT_SCHEMA, PACKAGE_CONTRACT_FORMAT, PACKAGE_LOCK_SCHEMA,
    PACKAGE_MANIFEST_SCHEMA, PACKAGE_STATUS_SCHEMA,
};
pub use recovery::{retry_job, retry_job_at, JobRetryOutcome};
pub use store::cas::ChunkStore;
pub use sync::{
    build_remote_health_report, build_sync_overview, discover_authenticated_backends, execute_sync,
    infer_primary_remote, load_recent_mirror_runs, load_remote_registry, parse_branch_mapping,
    parse_remote_kind, parse_sync_direction, plan_sync, plan_sync_with_registry, remote_definition,
    remove_remote, save_remote_registry, upsert_remote, BranchMapping, BranchStrategy,
    ConfiguredRemote, MirrorRunSummary, RemoteCapability, RemoteDefinition, RemoteHealth,
    RemoteKind, RemoteRegistry, SyncAction, SyncActionKind, SyncActionResult, SyncActionState,
    SyncConflict, SyncConflictKind, SyncDirection, SyncExecutionReport, SyncOverview, SyncPlan,
};
pub use transport::protocol::{
    chunk_request_message, client_chunk_message, client_chunk_message_for_commit,
    decode_binary_payload, deserialize_client_message, deserialize_server_message,
    encode_binary_payload, push_manifest_message, read_client_message, read_server_message,
    serialize_client_message, serialize_server_message, server_chunk_message,
    server_manifest_message, write_client_message, write_server_message, ClientMessage,
    ServerMessage,
};
pub use transport::quic::{
    accept_forge_stream, connect_client, open_forge_stream, receive_client_request,
    respond_to_client, send_client_request, start_server, QuicClientConfig, QuicClientSession,
    QuicServerConfig, QuicServerEndpoint,
};
pub use transport::repository::{
    handle_client_message, is_commit_complete, pull_commit_from_transport,
    push_commit_to_transport, serve_transport_message, TransportHandleReport, TransportPullReport,
    TransportPushReport,
};
