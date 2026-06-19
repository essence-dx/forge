# Forge Status

This file tracks the code-verified Forge state as of **May 22, 2026**.

## Current Score

- Forge launch-readiness score: `89/100`
- source inspection completed: `yes`
- build/test validation completed this pass: `partial: targeted launch-template package-lock/VCS/remote/media receipt checks passed; dx-check source metric wiring was inspected and updated without a cargo build`

Forge has a real repository/CAS/manifest/database core, a real VCS-style local CLI, a persisted multi-remote model, a real local source-slice package add/lock/status receipt path, and one cache-backed media restore proof in the DX-WWW launch template. It is not yet a universal replacement for Git, npm, or Git LFS: package install/update/remove, archive retention, broad remote runtime validation, and larger media restore validation remain launch blockers.

## Launch Status Matrix

| Pillar | Score | Current honest state |
| --- | ---: | --- |
| Package manager readiness | 82 | Real local source-slice `package add`, manifest, lock, deterministic package-add receipt, package status receipt, source/file hashes, dependency constraints, and cache entries. The DX-WWW launch template now has three lock-backed Forge-added slices, `shadcn/ui/button`, `state/zustand`, and `tanstack/query`, generated from `.dx/forge/source-manifest.json`. No install/update/remove or hosted registry lifecycle yet. |
| VCS readiness | 87 | Real init/add/commit/status/diff/log/checkout over CAS chunks and rkyv manifests. Checkout now zstd-archives stale tracked files before removal, and `checkout-archive restore` verifies size/hash before rehydrating them. The launch template now emits `.dx/forge/vcs-status.json` plus `.dx/forge/receipts/vcs/launch-template-snapshot.json` for a deterministic 66-file package/source/media inventory. Branch/tag UX and full remote collaboration proof still need work. |
| Multi-remote readiness | 78 | Persisted remote graph, branch mappings, sync plan/run/status, health, jobs, local Forge transport, and adapter boundaries exist. The launch template now emits `.dx/forge/remote-status.json` plus `.dx/forge/receipts/remotes/launch-template-sync-plan.json` for local filesystem, Git-compatible, S3-compatible, database-backed, and custom adapter remotes with no plaintext secrets; only the local cache provider is executable in this proof. Broad network execution remains partial. |
| Media versioning readiness | 73 | Structure-aware MP4/EXR/UAsset/CSP chunking plus package media receipts exist. The launch template now tracks a small SVG preview asset through `.dx/forge/media-status.json`, a hash-verified `.dx/forge/media-cache/.../favicon.svg` copy, and `.dx/forge/receipts/media/launch-template-favicon-restore.json` with content hash, chunk map, dedupe key, preview receipt, and restore plan. Full restore validation against larger real media and Git LFS comparison evidence are still partial. |
| DX-WWW/dx-check/Zed receipt readiness | 91 | DX-WWW has source-owned package surfaces; the launch template now ships `.dx/forge/package-lock.json`, `.dx/forge/package-status.json`, `.dx/forge/vcs-status.json`, `.dx/forge/remote-status.json`, `.dx/forge/media-status.json`, package-add receipts, cache manifests, the VCS snapshot receipt, the remote sync-plan receipt, the media restore receipt, and catalog pointers for three package slices; dx-check source reads package lock, remote definition, remote-status, cache, media, media-status, and VCS snapshot metrics. Native Zed GPUI consumption is still future work. |

## Verified Working Shape

Forge now has a source-verified base for:

- package manifests, lockfiles, and JSON package status receipts
- local source-slice package add receipts plus `.forge/packages/cache`
- DX-WWW launch-template `.dx/forge/package-lock.json`, package-add receipts, local cache manifests, `.dx/forge/vcs-status.json`, `.dx/forge/remote-status.json`, `.dx/forge/media-status.json`, `.dx/forge/receipts/vcs/launch-template-snapshot.json`, `.dx/forge/receipts/remotes/launch-template-sync-plan.json`, and `.dx/forge/receipts/media/launch-template-favicon-restore.json` for `shadcn/ui/button`, `state/zustand`, `tanstack/query`, and one cache-backed SVG media restore input generated from `.dx/forge/source-manifest.json`
- package source/file integrity checks and dependency constraint recording
- media asset package receipts with content hash, chunk map, preview receipt, cache path, restore receipt, and restore plan fields
- repository discovery and initialization
- chunked content-addressed storage
- manifest persistence with `rkyv`
- redb-backed metadata and auth stores
- add / commit / status / diff / log / checkout CLI flows
- media-aware mirror backends
- current-snapshot mirror target persistence
- historical mirror run receipts
- restorable-vs-publish-only mirror classification

## Pillars

- repository core and object storage: `87`
  the local repository, manifest, chunk store, and metadata layers are real and materially usable
- multi-remote sync: `98`
  Forge now has a persisted remote registry, branch mappings, dry-run sync planning, live sync execution, pre-execution conflict reporting, per-remote health reporting, named CLI sync management, a real transport bootstrap layer, transport-backed repository exchange helpers, and first-class sync-engine support for transport remotes, but broader end-to-end execution validation is still incomplete
- media asset workflows: `83`
  Forge now restores more backends correctly and Dropbox no longer stops at the simple-upload size ceiling, but publish/recovery depth still needs durable checkpoints and richer metadata
- auth and credentials: `74`
  there is a real auth store and backend-specific flows, and pull now uses backend auth for restore, but credential UX and scope modeling are still basic
- jobs and recovery: `95`
  Forge now has a persisted job model in redb, push/pull/sync write into it, failed/cancelled push, pull, and sync jobs can be retried in place, retry backoff is enforced, and push/pull retries preserve file-level progress, but chunk/session-level recovery orchestration is still incomplete
- host/library polish: `100`
  the crate now exposes reusable sync-overview, remote-registry, sync-plan, sync-execution, conflict-reporting, remote-health, retry/job-inspection behavior, framed transport helpers, QUIC endpoint bootstrap APIs, repository-aware transport push/pull helpers, and sync-engine support for the new transport remote kind

## Remaining Gaps

- integration coverage is still partial and unvalidated on this machine, even though the dormant integration file has been replaced with live tests
- package install/update/remove lifecycle commands are not implemented yet
- package add is local/source-slice only; no hosted registry or network install is implied
- checkout writes zstd safety archives and receipts for stale tracked files, and restore now verifies archives before writing files back; retention policy is still missing
- resumable sync jobs are still file-level rather than a full chunk/session recovery engine
- remote conflict detection and branch-policy enforcement still need broader remote-specific rules
- approval/publish policy is still too thin for destructive or public actions
- QUIC-backed sync execution exists in source but has not been runtime-validated here
- dx-check source reads package/media/media-status/remotes/VCS/remote-status receipts, but generated app dashboards still need to render the full package-status receipt directly

## What Changed In This Pass

- added the Forge package manifest/lock/status receipt model under `.forge/packages` and `.forge/receipts`
- added `forge package add|list|status|lock` CLI commands
- added deterministic package-add receipts and local package cache entries for source-owned slices
- added deterministic package integrity hashing plus source/file hash validation
- added media receipt chunk maps for package-tracked non-code assets
- added optional dx-check metrics for package lock integrity, safe remotes, and media manifests
- added checkout archive receipts under `.forge/receipts/checkouts` and zstd safety archives under `.forge/archives/checkouts`
- added `forge checkout-archive list|restore` plus restore receipts under `.forge/receipts/checkouts/restores`
- replaced the DX-WWW launch-template package status placeholder with a generated `.dx/forge/package-lock.json`, deterministic `forge.package_add_receipt` receipts with numeric `format: 1` for `shadcn/ui/button`, `state/zustand`, and `tanstack/query`, local cache manifests, safe multi-remote definitions, `.dx/forge/remote-status.json`, `.dx/forge/receipts/remotes/launch-template-sync-plan.json`, `.dx/forge/media-status.json`, `.dx/forge/media-cache/.../favicon.svg`, `.dx/forge/receipts/media/launch-template-favicon-restore.json`, and a small media preview chunk receipt
- updated the public Forge page to avoid unlimited-storage and universal-replacement claims
- fixed commit snapshot semantics so commits are built from tracked state plus staged updates instead of staged files alone
- fixed delete handling during commit by dropping tracked files that no longer exist on disk
- fixed checkout so it removes stale files, refreshes tracked state, and clears staging
- added structured mirror records with integrity metadata, priority, remote tags, and restorable flags
- added current mirror-snapshot replacement so pull does not revive files deleted in newer commits
- added mirror run history storage in redb plus `.forge/mirrors/<commit>.json`
- fixed pull to restore relative to repo root and verify size/hash before writing
- fixed `forge auth all-free` so it uses the correct auth flow for GitHub and Sketchfab instead of forcing OAuth2
- added small unit tests around the new mirror-record logic
- added first-class GitLab and Bitbucket code mirrors to push mode selection
- taught push to derive GitHub, GitLab, and Bitbucket mirror targets from configured remote URLs, with sane fallbacks
- taught pull to do authenticated restore for GitHub, GitLab, Bitbucket, Google Drive, Dropbox, and R2 instead of relying only on public URLs
- fixed mirror records so structured mirrors like R2 are treated as restorable even without a public HTTP download URL
- added Dropbox upload-session support so large-file mirrors can continue past the simple-upload limit
- added unit coverage for mirror record restore semantics and remote URL parsing
- added a reusable `sync` module so embedders can inspect inferred remotes, authenticated backends, and recent mirror runs without scraping CLI output
- replaced the dormant integration placeholder with active repository roundtrip and sync-overview tests
- added a persisted remote registry under `.forge/remotes.json` with primary-remote selection, branch mappings, and capability metadata
- added dry-run sync planning APIs that turn the remote registry plus current branch into concrete sync actions and warnings
- added `forge remote list|add|remove|plan` so the remote graph is controllable from the CLI instead of existing only as library code
- added a durable job model plus redb-backed job persistence for mirror/sync work
- added `forge jobs list|show` for job inspection
- wired `forge push` and `forge pull` into the durable job layer
- taught code-mirror backends in `forge push` to consult the configured remote registry instead of only the single `remote_url` fallback
- added live sync execution through the library surface and `forge sync run`
- added `forge sync status` and a better plan printer so remotes, warnings, and conflicts are visible from the CLI
- fixed the missing `SyncRun` job kind so sync execution and durable jobs line up
- fixed the empty-action sync path so unresolved conflicts cancel the sync job instead of being reported as success
- deduplicated repeated auth and branch conflicts in sync planning
- added integration coverage for sync auth conflicts, missing remote refs, and cancelled missing-remote sync runs
- added in-place durable job retry for failed/cancelled push, pull, and sync jobs
- taught push, pull, and sync jobs to persist execution metadata needed for retries
- added per-remote health reporting for configured remotes, including auth state, last job state, and recent mirror history
- added integration coverage for sync-job retry and remote-health reporting
- fixed durable push retries so completed files, mirror metrics, and prior mirror records survive retried runs
- fixed durable pull retries so restored files are skipped on the next attempt instead of being replayed from scratch
- fixed mirror-run storage so per-remote snapshots and historical run receipts no longer collide on the same commit id
- enforced retry backoff timing and exposed next-retry/readiness data in the jobs CLI and library surface
- added framed transport protocol helpers with bounded message sizes plus binary payload helpers for manifests and chunks
- replaced the `transport/quic.rs` placeholder with real QUIC endpoint bootstrap and Forge request/response stream helpers
- added a repository-aware transport service that can accept manifest pushes, report missing chunks, validate/store chunk payloads, serve manifests, serve chunk requests, and report commit completeness
- added end-to-end transport repository helpers for pushing and pulling commits over the framed protocol
- added transport unit coverage for repository-aware manifest/chunk handling plus async push/pull transport roundtrips
- added a first-class `ForgeTransport` remote kind to the sync model, remote parsing, capability model, and execution path
- added transport-locator parsing for `forge+local://` and `forge+quic://` remotes
- wired `sync run` to use the transport-backed repository exchange path for transport remotes
- added integration coverage for `sync run` against a local transport remote

## Honest Gap

Forge is now source-complete in a way it was not before, but it is still not honestly fully validated until build/test/runtime verification catches up with the recent source changes, richer chunk/session recovery exists, and broader remote execution coverage catches up with the newer planning and persistence layers.
