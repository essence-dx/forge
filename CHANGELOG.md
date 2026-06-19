# Changelog

## 2026-05-22

- added a real Forge package manifest, lock, and status receipt model under `.forge/packages` and `.forge/receipts`
- added `forge package list|status|lock` CLI surfaces for package/version receipts without claiming package-manager parity
- added `forge package add` for local source-owned package slices; it hashes file/directory sources, writes the manifest, lock, package-status receipt, deterministic package-add receipt, and `.forge/packages/cache` entries without touching `node_modules`
- added package source/file hash checks, dependency constraint recording, and deterministic package integrity hashing
- added media status receipt entries with content hashes, structure-aware chunk maps, preview receipt paths, and restore plans
- added secret-safe remote status modeling for local/filesystem, Forge transport, and adapter-boundary remotes
- added zstd checkout archives plus JSON checkout archive receipts before stale tracked files are removed during checkout
- added `forge checkout-archive list|restore` so archived stale files can be inspected and restored with size/hash verification
- added launch-template package status metadata that derives package rows from the real launch catalog without pretending a lock was generated
- added launch-template `.dx/forge/package-lock.json`, deterministic package-add receipts, local package cache manifests, safe multi-remote config entries, and a small media preview chunk receipt generated from `.dx/forge/source-manifest.json`
- extended the launch-template lock-backed package proof from one `shadcn/ui/button` slice to two real Forge-added slices by adding `state/zustand` through `dx forge add state/zustand --write`
- added integration coverage for package lock writing, package status receipts, and a tracked MP4 media fixture
- connected dx-check to optional `.dx/forge/package-lock.json`, `.dx/forge/remotes.json`, and `.dx/forge/media-manifest.json` health metrics
- updated the public Forge page to remove unlimited-storage and universal-replacement claims until the source proves them

## 2026-04-26

- inspected the Forge source tree and replaced the old provisional `50/100` estimate with a code-verified status document
- fixed commit snapshot semantics so a commit now reflects tracked state plus staged updates instead of staged files only
- fixed delete handling during commit by dropping tracked files that no longer exist on disk
- fixed checkout so it removes stale files, refreshes tracked metadata, and clears staging
- added structured mirror records with file hash, size, remote tag, priority, and restorable metadata
- added mirror-run history persistence in redb and `.forge/mirrors/<commit>.json`
- fixed pull to restore relative to repo root, prefer better mirrors, and verify downloaded bytes before writing
- fixed `forge auth all-free` to use backend-appropriate auth flows
- added small unit tests for mirror-record upgrade and ordering behavior
- added a Forge-local status baseline document
- added a Forge 99% completion plan focused on multi-remote sync, media assets, jobs, auth, and library-first APIs
- added a Forge-local TODO file so the project has its own execution backlog
- recorded the earlier no-command baseline as historical context, then replaced it with a source-inspected score once command-based inspection was allowed
- added GitLab and Bitbucket as first-class code mirror backends in `forge push`
- taught Forge to derive GitHub, GitLab, and Bitbucket targets from configured remote URLs with sane fallback slugs
- taught `forge pull` to restore from authenticated GitHub, GitLab, Bitbucket, Google Drive, Dropbox, and R2 sources
- fixed structured mirror records so non-public backends like R2 can still be restored correctly
- added Dropbox upload-session support so large media mirrors can continue past the simple-upload limit
- added unit coverage for remote URL parsing and structured restore behavior
- added a reusable sync-overview API for inferred remotes, authenticated backends, and recent mirror-run summaries
- replaced the dormant integration placeholder with active repository roundtrip and sync-overview tests
- added a persisted remote registry with branch mappings and primary-remote selection
- added dry-run sync planning APIs and `forge remote list|add|remove|plan`
- added a durable redb-backed job model and `forge jobs list|show`
- wired `forge push` and `forge pull` into the durable job layer
- taught code-mirror backends in `forge push` to consult the configured remote registry
- added live sync execution APIs and `forge sync status|plan|run`
- fixed the sync job model mismatch by adding a dedicated `SyncRun` job kind
- fixed the empty-action sync edge case so unresolved conflicts cancel the sync job instead of reporting success
- deduplicated repeated sync conflicts per remote/action class
- added integration coverage for auth conflicts, missing remote refs, and cancelled sync runs
- added in-place job retry so failed/cancelled push, pull, and sync jobs can be rerun on the same durable job record
- taught push, pull, and sync jobs to persist enough execution metadata for retries
- added per-remote health reporting across auth state, last durable job state, and recent mirror history
- added integration coverage for sync-job retry and remote-health reporting
- fixed durable push and pull retries so they preserve prior completed-file checkpoints instead of restarting from empty progress
- fixed mirror snapshot updates so one remote no longer overwrites another remote's file-level mirror records
- stored remote-specific mirror-run receipts under distinct keys and filenames instead of collapsing runs by commit only
- enforced retry backoff timing in the recovery path and exposed retry timing/eligibility in `forge jobs list|show`
- added framed transport helpers for bounded client/server protocol messages, including manifest and chunk payload helpers
- replaced the QUIC placeholder with a real endpoint bootstrap layer for server/client startup and bidirectional Forge message streams
- added a repository-aware transport service that can ingest manifests, validate/store compressed chunks, serve manifest/chunk requests, and report commit completeness
- added transport-backed `push_commit_to_transport`, `pull_commit_from_transport`, and `serve_transport_message` helpers so Forge can exchange real repository data over the framed protocol
- extended transport tests with manifest/chunk service coverage and end-to-end push/pull-over-transport repository roundtrips
- wired the sync engine to treat Forge transport as a first-class remote kind with `forge+local://` and `forge+quic://` locator support
- added sync execution support for transport-backed push and pull paths plus integration coverage for `sync run` against a local transport remote
