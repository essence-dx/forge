# Forge

Forge is the code-and-media version-control layer for the broader `dx` stack.

It is being designed for projects that need more than normal Git alone:

- source code
- audio
- video
- images
- 3D assets
- project files
- other large binary media

## Direction

Forge is not meant to be only a GitHub client and not meant to be only for code.

The long-term goal is:

- content-addressed storage for large files
- deduplicated chunking for massive assets
- structure-aware chunking for media formats
- multi-remote synchronization
- support for both developer remotes and media remotes

## Planned Remote Model

Forge should eventually support fan-out syncing to many remotes from one local project, including combinations such as:

- GitHub
- GitLab
- Bitbucket
- object storage
- YouTube
- Sketchfab
- SoundCloud
- Dropbox
- Google Drive
- Mega

The exact remote set will keep evolving, but the rule stays the same: one local source graph, many remote destinations.

## Why Forge Exists In DX

DX is not only a coding product.

DX is intended to plug into:

- code editors
- video editing software
- 3D tools
- creative production workflows
- media-heavy AI pipelines

That means DX needs a version-control and sync layer that understands more than source code repositories. Forge is that layer.

## Relationship To Flow

Flow is the local and remote AI runtime layer.

Forge is the storage, versioning, and multi-remote asset layer.

Together they should allow DX to:

- run local and remote AI
- manage large code and media assets
- move projects across many remotes
- keep creative and engineering workflows in one system

## Current Focus Areas

- repository core
- chunking and structure-aware chunking
- content-addressed storage
- metadata database
- package manifest, lock, and status receipts
- transport and remote sync
- media-aware remote backends
- CLI reference workflow

## Current State

Forge is still incomplete and should be treated as an active build-out project, not a finished product.

High-priority work includes:

- validating the newer QUIC/bootstrap, framed protocol, and transport-backed repository sync flows end to end
- hardening mirror recovery and resumable job behavior at chunk and session depth
- hardening the now transport-aware live sync execution path across more remotes and recovery cases
- widening package lock receipt validation beyond the current DX-WWW launch-template proof and dx-check source metric test
- validating the current core with an active integration suite

What is already real in the source:

- source-owned package manifest parsing under `.forge/packages/manifest.json`
- local source-slice package add via `forge package add`, including deterministic package receipts and `.forge/packages/cache`
- package lock generation under `.forge/packages/lock.json`
- package status receipts under `.forge/receipts/package-status.json`
- DX-WWW launch-template `.dx/forge/package-lock.json`, `forge.package_add_receipt` receipts with numeric `format: 1` for `shadcn/ui/button` and `state/zustand`, local cache manifests, safe remote definitions, and SVG preview chunk receipt generated from `.dx/forge/source-manifest.json`
- package file/source hash checking, dependency constraint recording, and secret-safe remote status modeling
- media asset status receipts with content hash, structure-aware chunk map, preview receipt, and restore plan fields
- checkout archive receipts that zstd-compress stale tracked files before checkout removes them
- checkout archive list/restore commands that verify size and BLAKE3 before restoring archived files
- repository initialization and discovery
- chunked content-addressed storage
- manifest persistence
- add / commit / status / diff / log / checkout flows
- mirror fan-out to multiple media-aware backends
- historical mirror-run receipts and current mirror snapshot state
- authenticated restore for GitHub, GitLab, Bitbucket, Google Drive, Dropbox, and R2
- large-file Dropbox upload via upload sessions
- reusable sync-overview APIs for hosts that need inferred remotes, auth state, and recent mirror history
- persisted remote registry and branch mappings for named remotes
- dry-run sync planning and CLI remote management
- live sync execution APIs plus `forge sync status|plan|run`
- per-remote health reporting across auth state, last job state, and mirror history
- pre-execution sync conflict reporting and forced/dirty-worktree override controls
- durable job persistence plus CLI job inspection and in-place `forge jobs retry`
- retry backoff scheduling, retry eligibility inspection, and source-visible retry timing
- resumable per-file push and pull checkpoints that preserve progress across retries
- remote-specific mirror-run receipts so one remote no longer clobbers another remote's snapshot history
- framed transport protocol helpers for manifest/chunk messages with bounded message sizes
- QUIC endpoint bootstrap helpers for server/client startup plus bidirectional Forge message streams
- repository-aware transport service logic that can store manifests, validate chunks, serve chunk requests, and push/pull commits over the framed transport protocol
- sync-engine support for a first-class Forge transport remote kind, including local transport bridges and QUIC locators

What is still partial:

- Forge package install/update/remove commands are not implemented as a package-manager replacement yet
- `forge package add` is local/source-slice only; it writes receipts and cache entries but does not fetch from a hosted registry
- the package registry boundary is local/source-owned only; it is not a fake hosted registry
- local and Forge transport remotes are the safest proven sync path; wider network providers still need governed validation
- checkout archive safety and restore now exist for stale tracked files, but archive retention policy still needs work
- media chunk maps prove content-aware tracking, but full Git LFS replacement claims still require restore validation across larger real assets
