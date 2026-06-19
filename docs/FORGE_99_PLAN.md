# Forge 99 Plan

Forge should become a Rust-first sync and publish engine for both code and media.

## 1. Unified Object Model

Forge needs one internal model for:

- repositories
- remotes
- references
- commits
- branches
- artifacts
- media assets
- manifests
- jobs
- publish targets

Without this, Forge stays a half-connected set of ideas instead of a real engine.

## 2. Multi-Remote Core

Forge should treat multi-remote sync as a native feature.

Required outcomes:

- many remotes per project
- per-remote capability flags
- per-remote auth
- per-remote branch strategy
- dry-run sync planning
- mirror mode
- conflict reporting before execution

## 3. Media-Aware Asset Layer

Forge must support more than source code.

Required outcomes:

- asset fingerprints
- metadata extraction
- preview descriptors
- large artifact manifests
- resumable upload state
- publish-target routing

## 4. Job Engine

Forge needs a durable job model for:

- clone
- fetch
- sync
- mirror
- upload
- publish
- verify
- retry
- cleanup

Every long-running or failure-prone operation should have a recoverable job state.

## 5. Auth And Consent

Forge must not blur remote accounts, credentials, and permissions.

Required outcomes:

- remote account registry
- scope-aware auth model
- explicit publish approvals
- destructive-action approvals
- local-only storage policy for secrets

## 6. Library Surface

Forge should be easy to embed into other Rust hosts.

Required outcomes:

- clear planning APIs
- live execution APIs
- dry-run support
- health/status inspection
- no UI lock-in

## 7. Validation

Forge should not be considered near-complete until:

- multi-remote integration is tested
- resumable job recovery is tested
- media manifest stability is tested
- auth and approval flows are validated
- the library API can be consumed cleanly by another host

## Immediate Constraint

The next real jump is no longer basic source inspection, code-mirror support, remote/job planning, the first live sync path, the first in-place retry path, the first QUIC/protocol bootstrap, the first transport-backed repository exchange path, or sync-engine integration for that transport path. The main remaining constraint is execution validation, richer chunk/session-level resumable recovery depth, and broader multi-remote execution coverage catching up with the newer planning and persistence layers.
