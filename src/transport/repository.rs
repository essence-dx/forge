use std::collections::BTreeSet;
use std::fs;

use anyhow::{anyhow, bail, Context, Result};
use tokio::io::{AsyncRead, AsyncWrite};

use crate::core::manifest::{deserialize_commit, serialize_commit, Commit};
use crate::core::repository::Repository;
use crate::store::cas::ChunkStore;
use crate::store::compression;
use crate::transport::protocol::{
    chunk_request_message, client_chunk_message_for_commit, decode_binary_payload,
    push_manifest_message, read_client_message, read_server_message, server_chunk_message,
    server_manifest_message, write_client_message, write_server_message, ClientMessage,
    ServerMessage,
};

#[derive(Debug, Clone)]
pub struct TransportPushReport {
    pub commit_id: String,
    pub manifest_sent: bool,
    pub requested_chunks: usize,
    pub sent_chunks: usize,
    pub complete: bool,
}

#[derive(Debug, Clone)]
pub struct TransportPullReport {
    pub commit_id: String,
    pub manifest_received: bool,
    pub requested_chunks: usize,
    pub received_chunks: usize,
    pub complete: bool,
}

#[derive(Debug, Clone)]
pub struct TransportHandleReport {
    pub commit_id: Option<String>,
    pub response: ServerMessage,
    pub missing_chunks: Vec<String>,
    pub stored_manifest: bool,
    pub stored_chunk: bool,
    pub error_message: Option<String>,
}

pub fn handle_client_message(
    repo: &Repository,
    message: ClientMessage,
) -> Result<TransportHandleReport> {
    match message {
        ClientMessage::Hello { client, version } => Ok(TransportHandleReport {
            commit_id: None,
            response: ServerMessage::Ok {
                message: format!("forge transport ready for {client} {version}"),
            },
            missing_chunks: Vec::new(),
            stored_manifest: false,
            stored_chunk: false,
            error_message: None,
        }),
        ClientMessage::PushManifest {
            commit_id,
            data_b64,
        } => {
            let manifest_bytes = decode_binary_payload(&data_b64)?;
            let (commit, stored_manifest) = store_manifest(repo, &commit_id, &manifest_bytes)?;
            let missing_chunks = collect_missing_chunks(repo, &commit)?;
            let response = if missing_chunks.is_empty() {
                ServerMessage::AckCommit {
                    commit_id: commit_id.clone(),
                }
            } else {
                ServerMessage::NeedChunks {
                    hashes: missing_chunks.clone(),
                }
            };
            Ok(TransportHandleReport {
                commit_id: Some(commit_id),
                response,
                missing_chunks,
                stored_manifest,
                stored_chunk: false,
                error_message: None,
            })
        }
        ClientMessage::ChunkData {
            commit_id,
            hash,
            data_b64,
        } => {
            let compressed_chunk = decode_binary_payload(&data_b64)?;
            let stored_chunk = store_chunk(repo, &hash, &compressed_chunk)?;

            let (response, missing_chunks, resolved_commit_id) = match commit_id {
                Some(commit_id) => {
                    let commit = load_manifest(repo, &commit_id)?;
                    let missing_chunks = collect_missing_chunks(repo, &commit)?;
                    let response = if missing_chunks.is_empty() {
                        ServerMessage::AckCommit {
                            commit_id: commit_id.clone(),
                        }
                    } else {
                        ServerMessage::NeedChunks {
                            hashes: missing_chunks.clone(),
                        }
                    };
                    (response, missing_chunks, Some(commit_id))
                }
                None => (
                    ServerMessage::Ok {
                        message: format!("stored chunk {hash}"),
                    },
                    Vec::new(),
                    None,
                ),
            };

            Ok(TransportHandleReport {
                commit_id: resolved_commit_id,
                response,
                missing_chunks,
                stored_manifest: false,
                stored_chunk,
                error_message: None,
            })
        }
        ClientMessage::ChunkRequest { hash } => {
            let chunk_bytes = load_chunk(repo, &hash)?;
            Ok(TransportHandleReport {
                commit_id: None,
                response: server_chunk_message(&hash, &chunk_bytes),
                missing_chunks: Vec::new(),
                stored_manifest: false,
                stored_chunk: false,
                error_message: None,
            })
        }
        ClientMessage::PullRequest { commit_id } => {
            let manifest_bytes = load_manifest_bytes(repo, &commit_id)?;
            Ok(TransportHandleReport {
                commit_id: Some(commit_id.clone()),
                response: server_manifest_message(&commit_id, &manifest_bytes),
                missing_chunks: Vec::new(),
                stored_manifest: false,
                stored_chunk: false,
                error_message: None,
            })
        }
        ClientMessage::Ack { id } => Ok(TransportHandleReport {
            commit_id: Some(id.clone()),
            response: ServerMessage::Ok {
                message: format!("acknowledged {id}"),
            },
            missing_chunks: Vec::new(),
            stored_manifest: false,
            stored_chunk: false,
            error_message: None,
        }),
    }
}

pub async fn serve_transport_message<IO>(
    repo: &Repository,
    io: &mut IO,
) -> Result<Option<TransportHandleReport>>
where
    IO: AsyncRead + AsyncWrite + Unpin,
{
    let Some(message) = read_client_message(io).await? else {
        return Ok(None);
    };
    let report = match handle_client_message(repo, message) {
        Ok(report) => report,
        Err(error) => TransportHandleReport {
            commit_id: None,
            response: ServerMessage::Error {
                message: error.to_string(),
            },
            missing_chunks: Vec::new(),
            stored_manifest: false,
            stored_chunk: false,
            error_message: Some(error.to_string()),
        },
    };
    write_server_message(io, &report.response).await?;
    Ok(Some(report))
}

pub async fn push_commit_to_transport<IO>(
    repo: &Repository,
    io: &mut IO,
    commit_id: &str,
) -> Result<TransportPushReport>
where
    IO: AsyncRead + AsyncWrite + Unpin,
{
    let manifest_bytes = load_manifest_bytes(repo, commit_id)?;
    write_client_message(io, &push_manifest_message(commit_id, &manifest_bytes)).await?;

    let mut pending_chunks = BTreeSet::new();
    let mut requested_hashes = BTreeSet::new();
    let mut sent_chunks = 0usize;
    let mut complete = false;

    match read_server_message(io).await? {
        Some(ServerMessage::NeedChunks { hashes }) => {
            requested_hashes.extend(hashes.iter().cloned());
            pending_chunks.extend(hashes);
        }
        Some(ServerMessage::AckCommit { commit_id: ack_id }) => {
            if ack_id != commit_id {
                bail!(
                    "unexpected commit ack '{}', expected '{}'",
                    ack_id,
                    commit_id
                );
            }
            complete = true;
        }
        Some(ServerMessage::Error { message }) => {
            bail!("transport peer rejected manifest: {message}")
        }
        Some(other) => bail!("unexpected response to manifest push: {other:?}"),
        None => bail!("transport peer closed while pushing manifest"),
    }

    while let Some(hash) = pending_chunks.iter().next().cloned() {
        pending_chunks.remove(&hash);
        let chunk_bytes = load_chunk(repo, &hash)?;
        write_client_message(
            io,
            &client_chunk_message_for_commit(commit_id, &hash, &chunk_bytes),
        )
        .await?;
        sent_chunks += 1;

        match read_server_message(io).await? {
            Some(ServerMessage::NeedChunks { hashes }) => {
                requested_hashes.extend(hashes.iter().cloned());
                pending_chunks.extend(hashes);
            }
            Some(ServerMessage::AckCommit { commit_id: ack_id }) => {
                if ack_id != commit_id {
                    bail!(
                        "unexpected commit ack '{}', expected '{}'",
                        ack_id,
                        commit_id
                    );
                }
                complete = true;
                pending_chunks.clear();
            }
            Some(ServerMessage::Ok { .. }) => {}
            Some(ServerMessage::Error { message }) => {
                bail!("transport peer rejected chunk '{hash}': {message}")
            }
            Some(other) => bail!("unexpected response while pushing chunk '{hash}': {other:?}"),
            None => bail!("transport peer closed while pushing chunk '{hash}'"),
        }
    }

    if !complete {
        complete = is_commit_complete(repo, commit_id)?;
    }

    Ok(TransportPushReport {
        commit_id: commit_id.to_string(),
        manifest_sent: true,
        requested_chunks: requested_hashes.len(),
        sent_chunks,
        complete,
    })
}

pub async fn pull_commit_from_transport<IO>(
    repo: &Repository,
    io: &mut IO,
    commit_id: &str,
) -> Result<TransportPullReport>
where
    IO: AsyncRead + AsyncWrite + Unpin,
{
    write_client_message(
        io,
        &ClientMessage::PullRequest {
            commit_id: commit_id.to_string(),
        },
    )
    .await?;

    let manifest_bytes = match read_server_message(io).await? {
        Some(ServerMessage::Manifest {
            commit_id: response_commit_id,
            data_b64,
        }) => {
            if response_commit_id != commit_id {
                bail!(
                    "unexpected manifest commit '{}', expected '{}'",
                    response_commit_id,
                    commit_id
                );
            }
            decode_binary_payload(&data_b64)?
        }
        Some(ServerMessage::Error { message }) => bail!("transport peer rejected pull: {message}"),
        Some(other) => bail!("unexpected response to pull request: {other:?}"),
        None => bail!("transport peer closed while pulling manifest"),
    };

    let (commit, _stored_manifest) = store_manifest(repo, commit_id, &manifest_bytes)?;
    let missing_chunks = collect_missing_chunks(repo, &commit)?;
    let requested_chunks = missing_chunks.len();
    let mut received_chunks = 0usize;

    for hash in missing_chunks {
        write_client_message(io, &chunk_request_message(&hash)).await?;
        match read_server_message(io).await? {
            Some(ServerMessage::ChunkData {
                hash: response_hash,
                data_b64,
            }) => {
                if response_hash != hash {
                    bail!("unexpected chunk '{}', expected '{}'", response_hash, hash);
                }
                let chunk_bytes = decode_binary_payload(&data_b64)?;
                store_chunk(repo, &hash, &chunk_bytes)?;
                received_chunks += 1;
            }
            Some(ServerMessage::Error { message }) => {
                bail!("transport peer could not supply chunk '{hash}': {message}")
            }
            Some(other) => bail!("unexpected chunk response for '{hash}': {other:?}"),
            None => bail!("transport peer closed while pulling chunk '{hash}'"),
        }
    }

    Ok(TransportPullReport {
        commit_id: commit_id.to_string(),
        manifest_received: true,
        requested_chunks,
        received_chunks,
        complete: is_commit_complete(repo, commit_id)?,
    })
}

pub fn is_commit_complete(repo: &Repository, commit_id: &str) -> Result<bool> {
    let commit = load_manifest(repo, commit_id)?;
    Ok(collect_missing_chunks(repo, &commit)?.is_empty())
}

fn load_manifest(repo: &Repository, commit_id: &str) -> Result<Commit> {
    let manifest_bytes = load_manifest_bytes(repo, commit_id)?;
    deserialize_commit(&manifest_bytes)
}

fn load_manifest_bytes(repo: &Repository, commit_id: &str) -> Result<Vec<u8>> {
    let manifest_path = repo.forge_dir.join("manifests").join(commit_id);
    fs::read(&manifest_path).with_context(|| format!("read manifest {}", manifest_path.display()))
}

fn store_manifest(
    repo: &Repository,
    commit_id: &str,
    manifest_bytes: &[u8],
) -> Result<(Commit, bool)> {
    let commit = deserialize_commit(manifest_bytes)?;
    let parsed_commit_id = decode_commit_id(commit_id)?;
    if commit.id != parsed_commit_id {
        bail!(
            "manifest commit id '{}' does not match payload '{}'",
            commit_id,
            hex::encode(commit.id)
        );
    }

    let canonical_bytes = serialize_commit(&commit)?;
    let manifest_path = repo.forge_dir.join("manifests").join(commit_id);
    let stored_manifest = if manifest_path.exists() {
        false
    } else {
        fs::write(&manifest_path, canonical_bytes)
            .with_context(|| format!("write manifest {}", manifest_path.display()))?;
        true
    };

    Ok((commit, stored_manifest))
}

fn collect_missing_chunks(repo: &Repository, commit: &Commit) -> Result<Vec<String>> {
    let store = ChunkStore::new(repo.forge_dir.join("objects/chunks"));
    let mut hashes = BTreeSet::new();
    for file in &commit.files {
        for chunk in &file.chunks {
            let hash = blake3::Hash::from(chunk.hash);
            if !store.contains(&hash) {
                hashes.insert(hash.to_hex().to_string());
            }
        }
    }
    Ok(hashes.into_iter().collect())
}

fn store_chunk(repo: &Repository, hash_hex: &str, compressed_chunk: &[u8]) -> Result<bool> {
    let expected_hash = decode_chunk_hash(hash_hex)?;
    let raw_chunk = compression::decompress(compressed_chunk)
        .with_context(|| format!("decompress transported chunk {hash_hex}"))?;
    let actual_hash = *blake3::hash(&raw_chunk).as_bytes();
    if actual_hash != expected_hash {
        bail!(
            "chunk '{}' failed integrity check; payload hash was '{}'",
            hash_hex,
            hex::encode(actual_hash)
        );
    }

    let store = ChunkStore::new(repo.forge_dir.join("objects/chunks"));
    store.store(&blake3::Hash::from(expected_hash), compressed_chunk)
}

fn load_chunk(repo: &Repository, hash_hex: &str) -> Result<Vec<u8>> {
    let hash = decode_chunk_hash(hash_hex)?;
    let store = ChunkStore::new(repo.forge_dir.join("objects/chunks"));
    store
        .read(&blake3::Hash::from(hash))
        .with_context(|| format!("load chunk {hash_hex}"))
}

fn decode_commit_id(commit_id: &str) -> Result<[u8; 32]> {
    let decoded =
        hex::decode(commit_id).with_context(|| format!("decode commit id '{commit_id}'"))?;
    decoded
        .try_into()
        .map_err(|_| anyhow!("invalid commit id length for '{commit_id}'"))
}

fn decode_chunk_hash(hash_hex: &str) -> Result<[u8; 32]> {
    let decoded =
        hex::decode(hash_hex).with_context(|| format!("decode chunk hash '{hash_hex}'"))?;
    decoded
        .try_into()
        .map_err(|_| anyhow!("invalid chunk hash length for '{hash_hex}'"))
}

#[cfg(test)]
mod tests {
    use std::path::Path;
    use std::sync::{Mutex, OnceLock};

    use tempfile::tempdir;
    use tokio::io::duplex;

    use super::*;
    use crate::cli;
    use crate::core::manifest::{ChunkRef, FileEntry, FileType};

    fn cwd_lock() -> &'static Mutex<()> {
        static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
        LOCK.get_or_init(|| Mutex::new(()))
    }

    struct CurrentDirGuard {
        original: std::path::PathBuf,
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

    fn fixture_commit(repo: &Repository, name: &str, data: &[u8]) -> (String, String) {
        let chunk_hash = blake3::hash(data);
        let file_hash = blake3::hash(data);
        let compressed = compression::compress(data, 3).expect("compress chunk");
        let store = ChunkStore::new(repo.forge_dir.join("objects/chunks"));
        store
            .store(&chunk_hash, &compressed)
            .expect("store chunk fixture");

        let commit = Commit {
            id: [7u8; 32],
            parents: Vec::new(),
            files: vec![FileEntry {
                path: name.to_string(),
                size: data.len() as u64,
                file_hash: *file_hash.as_bytes(),
                chunks: vec![ChunkRef {
                    hash: *chunk_hash.as_bytes(),
                    offset: 0,
                    length: data.len() as u32,
                    compressed_length: compressed.len() as u32,
                }],
                mode: 0o100644,
                mtime_ns: 0,
                file_type: FileType::Unknown,
            }],
            message: "fixture".to_string(),
            author: "forge-test".to_string(),
            timestamp_ns: 0,
        };
        let commit_id = hex::encode(commit.id);
        fs::write(
            repo.forge_dir.join("manifests").join(&commit_id),
            serialize_commit(&commit).expect("serialize fixture commit"),
        )
        .expect("write fixture manifest");
        (commit_id, chunk_hash.to_hex().to_string())
    }

    #[test]
    fn handle_push_manifest_requests_missing_chunks() {
        let dir = tempdir().expect("tempdir");
        let repo = Repository::init(dir.path()).expect("init repo");
        let (commit_id, chunk_hash) = fixture_commit(&repo, "notes.txt", b"forge");
        let manifest_bytes = fs::read(repo.forge_dir.join("manifests").join(&commit_id))
            .expect("read fixture manifest");
        let _ = fs::remove_file(repo.forge_dir.join("manifests").join(&commit_id));
        let _ = ChunkStore::new(repo.forge_dir.join("objects/chunks"))
            .remove(&blake3::Hash::from(
                decode_chunk_hash(&chunk_hash).expect("decode chunk hash"),
            ))
            .expect("remove chunk");

        let report =
            handle_client_message(&repo, push_manifest_message(&commit_id, &manifest_bytes))
                .expect("handle manifest");

        assert!(report.stored_manifest);
        assert_eq!(report.missing_chunks, vec![chunk_hash]);
        assert!(matches!(report.response, ServerMessage::NeedChunks { .. }));
    }

    #[tokio::test]
    async fn push_commit_to_transport_transfers_manifest_and_chunks() {
        let source_dir = tempdir().expect("source tempdir");
        let target_dir = tempdir().expect("target tempdir");
        let source_repo = Repository::init(source_dir.path()).expect("init source repo");
        let target_repo = Repository::init(target_dir.path()).expect("init target repo");
        let (commit_id, chunk_hash) = fixture_commit(&source_repo, "notes.txt", b"forge");

        let (mut client_io, mut server_io) = duplex(1 << 20);
        let server_repo = target_repo.clone();
        let server_task = tokio::spawn(async move {
            while serve_transport_message(&server_repo, &mut server_io)
                .await
                .expect("serve message")
                .is_some()
            {}
        });

        let report = push_commit_to_transport(&source_repo, &mut client_io, &commit_id)
            .await
            .expect("push commit");
        drop(client_io);
        server_task.await.expect("server task");

        assert!(report.complete);
        assert_eq!(report.sent_chunks, 1);
        assert!(target_repo
            .forge_dir
            .join("manifests")
            .join(&commit_id)
            .exists());
        assert!(
            ChunkStore::new(target_repo.forge_dir.join("objects/chunks")).contains(
                &blake3::Hash::from(decode_chunk_hash(&chunk_hash).expect("decode chunk hash"))
            )
        );
    }

    #[tokio::test]
    async fn pull_commit_from_transport_restores_manifest_and_chunks() {
        let source_dir = tempdir().expect("source tempdir");
        let target_dir = tempdir().expect("target tempdir");
        let source_repo = Repository::init(source_dir.path()).expect("init source repo");
        let target_repo = Repository::init(target_dir.path()).expect("init target repo");

        let commit_id = with_repo_dir(source_dir.path(), || {
            fs::write("notes.txt", b"forge transport pull\n").expect("write notes");
            cli::add::run(&["notes.txt".to_string()], false).expect("stage notes");
            cli::commit::run("transport fixture").expect("commit notes");
            let repo = Repository::discover(Path::new(".")).expect("discover source repo");
            hex::encode(repo.read_head().expect("read head").expect("head commit"))
        });

        let manifest_path = source_repo.forge_dir.join("manifests").join(&commit_id);
        assert!(manifest_path.exists());

        let (mut client_io, mut server_io) = duplex(1 << 20);
        let server_repo = source_repo.clone();
        let server_task = tokio::spawn(async move {
            while serve_transport_message(&server_repo, &mut server_io)
                .await
                .expect("serve message")
                .is_some()
            {}
        });

        let report = pull_commit_from_transport(&target_repo, &mut client_io, &commit_id)
            .await
            .expect("pull commit");
        drop(client_io);
        server_task.await.expect("server task");

        assert!(report.manifest_received);
        assert!(report.complete);
        assert!(target_repo
            .forge_dir
            .join("manifests")
            .join(&commit_id)
            .exists());
        assert!(is_commit_complete(&target_repo, &commit_id).expect("commit completeness"));
    }

    #[test]
    fn store_chunk_rejects_wrong_hash() {
        let dir = tempdir().expect("tempdir");
        let repo = Repository::init(dir.path()).expect("init repo");
        let compressed = compression::compress(b"forge", 3).expect("compress");
        let error = store_chunk(&repo, &hex::encode([9u8; 32]), &compressed)
            .expect_err("wrong hash should fail");
        assert!(error.to_string().contains("failed integrity check"));
    }
}
