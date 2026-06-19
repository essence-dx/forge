use anyhow::{Context, Result};
use base64::Engine as _;
use serde::{Deserialize, Serialize};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

const MAX_FRAME_SIZE_BYTES: usize = 64 * 1024 * 1024;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ClientMessage {
    Hello {
        client: String,
        version: String,
    },
    PushManifest {
        commit_id: String,
        data_b64: String,
    },
    ChunkData {
        #[serde(default, skip_serializing_if = "Option::is_none")]
        commit_id: Option<String>,
        hash: String,
        data_b64: String,
    },
    ChunkRequest {
        hash: String,
    },
    PullRequest {
        commit_id: String,
    },
    Ack {
        id: String,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ServerMessage {
    Ok { message: String },
    Error { message: String },
    NeedChunks { hashes: Vec<String> },
    Manifest { commit_id: String, data_b64: String },
    ChunkData { hash: String, data_b64: String },
    AckCommit { commit_id: String },
}

pub fn serialize_client_message(msg: &ClientMessage) -> Result<Vec<u8>> {
    serde_json::to_vec(msg).context("serialize client message")
}

pub fn deserialize_client_message(bytes: &[u8]) -> Result<ClientMessage> {
    serde_json::from_slice(bytes).context("deserialize client message")
}

pub fn serialize_server_message(msg: &ServerMessage) -> Result<Vec<u8>> {
    serde_json::to_vec(msg).context("serialize server message")
}

pub fn deserialize_server_message(bytes: &[u8]) -> Result<ServerMessage> {
    serde_json::from_slice(bytes).context("deserialize server message")
}

pub fn encode_binary_payload(bytes: &[u8]) -> String {
    base64::engine::general_purpose::STANDARD.encode(bytes)
}

pub fn decode_binary_payload(payload: &str) -> Result<Vec<u8>> {
    base64::engine::general_purpose::STANDARD
        .decode(payload)
        .context("decode binary payload")
}

pub fn push_manifest_message(commit_id: &str, manifest_bytes: &[u8]) -> ClientMessage {
    ClientMessage::PushManifest {
        commit_id: commit_id.to_string(),
        data_b64: encode_binary_payload(manifest_bytes),
    }
}

pub fn client_chunk_message(hash: &str, chunk_bytes: &[u8]) -> ClientMessage {
    ClientMessage::ChunkData {
        commit_id: None,
        hash: hash.to_string(),
        data_b64: encode_binary_payload(chunk_bytes),
    }
}

pub fn client_chunk_message_for_commit(
    commit_id: &str,
    hash: &str,
    chunk_bytes: &[u8],
) -> ClientMessage {
    ClientMessage::ChunkData {
        commit_id: Some(commit_id.to_string()),
        hash: hash.to_string(),
        data_b64: encode_binary_payload(chunk_bytes),
    }
}

pub fn chunk_request_message(hash: &str) -> ClientMessage {
    ClientMessage::ChunkRequest {
        hash: hash.to_string(),
    }
}

pub fn server_manifest_message(commit_id: &str, manifest_bytes: &[u8]) -> ServerMessage {
    ServerMessage::Manifest {
        commit_id: commit_id.to_string(),
        data_b64: encode_binary_payload(manifest_bytes),
    }
}

pub fn server_chunk_message(hash: &str, chunk_bytes: &[u8]) -> ServerMessage {
    ServerMessage::ChunkData {
        hash: hash.to_string(),
        data_b64: encode_binary_payload(chunk_bytes),
    }
}

pub async fn write_client_message<W>(writer: &mut W, msg: &ClientMessage) -> Result<()>
where
    W: AsyncWrite + Unpin,
{
    let bytes = serialize_client_message(msg)?;
    write_frame(writer, &bytes).await
}

pub async fn read_client_message<R>(reader: &mut R) -> Result<Option<ClientMessage>>
where
    R: AsyncRead + Unpin,
{
    read_frame(reader)
        .await?
        .map(|bytes| deserialize_client_message(&bytes))
        .transpose()
}

pub async fn write_server_message<W>(writer: &mut W, msg: &ServerMessage) -> Result<()>
where
    W: AsyncWrite + Unpin,
{
    let bytes = serialize_server_message(msg)?;
    write_frame(writer, &bytes).await
}

pub async fn read_server_message<R>(reader: &mut R) -> Result<Option<ServerMessage>>
where
    R: AsyncRead + Unpin,
{
    read_frame(reader)
        .await?
        .map(|bytes| deserialize_server_message(&bytes))
        .transpose()
}

async fn write_frame<W>(writer: &mut W, bytes: &[u8]) -> Result<()>
where
    W: AsyncWrite + Unpin,
{
    if bytes.len() > MAX_FRAME_SIZE_BYTES {
        anyhow::bail!(
            "transport frame too large: {} bytes exceeds {}",
            bytes.len(),
            MAX_FRAME_SIZE_BYTES
        );
    }

    writer
        .write_u32(bytes.len() as u32)
        .await
        .context("write frame length")?;
    writer
        .write_all(bytes)
        .await
        .context("write frame payload")?;
    writer.flush().await.context("flush framed payload")?;
    Ok(())
}

async fn read_frame<R>(reader: &mut R) -> Result<Option<Vec<u8>>>
where
    R: AsyncRead + Unpin,
{
    let frame_len = match reader.read_u32().await {
        Ok(frame_len) => frame_len as usize,
        Err(error) if error.kind() == std::io::ErrorKind::UnexpectedEof => return Ok(None),
        Err(error) => return Err(error).context("read frame length"),
    };
    if frame_len > MAX_FRAME_SIZE_BYTES {
        anyhow::bail!(
            "transport frame too large: {} bytes exceeds {}",
            frame_len,
            MAX_FRAME_SIZE_BYTES
        );
    }

    let mut payload = vec![0u8; frame_len];
    reader
        .read_exact(&mut payload)
        .await
        .context("read frame payload")?;
    Ok(Some(payload))
}

#[cfg(test)]
mod tests {
    use tokio::io::duplex;

    use super::*;

    #[tokio::test]
    async fn client_message_frame_roundtrip() {
        let (mut writer, mut reader) = duplex(4096);
        let task = tokio::spawn(async move {
            write_client_message(
                &mut writer,
                &ClientMessage::ChunkData {
                    commit_id: None,
                    hash: "abc123".to_string(),
                    data_b64: encode_binary_payload(b"forge"),
                },
            )
            .await
        });

        let message = read_client_message(&mut reader)
            .await
            .expect("read message")
            .expect("message present");
        task.await.expect("writer task").expect("write message");

        match message {
            ClientMessage::ChunkData {
                commit_id,
                hash,
                data_b64,
            } => {
                assert_eq!(commit_id, None);
                assert_eq!(hash, "abc123");
                assert_eq!(
                    decode_binary_payload(&data_b64).expect("decode payload"),
                    b"forge"
                );
            }
            other => panic!("unexpected message: {other:?}"),
        }
    }

    #[tokio::test]
    async fn server_message_frame_roundtrip() {
        let (mut writer, mut reader) = duplex(4096);
        let task = tokio::spawn(async move {
            write_server_message(
                &mut writer,
                &server_manifest_message("deadbeef", b"{\"files\":1}"),
            )
            .await
        });

        let message = read_server_message(&mut reader)
            .await
            .expect("read message")
            .expect("message present");
        task.await.expect("writer task").expect("write message");

        match message {
            ServerMessage::Manifest {
                commit_id,
                data_b64,
            } => {
                assert_eq!(commit_id, "deadbeef");
                assert_eq!(
                    decode_binary_payload(&data_b64).expect("decode manifest"),
                    br#"{"files":1}"#
                );
            }
            other => panic!("unexpected message: {other:?}"),
        }
    }

    #[tokio::test]
    async fn oversized_frame_is_rejected() {
        let (mut writer, mut reader) = duplex(16);
        let task = tokio::spawn(async move {
            writer
                .write_u32((MAX_FRAME_SIZE_BYTES as u32).saturating_add(1))
                .await
                .expect("write oversized frame length");
        });

        let error = read_server_message(&mut reader)
            .await
            .expect_err("oversized frame should fail");
        task.await.expect("writer task");

        assert!(error.to_string().contains("transport frame too large"));
    }
}
