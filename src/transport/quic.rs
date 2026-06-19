use std::net::SocketAddr;
use std::path::{Path, PathBuf};

use anyhow::{Context, Result};
use s2n_quic::{client::Connect, stream::BidirectionalStream, Client, Connection, Server};
use tokio::io::AsyncWriteExt;

use crate::transport::protocol::{
    read_client_message, read_server_message, write_client_message, write_server_message,
    ClientMessage, ServerMessage,
};

#[derive(Debug, Clone)]
pub struct QuicServerConfig {
    pub listen_addr: String,
    pub certificate_path: PathBuf,
    pub private_key_path: PathBuf,
}

impl Default for QuicServerConfig {
    fn default() -> Self {
        Self {
            listen_addr: "127.0.0.1:0".to_string(),
            certificate_path: PathBuf::from("cert.pem"),
            private_key_path: PathBuf::from("key.pem"),
        }
    }
}

#[derive(Debug, Clone)]
pub struct QuicClientConfig {
    pub bind_addr: String,
    pub remote_addr: SocketAddr,
    pub ca_certificate_path: PathBuf,
    pub server_name: String,
    pub keep_alive: bool,
}

#[derive(Debug)]
pub struct QuicServerEndpoint {
    server: Server,
    pub local_addr: SocketAddr,
}

#[derive(Debug)]
pub struct QuicClientSession {
    client: Client,
    pub connection: Connection,
    pub local_addr: SocketAddr,
    pub remote_addr: SocketAddr,
}

impl QuicServerEndpoint {
    pub async fn accept(&mut self) -> Option<Connection> {
        self.server.accept().await
    }

    pub fn server(&self) -> &Server {
        &self.server
    }
}

impl QuicClientSession {
    pub fn client(&self) -> &Client {
        &self.client
    }
}

pub async fn start_server(config: &QuicServerConfig) -> Result<QuicServerEndpoint> {
    validate_server_config(config)?;

    let server = Server::builder()
        .with_tls((
            config.certificate_path.as_path(),
            config.private_key_path.as_path(),
        ))
        .context("configure QUIC server TLS")?
        .with_io(config.listen_addr.as_str())
        .context("configure QUIC server listen address")?
        .start()
        .context("start QUIC server endpoint")?;
    let local_addr = server
        .local_addr()
        .context("read QUIC server local address")?;

    Ok(QuicServerEndpoint { server, local_addr })
}

pub async fn connect_client(config: &QuicClientConfig) -> Result<QuicClientSession> {
    validate_client_config(config)?;

    let client = Client::builder()
        .with_tls(config.ca_certificate_path.as_path())
        .context("configure QUIC client TLS")?
        .with_io(config.bind_addr.as_str())
        .context("configure QUIC client bind address")?
        .start()
        .context("start QUIC client endpoint")?;
    let local_addr = client
        .local_addr()
        .context("read QUIC client local address")?;

    let connect = Connect::new(config.remote_addr).with_server_name(config.server_name.as_str());
    let mut connection = client
        .connect(connect)
        .await
        .context("connect QUIC client")?;
    if config.keep_alive {
        connection
            .keep_alive(true)
            .context("enable QUIC keepalive")?;
    }

    Ok(QuicClientSession {
        client,
        connection,
        local_addr,
        remote_addr: config.remote_addr,
    })
}

pub async fn open_forge_stream(connection: &mut Connection) -> Result<BidirectionalStream> {
    connection
        .open_bidirectional_stream()
        .await
        .context("open QUIC bidirectional stream")
}

pub async fn accept_forge_stream(
    connection: &mut Connection,
) -> Result<Option<BidirectionalStream>> {
    connection
        .accept_bidirectional_stream()
        .await
        .context("accept QUIC bidirectional stream")
}

pub async fn send_client_request(
    connection: &mut Connection,
    message: &ClientMessage,
) -> Result<Option<ServerMessage>> {
    let mut stream = open_forge_stream(connection).await?;
    write_client_message(&mut stream, message)
        .await
        .context("write Forge client message")?;
    stream
        .shutdown()
        .await
        .context("shutdown Forge client stream")?;
    read_server_message(&mut stream)
        .await
        .context("read Forge server response")
}

pub async fn receive_client_request(
    connection: &mut Connection,
) -> Result<Option<(BidirectionalStream, ClientMessage)>> {
    let mut stream = match accept_forge_stream(connection).await? {
        Some(stream) => stream,
        None => return Ok(None),
    };
    let message = match read_client_message(&mut stream)
        .await
        .context("read Forge client request")?
    {
        Some(message) => message,
        None => return Ok(None),
    };
    Ok(Some((stream, message)))
}

pub async fn respond_to_client(
    stream: &mut BidirectionalStream,
    message: &ServerMessage,
) -> Result<()> {
    write_server_message(stream, message)
        .await
        .context("write Forge server response")?;
    stream
        .shutdown()
        .await
        .context("shutdown Forge server stream")?;
    Ok(())
}

fn validate_server_config(config: &QuicServerConfig) -> Result<()> {
    validate_existing_file(&config.certificate_path, "QUIC certificate")?;
    validate_existing_file(&config.private_key_path, "QUIC private key")?;
    Ok(())
}

fn validate_client_config(config: &QuicClientConfig) -> Result<()> {
    validate_existing_file(&config.ca_certificate_path, "QUIC CA certificate")?;
    Ok(())
}

fn validate_existing_file(path: &Path, label: &str) -> Result<()> {
    if !path.is_file() {
        anyhow::bail!("{label} not found at {}", path.display());
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::net::SocketAddr;

    use tempfile::tempdir;

    use super::*;

    #[test]
    fn server_validation_requires_existing_tls_files() {
        let dir = tempdir().expect("tempdir");
        let config = QuicServerConfig {
            listen_addr: "127.0.0.1:0".to_string(),
            certificate_path: dir.path().join("missing-cert.pem"),
            private_key_path: dir.path().join("missing-key.pem"),
        };

        let error = validate_server_config(&config).expect_err("missing files should fail");
        assert!(error.to_string().contains("QUIC certificate not found"));
    }

    #[test]
    fn client_validation_requires_existing_ca_certificate() {
        let dir = tempdir().expect("tempdir");
        let config = QuicClientConfig {
            bind_addr: "0.0.0.0:0".to_string(),
            remote_addr: "127.0.0.1:4433".parse::<SocketAddr>().expect("socket addr"),
            ca_certificate_path: dir.path().join("missing-ca.pem"),
            server_name: "localhost".to_string(),
            keep_alive: true,
        };

        let error = validate_client_config(&config).expect_err("missing CA should fail");
        assert!(error.to_string().contains("QUIC CA certificate not found"));
    }
}
