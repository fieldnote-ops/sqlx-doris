use super::codec::Cursor;
use super::*;
use bytes::{Bytes, BytesMut};
use sha1::{Digest, Sha1};
use sqlx_core::net::{BufferedSocket, Socket, SocketIntoBox, tls};
use std::borrow::Cow;

pub(crate) struct Wire {
    pub(crate) socket: BufferedSocket<Box<dyn Socket>>,
    pub(crate) sequence: u8,
    limit: usize,
}
impl std::fmt::Debug for Wire {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DorisWire").finish_non_exhaustive()
    }
}
impl Wire {
    pub(crate) async fn send(&mut self, data: &[u8]) -> Result<(), Error> {
        if data.len() > self.limit {
            return Err(protocol("packet exceeds configured maximum"));
        }
        let mut start = 0;
        loop {
            let n = (data.len() - start).min(0xff_ffff);
            let b = (n as u32).to_le_bytes();
            let header = [b[0], b[1], b[2], self.sequence];
            self.socket.write(header.as_slice())?;
            self.socket.write(&data[start..start + n])?;
            self.sequence = self.sequence.wrapping_add(1);
            start += n;
            if n < 0xff_ffff {
                break;
            }
        }
        self.socket.flush().await?;
        Ok(())
    }
    pub(crate) async fn recv(&mut self) -> Result<Bytes, Error> {
        let mut assembled = BytesMut::new();
        loop {
            let h = self.socket.read_buffered(4).await?;
            if h[3] != self.sequence {
                return Err(protocol("packet sequence mismatch"));
            }
            self.sequence = self.sequence.wrapping_add(1);
            let n = usize::from(h[0]) | (usize::from(h[1]) << 8) | (usize::from(h[2]) << 16);
            if assembled.len().saturating_add(n) > self.limit {
                return Err(protocol("packet exceeds configured maximum"));
            }
            let body = self.socket.read_buffered(n).await?;
            if assembled.is_empty() && n < 0xff_ffff {
                return Ok(body.freeze());
            }
            assembled.extend_from_slice(&body);
            if n < 0xff_ffff {
                return Ok(assembled.freeze());
            }
        }
    }
    pub(crate) async fn command(&mut self, command: u8, body: &[u8]) -> Result<(), Error> {
        let mut data = Vec::with_capacity(1 + body.len());
        data.push(command);
        data.extend_from_slice(body);
        self.sequence = 0;
        self.send(&data).await
    }
    pub(crate) fn connect(o: &DorisConnectOptions) -> BoxFuture<'_, Result<Self, Error>> {
        Box::pin(async move {
            let tcp = tokio::net::TcpStream::connect((o.host.as_str(), o.port)).await?;
            tcp.set_nodelay(true)?;
            let socket: Box<dyn Socket> = Box::new(tcp);
            let mut w = Self {
                socket: BufferedSocket::new(socket),
                sequence: 0,
                limit: o.max_packet_size,
            };
            let hello = w.recv().await?;
            if hello.first() == Some(&255) {
                return Err(database_error(&hello)?);
            }
            let mut c = Cursor(&hello);
            if c.byte()? != 10 {
                return Err(protocol("unsupported handshake version"));
            }
            c.nul()?;
            c.u32()?;
            let mut salt = c.take(8)?.to_vec();
            c.byte()?;
            let low = u32::from(c.u16()?);
            c.byte()?;
            c.u16()?;
            let high = u32::from(c.u16()?);
            let capabilities = low | (high << 16);
            let auth_len = usize::from(c.byte()?);
            c.take(10)?;
            let rest = c.take(auth_len.saturating_sub(8).max(13).min(c.0.len()))?;
            salt.extend_from_slice(rest);
            if salt.last() == Some(&0) {
                salt.pop();
            }
            let plugin = if c.0.is_empty() {
                b"mysql_native_password".as_slice()
            } else {
                c.nul()?
            };
            // Protocol41, secure auth, plugin auth, transactions, multiple resultsets.
            // No LOCAL INFILE, multi-statements, compression or deprecated EOF.
            let mut flags = (1 | 512 | 8192 | 32768 | 131072 | 262144 | 524288) & capabilities;
            if o.database.is_some() {
                if capabilities & 8 == 0 {
                    return Err(protocol("FE cannot select database during handshake"));
                }
                flags |= 8;
            }
            let tls_enabled = o.ssl_mode != DorisSslMode::Disabled && capabilities & 2048 != 0;
            if !tls_enabled
                && matches!(
                    o.ssl_mode,
                    DorisSslMode::Required | DorisSslMode::VerifyCa | DorisSslMode::VerifyIdentity
                )
            {
                return Err(Error::Tls("FE does not offer TLS".into()));
            }
            if tls_enabled {
                flags |= 2048;
            }
            let mut response = Vec::with_capacity(128);
            response.extend_from_slice(&flags.to_le_bytes());
            response.extend_from_slice(
                &(o.max_packet_size.min(u32::MAX as usize) as u32).to_le_bytes(),
            );
            response.push(45);
            response.extend_from_slice(&[0; 23]);
            if tls_enabled {
                w.send(&response).await?;
                let socket = upgrade(w.socket.into_inner(), o).await?;
                w.socket = BufferedSocket::new(socket);
            }
            if o.username.contains('\0') || o.database.as_ref().is_some_and(|v| v.contains('\0')) {
                return Err(protocol("NUL in handshake identifier"));
            }
            let token = auth(plugin, o.password.as_deref().unwrap_or(""), &salt)?;
            response.extend_from_slice(o.username.as_bytes());
            response.push(0);
            response.push(token.len() as u8);
            response.extend_from_slice(&token);
            if let Some(db) = &o.database {
                response.extend_from_slice(db.as_bytes());
                response.push(0);
            }
            response.extend_from_slice(b"mysql_native_password\0");
            w.send(&response).await?;
            let mut reply = w.recv().await?;
            if reply.first() == Some(&254) {
                let mut c = Cursor(&reply[1..]);
                let plugin = c.nul()?;
                let salt = c.0.strip_suffix(&[0]).unwrap_or(c.0);
                let token = auth(plugin, o.password.as_deref().unwrap_or(""), salt)?;
                w.send(&token).await?;
                reply = w.recv().await?;
            }
            if reply.first() == Some(&255) {
                return Err(database_error(&reply)?);
            }
            if reply.first() != Some(&0) {
                return Err(protocol("unsupported authentication response"));
            }
            Ok(w)
        })
    }
}
fn auth(plugin: &[u8], password: &str, salt: &[u8]) -> Result<Vec<u8>, Error> {
    if plugin != b"mysql_native_password" {
        return Err(protocol(
            "Doris driver supports mysql_native_password authentication",
        ));
    }
    if password.is_empty() {
        return Ok(vec![]);
    }
    if salt.len() < 20 {
        return Err(protocol("authentication salt is too short"));
    }
    let first = Sha1::digest(password.as_bytes());
    let second = Sha1::digest(first);
    let mut hash = Sha1::new();
    hash.update(&salt[..20]);
    hash.update(second);
    Ok(first
        .iter()
        .zip(hash.finalize())
        .map(|(a, b)| a ^ b)
        .collect())
}
/// A server error with its numeric code and SQLSTATE preserved.
#[derive(Debug, thiserror::Error)]
#[error("{message}")]
pub struct DorisDatabaseError {
    number: u16,
    state: String,
    message: String,
}
impl DorisDatabaseError {
    /// Numeric error code from the FE.
    pub fn number(&self) -> u16 {
        self.number
    }
}
impl sqlx_core::error::DatabaseError for DorisDatabaseError {
    fn message(&self) -> &str {
        &self.message
    }
    fn code(&self) -> Option<Cow<'_, str>> {
        Some(Cow::Borrowed(&self.state))
    }
    fn as_error(&self) -> &(dyn std::error::Error + Send + Sync + 'static) {
        self
    }
    fn as_error_mut(&mut self) -> &mut (dyn std::error::Error + Send + Sync + 'static) {
        self
    }
    fn into_error(self: Box<Self>) -> Box<dyn std::error::Error + Send + Sync + 'static> {
        self
    }
    fn kind(&self) -> sqlx_core::error::ErrorKind {
        sqlx_core::error::ErrorKind::Other
    }
}
pub(crate) fn database_error(packet: &[u8]) -> Result<Error, Error> {
    let mut c = Cursor(packet);
    if c.byte()? != 255 {
        return Err(protocol("expected server error"));
    }
    let number = c.u16()?;
    let state = if c.0.first() == Some(&b'#') {
        c.byte()?;
        String::from_utf8_lossy(c.take(5)?).into_owned()
    } else {
        "HY000".into()
    };
    Ok(Error::Database(Box::new(DorisDatabaseError {
        number,
        state,
        message: String::from_utf8_lossy(c.0).into_owned(),
    })))
}

fn upgrade<S: Socket>(
    socket: S,
    o: &DorisConnectOptions,
) -> BoxFuture<'_, Result<Box<dyn Socket>, Error>> {
    Box::pin(async move {
        let config = tls::TlsConfig {
            accept_invalid_certs: matches!(
                o.ssl_mode,
                DorisSslMode::Preferred | DorisSslMode::Required
            ),
            accept_invalid_hostnames: o.ssl_mode != DorisSslMode::VerifyIdentity,
            hostname: &o.host,
            root_cert_path: o.ca.as_ref(),
            client_cert_path: o.cert.as_ref(),
            client_key_path: o.key.as_ref(),
        };
        tls::handshake(socket, config, SocketIntoBox).await
    })
}
