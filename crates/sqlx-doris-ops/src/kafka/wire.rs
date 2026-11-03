//! Bounded codecs for a fixed read-only Kafka API allowlist. No records/group APIs.
use super::{Config, Result, fault, model::*};
use std::collections::BTreeMap;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::net::TcpStream;

pub(super) const FRAME_MAX: usize = 1_048_576;
trait Socket: AsyncRead + AsyncWrite + Unpin + Send {}
impl<T: AsyncRead + AsyncWrite + Unpin + Send> Socket for T {}
pub(super) struct Wire {
    io: Box<dyn Socket>,
    correlation: i32,
}
pub(super) fn endpoint(value: &str) -> Result<(&str, u16)> {
    let (host, port) = value
        .rsplit_once(':')
        .ok_or_else(|| fault(2, "Kafka endpoint must be host:port"))?;
    let host = host
        .strip_prefix('[')
        .and_then(|v| v.strip_suffix(']'))
        .unwrap_or(host);
    if host.is_empty()
        || host.len() > 253
        || host
            .bytes()
            .any(|b| b.is_ascii_control() || b.is_ascii_whitespace() || b"/@\\".contains(&b))
    {
        return Err(fault(2, "invalid Kafka endpoint"));
    }
    let port = port
        .parse::<u16>()
        .ok()
        .filter(|v| *v > 0)
        .ok_or_else(|| fault(2, "invalid Kafka endpoint port"))?;
    Ok((host, port))
}
#[derive(Default)]
pub(super) struct Writer(pub Vec<u8>);
impl Writer {
    pub fn i8(&mut self, v: i8) {
        self.0.push(v as u8);
    }
    pub fn i16(&mut self, v: i16) {
        self.0.extend(v.to_be_bytes());
    }
    pub fn i32(&mut self, v: i32) {
        self.0.extend(v.to_be_bytes());
    }
    pub fn i64(&mut self, v: i64) {
        self.0.extend(v.to_be_bytes());
    }
    pub fn string(&mut self, s: &str) {
        self.i16(s.len() as i16);
        self.0.extend(s.as_bytes());
    }
    pub fn var(&mut self, mut n: u32) {
        while n >= 128 {
            self.0.push((n as u8) | 128);
            n >>= 7;
        }
        self.0.push(n as u8);
    }
    pub fn compact(&mut self, s: &str) {
        self.var(s.len() as u32 + 1);
        self.0.extend(s.as_bytes());
    }
}
pub(super) struct Reader<'a> {
    data: &'a [u8],
    pos: usize,
}
impl<'a> Reader<'a> {
    pub fn new(data: &'a [u8]) -> Self {
        Self { data, pos: 0 }
    }
    pub fn bytes(&mut self, n: usize) -> Result<&'a [u8]> {
        let end = self
            .pos
            .checked_add(n)
            .filter(|end| *end <= self.data.len())
            .ok_or_else(|| fault(7, "truncated Kafka response"))?;
        let value = &self.data[self.pos..end];
        self.pos = end;
        Ok(value)
    }
    pub fn i8(&mut self) -> Result<i8> {
        Ok(self.bytes(1)?[0] as i8)
    }
    pub fn i16(&mut self) -> Result<i16> {
        Ok(i16::from_be_bytes(self.bytes(2)?.try_into()?))
    }
    pub fn i32(&mut self) -> Result<i32> {
        Ok(i32::from_be_bytes(self.bytes(4)?.try_into()?))
    }
    pub fn i64(&mut self) -> Result<i64> {
        Ok(i64::from_be_bytes(self.bytes(8)?.try_into()?))
    }
    pub fn var(&mut self) -> Result<u32> {
        let mut n = 0;
        for shift in [0, 7, 14, 21, 28] {
            let b = self.i8()? as u8;
            if shift == 28 && b > 15 {
                return Err(fault(7, "invalid Kafka varint"));
            }
            n |= u32::from(b & 127) << shift;
            if b < 128 {
                return Ok(n);
            }
        }
        Err(fault(7, "invalid Kafka varint"))
    }
    pub fn count(&mut self, compact: bool, max: usize) -> Result<usize> {
        let n = if compact {
            self.var()?
                .checked_sub(1)
                .ok_or_else(|| fault(7, "unexpected null Kafka array"))? as i64
        } else {
            i64::from(self.i32()?)
        };
        if n < 0 || n as usize > max {
            return Err(fault(7, "Kafka array exceeds observation limit"));
        }
        Ok(n as usize)
    }
    pub fn string(&mut self, compact: bool) -> Result<Option<String>> {
        let n = if compact {
            i64::from(self.var()?) - 1
        } else {
            i64::from(self.i16()?)
        };
        if n == -1 {
            return Ok(None);
        }
        if !(0..=4096).contains(&n) {
            return Err(fault(7, "invalid Kafka string length"));
        }
        Ok(Some(
            std::str::from_utf8(self.bytes(n as usize)?)
                .map_err(|_| fault(7, "invalid Kafka UTF-8"))?
                .into(),
        ))
    }
    pub fn required(&mut self, compact: bool) -> Result<String> {
        self.string(compact)?
            .ok_or_else(|| fault(7, "required Kafka string missing"))
    }
    pub fn tags(&mut self) -> Result<()> {
        // Unknown tags could change the evidence contract; never guess.
        if self.var()? != 0 {
            return Err(fault(7, "unsupported Kafka tagged metadata"));
        }
        Ok(())
    }
    pub fn done(&self) -> Result<()> {
        if self.pos == self.data.len() {
            Ok(())
        } else {
            Err(fault(7, "unexpected Kafka response trailing bytes"))
        }
    }
}
impl Wire {
    pub async fn connect(config: &Config, address: &str) -> Result<Self> {
        let (host, port) = endpoint(address)?;
        let tcp = TcpStream::connect((host, port))
            .await
            .map_err(|_| super::connect_fault())?;
        tcp.set_nodelay(true)
            .map_err(|_| fault(7, "Kafka connection configuration failed"))?;
        let io: Box<dyn Socket> = if let Some(tls) = &config.tls {
            let name = rustls::pki_types::ServerName::try_from(host.to_owned())
                .map_err(|_| fault(2, "invalid Kafka TLS server name"))?;
            Box::new(
                tokio_rustls::TlsConnector::from(tls.clone())
                    .connect(name, tcp)
                    .await
                    .map_err(|_| fault(7, "Kafka TLS verification/handshake failed"))?,
            )
        } else {
            Box::new(tcp)
        };
        let mut wire = Self { io, correlation: 0 };
        let data = wire.call(18, 0, Writer::default()).await?;
        let mut r = Reader::new(&data);
        if r.i16()? != 0 {
            return Err(fault(7, "Kafka ApiVersions rejected"));
        }
        let mut versions = BTreeMap::new();
        for _ in 0..r.count(false, 256)? {
            let key = r.i16()?;
            let min = r.i16()?;
            let max = r.i16()?;
            if min > max || versions.insert(key, (min, max)).is_some() {
                return Err(fault(7, "ambiguous Kafka API versions"));
            }
        }
        r.done()?;
        for (key, version) in [(3, 12), (2, 5), (23, 3)] {
            if !versions
                .get(&key)
                .is_some_and(|(min, max)| *min <= version && version <= *max)
            {
                return Err(fault(7, "required Kafka read API version unavailable"));
            }
        }
        if let Some(auth) = &config.sasl {
            for key in [17, 36] {
                if !versions
                    .get(&key)
                    .is_some_and(|(min, max)| *min <= 1 && *max >= 1)
                {
                    return Err(fault(7, "Kafka SASL API unavailable"));
                }
            }
            let mut b = Writer::default();
            b.string("PLAIN");
            let data = wire.call(17, 1, b).await?;
            let mut r = Reader::new(&data);
            if r.i16()? != 0 {
                return Err(fault(7, "Kafka SASL mechanism rejected"));
            }
            let mut plain = false;
            for _ in 0..r.count(false, 64)? {
                plain |= r.required(false)? == "PLAIN";
            }
            r.done()?;
            if !plain {
                return Err(fault(7, "Kafka SASL PLAIN unavailable"));
            }
            let mut b = Writer::default();
            b.i32((auth.username.len() + auth.password.len() + 2) as i32);
            b.i8(0);
            b.0.extend(auth.username.as_bytes());
            b.i8(0);
            b.0.extend(auth.password.as_bytes());
            let data = wire.call(36, 1, b).await?;
            let mut r = Reader::new(&data);
            let code = r.i16()?;
            let _message = r.string(false)?;
            let len = r.count(false, 16384)?;
            r.bytes(len)?;
            r.i64()?;
            r.done()?;
            if code != 0 {
                return Err(fault(7, "Kafka SASL authentication failed"));
            }
        }
        Ok(wire)
    }
    pub async fn call(&mut self, key: i16, version: i16, body: Writer) -> Result<Vec<u8>> {
        if !matches!(
            (key, version),
            (18, 0) | (3, 12) | (2, 5) | (23, 3) | (17, 1) | (36, 1)
        ) {
            return Err(fault(7, "non-read-only Kafka API refused"));
        }
        self.correlation = self
            .correlation
            .checked_add(1)
            .ok_or_else(|| fault(7, "Kafka correlation exhausted"))?;
        let mut b = Writer::default();
        b.i16(key);
        b.i16(version);
        b.i32(self.correlation);
        b.string("sqlx-doris-readonly");
        if key == 3 {
            b.var(0);
        }
        b.0.extend(body.0);
        if b.0.len() > FRAME_MAX {
            return Err(fault(2, "Kafka request exceeds limit"));
        }
        self.io
            .write_i32(b.0.len() as i32)
            .await
            .map_err(|_| fault(7, "Kafka read request failed"))?;
        self.io
            .write_all(&b.0)
            .await
            .map_err(|_| fault(7, "Kafka read request failed"))?;
        let len = self
            .io
            .read_i32()
            .await
            .map_err(|_| fault(7, "Kafka response unavailable"))?;
        if len < 4 || len as usize > FRAME_MAX {
            return Err(fault(7, "Kafka response exceeds frame limit"));
        }
        let mut response = vec![0; len as usize];
        self.io
            .read_exact(&mut response)
            .await
            .map_err(|_| fault(7, "Kafka response disconnected"))?;
        let mut r = Reader::new(&response);
        if r.i32()? != self.correlation {
            return Err(fault(7, "Kafka response correlation mismatch"));
        }
        if key == 3 {
            r.tags()?;
        }
        Ok(response[r.pos..].to_vec())
    }
    pub async fn metadata(&mut self, topic: &str) -> Result<Metadata> {
        let mut b = Writer::default();
        b.var(2);
        b.0.extend([0; 16]);
        b.compact(topic);
        b.var(0);
        b.i8(0); // allow_auto_topic_creation=false
        b.i8(0);
        b.var(0); // include_topic_authorized_operations=false, tags
        let data = self.call(3, 12, b).await?;
        decode_metadata(&data, topic)
    }
    pub async fn offsets(
        &mut self,
        topic: &str,
        parts: &[(i32, i32)],
        time: i64,
    ) -> Result<BTreeMap<i32, i64>> {
        let mut b = Writer::default();
        b.i32(-1);
        b.i8(0);
        b.i32(1);
        b.string(topic);
        b.i32(parts.len() as i32);
        for (p, e) in parts {
            b.i32(*p);
            b.i32(*e);
            b.i64(time);
        }
        let data = self.call(2, 5, b).await?;
        let mut r = Reader::new(&data);
        r.i32()?;
        if r.count(false, 1)? != 1 || r.required(false)? != topic {
            return Err(fault(7, "Kafka offsets topic mismatch"));
        }
        let mut out = BTreeMap::new();
        for _ in 0..r.count(false, MAX_PARTITIONS)? {
            let p = r.i32()?;
            let error = r.i16()?;
            r.i64()?;
            let offset = r.i64()?;
            let epoch = r.i32()?;
            if error != 0
                || offset < 0
                || !parts
                    .iter()
                    .any(|(id, e)| *id == p && epoch >= -1 && epoch <= *e)
                || out.insert(p, offset).is_some()
            {
                return Err(fault(7, "Kafka range metadata unavailable or fenced"));
            }
        }
        r.done()?;
        if out.len() != parts.len() {
            return Err(fault(7, "Kafka offset partitions missing"));
        }
        Ok(out)
    }
    pub async fn epochs(
        &mut self,
        topic: &str,
        parts: &[(i32, i32)],
        baseline: &BTreeMap<i32, i32>,
    ) -> Result<BTreeMap<i32, Epoch>> {
        let mut b = Writer::default();
        b.i32(-1);
        b.i32(1);
        b.string(topic);
        b.i32(parts.len() as i32);
        for (p, e) in parts {
            b.i32(*p);
            b.i32(*e);
            b.i32(*baseline.get(p).unwrap_or(e));
        }
        let data = self.call(23, 3, b).await?;
        let mut r = Reader::new(&data);
        r.i32()?;
        if r.count(false, 1)? != 1 || r.required(false)? != topic {
            return Err(fault(7, "Kafka epoch topic mismatch"));
        }
        let mut out = BTreeMap::new();
        for _ in 0..r.count(false, MAX_PARTITIONS)? {
            let error = r.i16()?;
            let p = r.i32()?;
            let epoch = r.i32()?;
            let end = r.i64()?;
            if !parts.iter().any(|(id, _)| *id == p)
                || out.insert(p, Epoch { error, epoch, end }).is_some()
            {
                return Err(fault(7, "Kafka epoch partitions ambiguous"));
            }
        }
        r.done()?;
        if out.len() != parts.len() {
            return Err(fault(7, "Kafka epoch partitions missing"));
        }
        Ok(out)
    }
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) struct Metadata {
    pub cluster: String,
    pub topic: String,
    pub topic_id: String,
    pub leaders: BTreeMap<i32, (i32, i32)>,
    pub brokers: BTreeMap<i32, String>,
}
fn decode_metadata(data: &[u8], topic: &str) -> Result<Metadata> {
    let mut r = Reader::new(data);
    r.i32()?;
    let mut brokers = BTreeMap::new();
    for _ in 0..r.count(true, 256)? {
        let id = r.i32()?;
        let host = r.required(true)?;
        let port = r.i32()?;
        r.string(true)?;
        r.tags()?;
        let address = if host.contains(':') {
            format!("[{host}]:{port}")
        } else {
            format!("{host}:{port}")
        };
        endpoint(&address).map_err(|_| fault(7, "invalid advertised Kafka broker"))?;
        if id < 0 || brokers.insert(id, address).is_some() {
            return Err(fault(7, "ambiguous Kafka brokers"));
        }
    }
    let cluster = r.required(true)?;
    r.i32()?;
    if r.count(true, 1)? != 1 {
        return Err(fault(7, "unexpected Kafka topics"));
    }
    let error = r.i16()?;
    let name = r.required(true)?;
    let topic_id = uuid::Uuid::from_slice(r.bytes(16)?)?.to_string();
    if error != 0 || name != topic || r.i8()? != 0 {
        return Err(fault(7, "Kafka topic absent/unauthorized/internal"));
    }
    let mut leaders = BTreeMap::new();
    for _ in 0..r.count(true, MAX_PARTITIONS)? {
        let error = r.i16()?;
        let p = r.i32()?;
        let leader = r.i32()?;
        let epoch = r.i32()?;
        for _ in 0..3 {
            for _ in 0..r.count(true, 256)? {
                r.i32()?;
            }
        }
        r.tags()?;
        if error != 0
            || p < 0
            || epoch < 0
            || !brokers.contains_key(&leader)
            || leaders.insert(p, (leader, epoch)).is_some()
        {
            return Err(fault(7, "Kafka partition leader unavailable/ambiguous"));
        }
    }
    r.i32()?;
    r.tags()?;
    r.tags()?;
    r.done()?;
    if leaders.is_empty()
        || leaders.keys().copied().ne(0..leaders.len() as i32)
        || cluster.is_empty()
        || cluster.len() > 256
        || !cluster
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b"_-".contains(&b))
        || topic_id == uuid::Uuid::nil().to_string()
    {
        return Err(fault(7, "unknown Kafka identity/partition metadata"));
    }
    Ok(Metadata {
        cluster,
        topic: name,
        topic_id,
        leaders,
        brokers,
    })
}
