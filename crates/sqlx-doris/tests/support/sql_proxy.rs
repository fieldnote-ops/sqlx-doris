//! Packet forwarding to real fixtures with deterministic, test-only faults.
#![allow(dead_code)]
use sqlx_doris::sqlx::mysql::{MySqlConnectOptions, MySqlSslMode};
use std::{
    io,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    sync::Notify,
    task::{JoinHandle, JoinSet},
};

pub const MARKER: &str = "SQLX_DORIS_FAULT";
#[derive(Clone, Copy)]
pub enum Mode {
    HandshakeCut,
    NoTls,
    RowCut,
    LostWriteReply,
    PauseReply,
    PauseBegin,
    PauseCommit,
    LostCommitReply,
    Trace,
    PausePrepare,
    PauseExecute,
    LostExecuteReply,
    PauseRollback,
    LostRollbackReply,
    // One-column COM_QUERY fixtures only: column metadata ends before packet 4.
    PauseRowEnd,
    LostRowEnd,
    PauseRowMetadata,
    PauseSecondRow,
    MoreRowEnd,
    ShortRowEnd,
}

#[derive(Default)]
struct State {
    hits: AtomicUsize,
    auth: AtomicUsize,
    cut_size: AtomicUsize,
    hit: Notify,
    release: Notify,
    trace: Mutex<Vec<Vec<u8>>>,
    fault_reply: AtomicUsize,
    faults_suspended: AtomicBool,
}
pub struct Proxy {
    port: u16,
    state: Arc<State>,
    task: JoinHandle<()>,
}
impl Proxy {
    pub async fn start(upstream: &MySqlConnectOptions, mode: Mode) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        let upstream = (upstream.get_host().to_owned(), upstream.get_port());
        let state = Arc::new(State::default());
        let shared = state.clone();
        let task = tokio::spawn(async move {
            let mut connections = JoinSet::new();
            loop {
                tokio::select! {
                    result=listener.accept() => {
                        let Ok((socket,_))=result else {break};
                        let upstream=upstream.clone();let state=shared.clone();
                        connections.spawn(async move {let _=serve(socket,upstream,mode,state).await;});
                    }
                    _=connections.join_next(), if !connections.is_empty() => {}
                }
            }
        });
        Self { port, state, task }
    }
    pub fn options(&self, base: MySqlConnectOptions) -> MySqlConnectOptions {
        base.host("127.0.0.1")
            .port(self.port)
            .ssl_mode(MySqlSslMode::Disabled)
    }
    pub async fn reached(&self) {
        tokio::time::timeout(std::time::Duration::from_secs(5), self.state.hit.notified())
            .await
            .expect("fault not reached");
    }
    pub fn release(&self) {
        self.state.release.notify_one();
    }
    pub fn hits(&self) -> usize {
        self.state.hits.load(Ordering::SeqCst)
    }
    pub fn auth_packets(&self) -> usize {
        self.state.auth.load(Ordering::SeqCst)
    }
    pub fn cut_size(&self) -> usize {
        self.state.cut_size.load(Ordering::SeqCst)
    }
    pub fn trace(&self) -> Vec<Vec<u8>> {
        self.state.trace.lock().unwrap().clone()
    }
    pub fn fault_reply(&self) -> u8 {
        self.state.fault_reply.load(Ordering::SeqCst) as u8
    }
    pub fn suspend_faults(&self, suspended: bool) {
        self.state
            .faults_suspended
            .store(suspended, Ordering::SeqCst);
    }
}
impl Drop for Proxy {
    fn drop(&mut self) {
        self.task.abort();
    }
}

async fn packet<R: AsyncReadExt + Unpin>(stream: &mut R) -> io::Result<Option<Vec<u8>>> {
    let mut header = [0; 4];
    match stream.read_exact(&mut header).await {
        Ok(_) => {}
        Err(error) if error.kind() == io::ErrorKind::UnexpectedEof => return Ok(None),
        Err(error) => return Err(error),
    }
    let len = u32::from_le_bytes([header[0], header[1], header[2], 0]) as usize;
    let mut bytes = vec![0; len + 4];
    bytes[..4].copy_from_slice(&header);
    stream.read_exact(&mut bytes[4..]).await?;
    Ok(Some(bytes))
}
async fn serve(
    mut down: TcpStream,
    upstream: (String, u16),
    mode: Mode,
    state: Arc<State>,
) -> io::Result<()> {
    let mut up = TcpStream::connect((upstream.0.as_str(), upstream.1)).await?;
    let Some(mut hello) = packet(&mut up).await? else {
        return Ok(());
    };
    if matches!(mode, Mode::HandshakeCut) {
        down.write_all(&hello[..12]).await?;
        down.shutdown().await?;
        return Ok(());
    }
    if matches!(mode, Mode::NoTls) {
        let version_end = hello[5..].iter().position(|b| *b == 0).unwrap() + 5;
        let flags = version_end + 1 + 4 + 8 + 1;
        let value = u16::from_le_bytes([hello[flags], hello[flags + 1]]) & !0x0800;
        hello[flags..flags + 2].copy_from_slice(&value.to_le_bytes());
    }
    down.write_all(&hello).await?;
    let (mut from_client, mut to_client) = down.into_split();
    let (mut from_server, mut to_server) = up.into_split();
    let armed = Arc::new(AtomicBool::new(false));
    let deprecated = Arc::new(AtomicBool::new(false));
    let sending = {
        let armed = armed.clone();
        let deprecated = deprecated.clone();
        let state = state.clone();
        async move {
            let mut auth = true;
            while let Some(bytes) = packet(&mut from_client).await? {
                if auth {
                    state.auth.fetch_add(1, Ordering::SeqCst);
                    deprecated.store(
                        u32::from_le_bytes(bytes[4..8].try_into().unwrap()) & 0x0100_0000 != 0,
                        Ordering::SeqCst,
                    );
                    auth = false;
                } else {
                    if matches!(bytes.get(4), Some(3 | 22 | 23 | 25)) {
                        state.trace.lock().unwrap().push(bytes[4..].to_vec());
                    }
                    let binary_fault = matches!(mode, Mode::PausePrepare)
                        && bytes.get(4) == Some(&22)
                        || matches!(mode, Mode::PauseExecute | Mode::LostExecuteReply)
                            && bytes.get(4) == Some(&23);
                    if binary_fault
                        && !state.faults_suspended.load(Ordering::SeqCst)
                        && state
                            .hits
                            .compare_exchange(0, 1, Ordering::SeqCst, Ordering::SeqCst)
                            .is_ok()
                    {
                        armed.store(true, Ordering::SeqCst);
                    }
                    if bytes.get(4) != Some(&3) {
                        to_server.write_all(&bytes).await?;
                        continue;
                    }
                    let sql = std::str::from_utf8(&bytes[5..]).unwrap_or("");
                    if !state.faults_suspended.load(Ordering::SeqCst)
                        && (sql.contains(MARKER) && !matches!(mode, Mode::Trace)
                            || matches!(mode, Mode::PauseBegin) && sql == "BEGIN"
                            || matches!(mode, Mode::PauseCommit | Mode::LostCommitReply)
                                && sql == "COMMIT"
                            || matches!(mode, Mode::PauseRollback | Mode::LostRollbackReply)
                                && sql == "ROLLBACK")
                    {
                        armed.store(true, Ordering::SeqCst);
                        state.hits.fetch_add(1, Ordering::SeqCst);
                    }
                }
                to_server.write_all(&bytes).await?;
            }
            Ok::<_, io::Error>(())
        }
    };
    let receiving = async move {
        let mut count = 0;
        while let Some(mut bytes) = packet(&mut from_server).await? {
            if armed.load(Ordering::SeqCst) {
                state
                    .fault_reply
                    .store(usize::from(bytes[4]), Ordering::SeqCst);
                count += 1;
                match mode {
                    Mode::PauseRowMetadata if count == 2 => {
                        state.hit.notify_one();
                        state.release.notified().await;
                        armed.store(false, Ordering::SeqCst);
                    }
                    Mode::PauseSecondRow
                        if count
                            == if deprecated.load(Ordering::SeqCst) {
                                4
                            } else {
                                5
                            } =>
                    {
                        state.hit.notify_one();
                        state.release.notified().await;
                        armed.store(false, Ordering::SeqCst);
                    }
                    Mode::ShortRowEnd
                        if count >= 4
                            && bytes[4] == 0xfe
                            && bytes.len() >= 9
                            && bytes.len() - 4 < 9 =>
                    {
                        bytes.truncate(5);
                        bytes[..3].copy_from_slice(&[1, 0, 0]);
                        state.hit.notify_one();
                        armed.store(false, Ordering::SeqCst);
                    }
                    Mode::MoreRowEnd
                        if count >= 4
                            && bytes[4] == 0xfe
                            && bytes.len() >= 9
                            && bytes.len() - 4 < 9 =>
                    {
                        // Synthetic MORE_RESULTS flag on a real result terminator.
                        bytes[7] |= 0x08;
                        state.hit.notify_one();
                        armed.store(false, Ordering::SeqCst);
                    }
                    Mode::PauseRowEnd | Mode::LostRowEnd
                        if count >= 4 && bytes[4] == 0xfe && bytes.len() - 4 < 9 =>
                    {
                        state.hit.notify_one();
                        if matches!(mode, Mode::LostRowEnd) {
                            return Ok(());
                        }
                        state.release.notified().await;
                        armed.store(false, Ordering::SeqCst);
                    }
                    Mode::LostWriteReply
                    | Mode::LostCommitReply
                    | Mode::LostExecuteReply
                    | Mode::LostRollbackReply => {
                        state.hit.notify_one();
                        return Ok(());
                    }
                    Mode::PauseReply
                    | Mode::PauseBegin
                    | Mode::PauseCommit
                    | Mode::PausePrepare
                    | Mode::PauseExecute
                    | Mode::PauseRollback => {
                        state.hit.notify_one();
                        state.release.notified().await;
                        armed.store(false, Ordering::SeqCst);
                    }
                    Mode::RowCut
                        if count
                            == if deprecated.load(Ordering::SeqCst) {
                                3
                            } else {
                                4
                            } =>
                    {
                        state.cut_size.store(bytes.len() - 4, Ordering::SeqCst);
                        state.hit.notify_one();
                        to_client
                            .write_all(&bytes[..12.min(bytes.len() - 1)])
                            .await?;
                        to_client.shutdown().await?;
                        return Ok(());
                    }
                    _ => {}
                }
            }
            to_client.write_all(&bytes).await?;
        }
        Ok::<_, io::Error>(())
    };
    tokio::select! {
        result = sending => result,
        result = receiving => result
    }
}
