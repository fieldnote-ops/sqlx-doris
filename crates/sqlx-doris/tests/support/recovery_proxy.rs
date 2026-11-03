//! Deterministic packet barriers in front of a real Doris FE. No SQL is
//! emulated: the tests observe whether the actual server applied DROP.
use sqlx_doris::{DorisConnectOptions, DorisSslMode};
use std::{
    io,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};
use tokio::{
    io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    sync::{Notify, oneshot},
    task::JoinHandle,
};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Point {
    BeforeDrop,
    DropReply,
    RunnerReply,
    BeforeRunner,
    GateRead(usize),
    MainRead(usize),
    #[cfg(feature = "cli")]
    IdentityRead(usize),
    #[cfg(feature = "cli")]
    DrainRead,
    #[cfg(feature = "cli")]
    TransactionRead,
    #[cfg(feature = "cli")]
    MvTasks(usize),
    #[cfg(feature = "cli")]
    LoadRead(usize),
    #[cfg(feature = "cli")]
    RoutineRead(usize),
    #[cfg(feature = "cli")]
    RoutineHidden,
    Observe,
}
#[derive(Clone, Copy)]
pub enum Release {
    Forward,
    Disconnect,
}
#[derive(Clone, Debug, Default)]
pub struct Trace {
    pub commands: usize,
    pub drop_requests: usize,
    pub drop_forwarded: usize,
    pub drop_ok: usize,
    pub drop_replies: usize,
    pub parent_drops: usize,
}
pub struct Proxy {
    pub options: DorisConnectOptions,
    pub trace: Arc<Mutex<Trace>>,
    reached: oneshot::Receiver<()>,
    release: Option<oneshot::Sender<Release>>,
    ack: Arc<Notify>,
    task: Option<JoinHandle<io::Result<()>>>,
}
#[derive(Clone, Copy)]
enum Target {
    Transition,
    Main,
    #[cfg(feature = "cli")]
    Identity,
    #[cfg(feature = "cli")]
    Rollup,
    #[cfg(feature = "cli")]
    Mv,
    #[cfg(feature = "cli")]
    Routine,
    #[cfg(feature = "cli")]
    RoutineCreate,
}
async fn packet(reader: &mut (impl AsyncRead + Unpin)) -> io::Result<Vec<u8>> {
    let mut header = [0_u8; 4];
    reader.read_exact(&mut header).await?;
    let size = u32::from_le_bytes([header[0], header[1], header[2], 0]) as usize;
    if size == 0 || size > 1_048_576 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "unexpected fixture packet size",
        ));
    }
    let mut value = header.to_vec();
    value.resize(size + 4, 0);
    reader.read_exact(&mut value[4..]).await?;
    Ok(value)
}
async fn copy_packet(
    reader: &mut (impl AsyncRead + Unpin),
    writer: &mut (impl AsyncWrite + Unpin),
) -> io::Result<Vec<u8>> {
    let value = packet(reader).await?;
    writer.write_all(&value).await?;
    Ok(value)
}
fn contains(packet: &[u8], value: &[u8]) -> bool {
    packet.windows(value.len()).any(|part| part == value)
}
impl Proxy {
    pub async fn start(options: &DorisConnectOptions, point: Point) -> Self {
        Self::start_target(options, point, Target::Transition).await
    }
    pub async fn start_main(options: &DorisConnectOptions, point: Point) -> Self {
        Self::start_target(options, point, Target::Main).await
    }
    #[cfg(feature = "cli")]
    pub async fn start_identity(options: &DorisConnectOptions, point: Point) -> Self {
        Self::start_target(options, point, Target::Identity).await
    }
    #[cfg(feature = "cli")]
    pub async fn start_rollup(options: &DorisConnectOptions, point: Point) -> Self {
        Self::start_target(options, point, Target::Rollup).await
    }
    #[cfg(feature = "cli")]
    pub async fn start_mv(options: &DorisConnectOptions, point: Point) -> Self {
        Self::start_target(options, point, Target::Mv).await
    }
    #[cfg(feature = "cli")]
    pub async fn start_routine(options: &DorisConnectOptions, point: Point) -> Self {
        Self::start_target(options, point, Target::Routine).await
    }
    #[cfg(feature = "cli")]
    pub async fn start_routine_create(options: &DorisConnectOptions, point: Point) -> Self {
        Self::start_target(options, point, Target::RoutineCreate).await
    }
    async fn start_target(options: &DorisConnectOptions, point: Point, target: Target) -> Self {
        let listener = TcpListener::bind(("127.0.0.1", 0)).await.unwrap();
        let port = listener.local_addr().unwrap().port();
        let upstream = (options.get_host().to_owned(), options.get_port());
        let trace = Arc::new(Mutex::new(Trace::default()));
        let trace_task = trace.clone();
        let ack = Arc::new(Notify::new());
        let ack_task = ack.clone();
        let (reached_tx, reached) = oneshot::channel();
        let (release, release_rx) = oneshot::channel();
        let task = tokio::spawn(async move {
            // Managed creation uses an independent administrator connection
            // followed by the dedicated runner on the same verified endpoint.
            #[cfg(feature = "cli")]
            let mut bridges = tokio::task::JoinSet::new();
            #[cfg(feature = "cli")]
            if matches!(target, Target::RoutineCreate) {
                let (mut admin, _) = listener.accept().await?;
                let mut backend = TcpStream::connect(upstream.clone()).await?;
                bridges.spawn(async move {
                    tokio::io::copy_bidirectional(&mut admin, &mut backend).await
                });
            }
            let (mut client, _) = listener.accept().await?;
            let mut server = TcpStream::connect(upstream).await?;
            client.set_nodelay(true)?;
            server.set_nodelay(true)?;
            copy_packet(&mut server, &mut client).await?; // greeting
            copy_packet(&mut client, &mut server).await?; // native authentication, TLS disabled
            loop {
                let response = copy_packet(&mut server, &mut client).await?;
                match response[4] {
                    0 => break,
                    254 => {
                        copy_packet(&mut client, &mut server).await?;
                    }
                    _ => {
                        return Err(io::Error::new(
                            io::ErrorKind::InvalidData,
                            "fixture authentication failed",
                        ));
                    }
                }
            }
            let (mut cr, mut cw) = client.into_split();
            let (mut sr, mut sw) = server.into_split();
            let pending_drop = Arc::new(AtomicBool::new(false));
            let pending_response = pending_drop.clone();
            let pending_runner = Arc::new(AtomicBool::new(false));
            let runner_response = pending_runner.clone();
            let response_done = Arc::new(Notify::new());
            let response_received = response_done.clone();
            // Exactly one direction owns the selected barrier.
            let (client_barrier, server_barrier) =
                if matches!(point, Point::DropReply | Point::RunnerReply) {
                    (None, Some((reached_tx, release_rx)))
                } else {
                    (Some((reached_tx, release_rx)), None)
                };
            let trace_read = trace_task.clone();
            let client_loop = async move {
                let mut barrier = client_barrier;
                let (mut gates, mut mains) = (0, 0);
                #[cfg(feature = "cli")]
                let (mut identities, mut mv_tasks, mut loads, mut routines) = (0, 0, 0, 0);
                loop {
                    let value = match packet(&mut cr).await {
                        Ok(value) => value,
                        Err(e) => {
                            // Model a network intermediary retaining a queued
                            // command after its client has exited. Keep the
                            // backend alive until Doris replies to that DROP.
                            if pending_drop.load(Ordering::SeqCst) {
                                response_done.notified().await;
                            }
                            return Err(e);
                        }
                    };
                    trace_task.lock().unwrap().commands += 1;
                    let payload = &value[4..];
                    let recovery_object = contains(payload, b"_sqlx_migrations_gate")
                        || contains(payload, b"_sqlx_migrations_lock");
                    let parent_drop = payload[0] == 3
                        && payload[1..].starts_with(b"DROP TABLE ")
                        && recovery_object;
                    if parent_drop {
                        trace_task.lock().unwrap().parent_drops += 1;
                    }
                    let drop = match target {
                        #[cfg(feature = "cli")]
                        Target::RoutineCreate => {
                            payload[0] == 3 && payload[1..].starts_with(b"CREATE ROUTINE LOAD ")
                        }
                        #[cfg(feature = "cli")]
                        Target::Routine => {
                            payload[0] == 3 && payload[1..].starts_with(b"STOP ROUTINE LOAD FOR ")
                        }
                        #[cfg(feature = "cli")]
                        Target::Mv => {
                            payload[0] == 3
                                && payload[1..]
                                    .starts_with(b"CANCEL TASK WHERE jobName='inner_mtmv_")
                        }
                        #[cfg(feature = "cli")]
                        Target::Rollup => {
                            payload[0] == 3
                                && payload[1..].starts_with(b"CANCEL ALTER TABLE ROLLUP ")
                        }
                        #[cfg(feature = "cli")]
                        Target::Identity => {
                            payload[0] == 3 && payload[1..].starts_with(b"DROP USER 'sqlx_mig_")
                        }
                        Target::Main | Target::Transition => {
                            contains(
                                payload,
                                if matches!(target, Target::Main) {
                                    b"_sqlx_migrations_lock"
                                } else {
                                    b"_sqlx_migrations_gate"
                                },
                            ) && (parent_drop
                                || payload[0] == 3
                                    && payload[1..].starts_with(b"ALTER TABLE ")
                                    && contains(payload, b" DROP PARTITION "))
                        }
                    };
                    let hit = if drop {
                        let mut trace = trace_task.lock().unwrap();
                        trace.drop_requests += 1;
                        Point::BeforeDrop
                    } else if payload[0] == 3 && contains(payload, b"/*runner_checkpoint*/") {
                        Point::BeforeRunner
                    } else if payload[0] == 23 && contains(payload, b"_sqlx_migrations_gate") {
                        gates += 1;
                        Point::GateRead(gates)
                    } else if payload[0] == 23 && contains(payload, b"_sqlx_migrations_lock") {
                        mains += 1;
                        Point::MainRead(mains)
                    } else {
                        Point::Observe
                    };
                    #[cfg(feature = "cli")]
                    let hit = if payload[0] == 3
                        && (payload[1..].starts_with(b"SHOW ALL ROUTINE LOAD FOR ")
                            || payload[1..] == *b"SHOW ALL ROUTINE LOAD")
                    {
                        routines += 1;
                        Point::RoutineRead(routines)
                    } else if payload[0] == 3 && payload[1..].starts_with(b"SHOW LOAD FROM ") {
                        loads += 1;
                        Point::LoadRead(loads)
                    } else if payload[0] == 3 && payload[1..] == *b"SHOW ALL GRANTS" {
                        identities += 1;
                        Point::IdentityRead(identities)
                    } else if payload[0] == 3 && contains(payload, b"FROM tasks('type'='mv')") {
                        mv_tasks += 1;
                        Point::MvTasks(mv_tasks)
                    } else if payload[0] == 3 && payload[1..].starts_with(b"SHOW TRANSACTION FROM ")
                    {
                        Point::TransactionRead
                    } else if payload[0] == 3 && contains(payload, b"/transactions/") {
                        Point::DrainRead
                    } else {
                        hit
                    };
                    if hit == point
                        && point != Point::Observe
                        && let Some((reached, release)) = barrier.take()
                    {
                        let _ = reached.send(());
                        if !matches!(release.await, Ok(Release::Forward)) {
                            return Ok(());
                        }
                    }
                    if drop {
                        pending_drop.store(true, Ordering::SeqCst);
                        trace_task.lock().unwrap().drop_forwarded += 1;
                    }
                    if point == Point::RunnerReply
                        && payload[0] == 3
                        && contains(payload, b"/*runner_checkpoint*/")
                    {
                        pending_runner.store(true, Ordering::SeqCst);
                    }
                    #[cfg(feature = "cli")]
                    let value = if point == Point::RoutineHidden
                        && payload[0] == 3
                        && payload[1..] == *b"SHOW ALL ROUTINE LOAD"
                    {
                        // Ask the real server for a filtered empty history. This
                        // injects lost visibility without emulating Doris rows.
                        let sql = b"SHOW ALL ROUTINE LOAD LIKE 'sqlx_intentionally_hidden_history'";
                        let length = sql.len() + 1;
                        let mut packet = vec![
                            (length & 255) as u8,
                            ((length >> 8) & 255) as u8,
                            ((length >> 16) & 255) as u8,
                            value[3],
                            3,
                        ];
                        packet.extend_from_slice(sql);
                        packet
                    } else {
                        value
                    };
                    sw.write_all(&value).await?;
                }
            };
            let server_loop = async move {
                let mut barrier = server_barrier;
                loop {
                    let value = packet(&mut sr).await?;
                    if runner_response.swap(false, Ordering::SeqCst) {
                        assert_eq!(value[4], 0, "checkpoint INSERT must have committed");
                        if let Some((reached, release)) = barrier.take() {
                            let _ = reached.send(());
                            if !matches!(release.await, Ok(Release::Forward)) {
                                return Ok(());
                            }
                        }
                    }
                    if pending_response.swap(false, Ordering::SeqCst) {
                        trace_read.lock().unwrap().drop_replies += 1;
                        if value[4] == 0 {
                            trace_read.lock().unwrap().drop_ok += 1;
                        }
                        ack_task.notify_one();
                        response_received.notify_one();
                        if let Some((reached, release)) = barrier.take() {
                            let _ = reached.send(());
                            if !matches!(release.await, Ok(Release::Forward)) {
                                return Ok(());
                            }
                        }
                    }
                    cw.write_all(&value).await?;
                }
            };
            tokio::select! { result = client_loop => result, result = server_loop => result }
        });
        Self {
            options: options
                .clone()
                .host("127.0.0.1")
                .port(port)
                .ssl_mode(DorisSslMode::Disabled),
            trace,
            reached,
            release: Some(release),
            ack,
            task: Some(task),
        }
    }
    pub async fn reached(&mut self) {
        tokio::time::timeout(Duration::from_secs(10), &mut self.reached)
            .await
            .unwrap()
            .unwrap();
    }
    pub fn release(&mut self, action: Release) {
        self.release
            .take()
            .unwrap()
            .send(action)
            .unwrap_or_else(|_| panic!("proxy exited before barrier release"));
    }
    pub async fn acknowledged(&self) {
        self.responded().await;
        assert_eq!(self.trace.lock().unwrap().drop_ok, 1);
    }
    pub async fn responded(&self) {
        tokio::time::timeout(Duration::from_secs(10), self.ack.notified())
            .await
            .unwrap();
        assert_eq!(self.trace.lock().unwrap().drop_replies, 1);
    }
    pub async fn finish(mut self) {
        if let Some(release) = self.release.take() {
            let _ = release.send(Release::Disconnect);
        }
        let result = tokio::time::timeout(Duration::from_secs(10), self.task.take().unwrap())
            .await
            .unwrap()
            .unwrap();
        if let Err(e) = result {
            assert!(
                matches!(
                    e.kind(),
                    io::ErrorKind::UnexpectedEof
                        | io::ErrorKind::ConnectionReset
                        | io::ErrorKind::ConnectionAborted
                        | io::ErrorKind::BrokenPipe
                        | io::ErrorKind::NotConnected
                ),
                "{e}"
            );
        }
    }
}
impl Drop for Proxy {
    fn drop(&mut self) {
        if let Some(task) = &self.task {
            task.abort();
        }
    }
}
