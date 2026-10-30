//! A narrow packet proxy to drop an acknowledged write/commit response.
use sqlx::mysql::{MySqlConnectOptions, MySqlSslMode};
use std::{
    io,
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    task::{JoinHandle, JoinSet},
};

pub struct ReplyLoss {
    port: u16,
    pub hits: Arc<AtomicUsize>,
    pub ok_replies: Arc<AtomicUsize>,
    task: JoinHandle<()>,
}
async fn packet<T: AsyncReadExt + Unpin>(socket: &mut T) -> io::Result<Option<Vec<u8>>> {
    let mut header = [0; 4];
    match socket.read_exact(&mut header).await {
        Ok(_) => {}
        Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => return Ok(None),
        Err(e) => return Err(e),
    }
    let len =
        usize::from(header[0]) + (usize::from(header[1]) << 8) + (usize::from(header[2]) << 16);
    let mut bytes = vec![0; len + 4];
    bytes[..4].copy_from_slice(&header);
    socket.read_exact(&mut bytes[4..]).await?;
    Ok(Some(bytes))
}
impl ReplyLoss {
    pub async fn start(options: &MySqlConnectOptions, commit: bool) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        let upstream = (options.get_host().to_owned(), options.get_port());
        let hits = Arc::new(AtomicUsize::new(0));
        let ok_replies = Arc::new(AtomicUsize::new(0));
        let h = hits.clone();
        let ok = ok_replies.clone();
        let task = tokio::spawn(async move {
            let mut tasks = JoinSet::new();
            loop {
                tokio::select! {
                    accepted=listener.accept()=>{let Ok((down,_))=accepted else{break};let address=upstream.clone();let hits=h.clone();let ok=ok.clone();tasks.spawn(async move {
                        let mut up=TcpStream::connect(address).await?;
                        let (mut client_read,mut client_write)=down.into_split();
                        let hello=packet(&mut up).await?.ok_or_else(||io::Error::from(io::ErrorKind::UnexpectedEof))?;
                        client_write.write_all(&hello).await?;
                        let (mut server_read,mut server_write)=up.into_split();let armed=Arc::new(AtomicBool::new(false));let send_armed=armed.clone();
                        let sending=async move {
                            while let Some(bytes)=packet(&mut client_read).await? {
                                if bytes.get(4)==Some(&3) {
                                    let sql=std::str::from_utf8(&bytes[5..]).unwrap_or("");
                                    if (commit && sql=="COMMIT") || (!commit && sql.contains("MYSQL_REGRESSION_LOST_REPLY")) {
                                        hits.fetch_add(1,Ordering::SeqCst);send_armed.store(true,Ordering::SeqCst);
                                    }
                                }
                                server_write.write_all(&bytes).await?;
                            } Ok::<_,io::Error>(())
                        };
                        let receiving=async move {
                            while let Some(bytes)=packet(&mut server_read).await? {
                                if armed.load(Ordering::SeqCst) {
                                    if bytes.get(4)==Some(&0) {ok.fetch_add(1,Ordering::SeqCst);}
                                    client_write.shutdown().await?;return Ok::<_,io::Error>(());
                                }
                                client_write.write_all(&bytes).await?;
                            } Ok(())
                        };
                        tokio::select!{result=sending=>result,result=receiving=>result}
                    });},
                    _=tasks.join_next(),if !tasks.is_empty()=>{}
                }
            }
        });
        Self {
            port,
            hits,
            ok_replies,
            task,
        }
    }
    pub fn options(&self, options: MySqlConnectOptions) -> MySqlConnectOptions {
        options
            .host("127.0.0.1")
            .port(self.port)
            .ssl_mode(MySqlSslMode::Disabled)
    }
}
impl Drop for ReplyLoss {
    fn drop(&mut self) {
        self.task.abort();
    }
}
