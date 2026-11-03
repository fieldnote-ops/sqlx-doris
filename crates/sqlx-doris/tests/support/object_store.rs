//! Minimal external-data fixture. Only S3 file/list requests are emulated;
//! Doris job, transaction and cancellation metadata always come from the real FE.
use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpListener,
    sync::Notify,
    task::{JoinHandle, JoinSet},
};

pub struct Store {
    pub endpoint: String,
    released: Arc<AtomicBool>,
    release: Arc<Notify>,
    task: JoinHandle<()>,
}
impl Store {
    pub async fn start() -> Self {
        let host = std::env::var("DORIS_LOAD_FIXTURE_HOST")
            .expect("DORIS_LOAD_FIXTURE_HOST must route from Doris to the test host");
        assert!(
            host.bytes()
                .all(|c| c.is_ascii_alphanumeric() || matches!(c, b'.' | b'-' | b':'))
        );
        let listener = TcpListener::bind(("0.0.0.0", 0)).await.unwrap();
        let port = listener.local_addr().unwrap().port();
        let released = Arc::new(AtomicBool::new(false));
        let release = Arc::new(Notify::new());
        let flag = released.clone();
        let event = release.clone();
        let task = tokio::spawn(async move {
            let mut requests = JoinSet::new();
            loop {
                let (mut socket, _) = listener.accept().await.unwrap();
                let released = flag.clone();
                let release = event.clone();
                requests.spawn(async move {
                    let mut request = Vec::new();
                    while !request.ends_with(b"\r\n\r\n") {
                        let b = socket.read_u8().await?;
                        request.push(b);
                        if request.len() > 32768 { return Ok::<_,std::io::Error>(()); }
                    }
                    let request = String::from_utf8_lossy(&request);
                    let mut parts = request.split_whitespace();
                    let method = parts.next().unwrap();
                    let path = parts.next().unwrap();
                    let data = if method == "HEAD" { Vec::new() }
                    else if path.contains('?') {
                        b"<ListBucketResult xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\"><Name>fixture</Name><Prefix></Prefix><KeyCount>1</KeyCount><MaxKeys>1000</MaxKeys><IsTruncated>false</IsTruncated><Contents><Key>probe.csv</Key><LastModified>2026-10-31T00:00:00.000Z</LastModified><ETag>\"12345678123456781234567812345678\"</ETag><Size>4</Size><StorageClass>STANDARD</StorageClass></Contents></ListBucketResult>".to_vec()
                    } else {
                        let notified = release.notified();
                        if !released.load(Ordering::SeqCst) { notified.await; }
                        b"1,2\n".to_vec()
                    };
                    let len = if method == "HEAD" { 4 } else { data.len() };
                    socket.write_all(format!("HTTP/1.1 200 OK\r\nContent-Length: {len}\r\nETag: \"12345678123456781234567812345678\"\r\nConnection: close\r\n\r\n").as_bytes()).await?;
                    socket.write_all(&data).await?;
                    Ok(())
                });
                while requests.try_join_next().is_some() {}
            }
        });
        Self {
            endpoint: format!("http://{host}:{port}"),
            released,
            release,
            task,
        }
    }
    pub fn release(&self) {
        self.released.store(true, Ordering::SeqCst);
        self.release.notify_waiters();
    }
}
impl Drop for Store {
    fn drop(&mut self) {
        self.task.abort();
    }
}
