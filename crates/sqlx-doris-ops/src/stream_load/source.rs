//! Bounded file reads pinned to one opened file, with an independent cursor per body.
use bytes::Bytes;
use reqwest::Body;
use std::{
    fs::File,
    io::{self, Read, Seek, SeekFrom},
    path::{Path, PathBuf},
    sync::{Arc, Mutex},
    time::SystemTime,
};

pub(super) enum Source {
    Bytes(Bytes),
    File {
        path: PathBuf,
        opened: Option<FileSource>,
    },
}

impl Source {
    pub fn file(path: &Path) -> Self {
        Self::File {
            path: path.to_owned(),
            opened: None,
        }
    }

    pub async fn body(&mut self) -> io::Result<(Body, u64)> {
        match self {
            Self::Bytes(bytes) => Ok((Body::from(bytes.clone()), bytes.len() as u64)),
            Self::File { path, opened } => {
                if opened.is_none() {
                    let path = path.clone();
                    *opened = Some(
                        tokio::task::spawn_blocking(move || FileSource::open(&path))
                            .await
                            .map_err(io::Error::other)??,
                    );
                }
                let source = opened.as_ref().expect("opened above");
                let reader = source.clone();
                tokio::task::spawn_blocking(move || reader.check())
                    .await
                    .map_err(io::Error::other)??;
                let stream = futures_util::stream::try_unfold(
                    (source.clone(), 0_u64),
                    |(source, offset)| async move {
                        let reader = source.clone();
                        let chunk = tokio::task::spawn_blocking(move || reader.read_chunk(offset))
                            .await
                            .map_err(io::Error::other)??;
                        Ok::<_, io::Error>(chunk.map(|bytes| {
                            let next = offset + bytes.len() as u64;
                            (bytes, (source, next))
                        }))
                    },
                );
                Ok((Body::wrap_stream(stream), source.length))
            }
        }
    }
}

#[derive(Clone)]
pub(super) struct FileSource {
    file: Arc<Mutex<File>>,
    length: u64,
    modified: SystemTime,
}

impl FileSource {
    fn open(path: &Path) -> io::Result<Self> {
        // Reject FIFOs/devices before opening them; validate the opened file too.
        if !path.metadata()?.is_file() {
            return Err(io::Error::other("Stream Load input must be a regular file"));
        }
        let file = File::open(path)?;
        let metadata = file.metadata()?;
        if !metadata.is_file() {
            return Err(io::Error::other("Stream Load input must be a regular file"));
        }
        Ok(Self {
            length: metadata.len(),
            modified: metadata.modified()?,
            file: Arc::new(Mutex::new(file)),
        })
    }

    fn unchanged(&self, file: &File) -> io::Result<()> {
        let metadata = file.metadata()?;
        if metadata.len() != self.length || metadata.modified()? != self.modified {
            return Err(io::Error::other(
                "Stream Load input changed during the attempt",
            ));
        }
        Ok(())
    }

    fn check(&self) -> io::Result<()> {
        let file = self
            .file
            .lock()
            .map_err(|_| io::Error::other("input reader poisoned"))?;
        self.unchanged(&file)
    }

    fn read_chunk(&self, offset: u64) -> io::Result<Option<Bytes>> {
        let mut file = self
            .file
            .lock()
            .map_err(|_| io::Error::other("input reader poisoned"))?;
        self.unchanged(&file)?;
        if offset == self.length {
            return Ok(None);
        }
        let remaining =
            usize::try_from((self.length - offset).min(64 * 1024)).map_err(io::Error::other)?;
        let mut buffer = vec![0; remaining];
        // File::try_clone shares a cursor on several platforms. Keep seek+read
        // together so a late FE body and its redirected BE body cannot interfere.
        file.seek(SeekFrom::Start(offset))?;
        let count = file.read(&mut buffer)?;
        if count == 0 {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "input truncated",
            ));
        }
        self.unchanged(&file)?;
        buffer.truncate(count);
        Ok(Some(Bytes::from(buffer)))
    }
}
