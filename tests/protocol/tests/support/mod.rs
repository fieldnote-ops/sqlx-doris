//! A small controllable MySQL protocol peer. It is not a Doris emulator.

use std::sync::{Arc, Mutex};
use std::time::Duration;

use sqlx::{
    Connection, MySqlConnection,
    mysql::{MySqlConnectOptions, MySqlSslMode},
};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

#[derive(Clone)]
pub struct Column {
    pub name: String,
    pub kind: u8,
    pub charset: u16,
    pub flags: u16,
}

impl Column {
    pub fn new(kind: u8) -> Self {
        Self {
            name: "value".into(),
            kind,
            charset: 33,
            flags: 0,
        }
    }
}

#[derive(Clone)]
pub enum Cell {
    Null,
    Bytes(Vec<u8>),
}

impl From<&str> for Cell {
    fn from(value: &str) -> Self {
        Self::Bytes(value.as_bytes().to_vec())
    }
}

pub struct Peer {
    pub port: u16,
    pub commands: Arc<Mutex<Vec<u8>>>,
    task: tokio::task::JoinHandle<()>,
}

fn lenenc(value: &[u8], out: &mut Vec<u8>) {
    match value.len() {
        0..=250 => out.push(value.len() as u8),
        251..=65535 => {
            out.push(252);
            out.extend_from_slice(&(value.len() as u16).to_le_bytes());
        }
        _ => {
            out.push(254);
            out.extend_from_slice(&(value.len() as u64).to_le_bytes());
        }
    }
    out.extend_from_slice(value);
}

async fn write_packet(stream: &mut TcpStream, sequence: &mut u8, payload: &[u8]) {
    let length = (payload.len() as u32).to_le_bytes();
    stream
        .write_all(&[length[0], length[1], length[2], *sequence])
        .await
        .unwrap();
    stream.write_all(payload).await.unwrap();
    *sequence = sequence.wrapping_add(1);
}

async fn read_packet(stream: &mut TcpStream) -> Option<Vec<u8>> {
    let mut header = [0_u8; 4];
    if stream.read_exact(&mut header).await.is_err() {
        return None;
    }
    let length = u32::from_le_bytes([header[0], header[1], header[2], 0]) as usize;
    assert!(
        length < 1_048_576,
        "fixture received an unexpectedly large packet"
    );
    let mut payload = vec![0; length];
    stream.read_exact(&mut payload).await.unwrap();
    Some(payload)
}

fn ok(status: u16, header: u8) -> Vec<u8> {
    let [lo, hi] = status.to_le_bytes();
    vec![header, 0, 0, lo, hi, 0, 0]
}

fn eof(status: u16) -> Vec<u8> {
    let [lo, hi] = status.to_le_bytes();
    vec![254, 0, 0, lo, hi]
}

fn definition(column: &Column) -> Vec<u8> {
    let mut packet = Vec::new();
    for value in ["def", "", "", "", &column.name, &column.name] {
        lenenc(value.as_bytes(), &mut packet);
    }
    packet.push(12);
    packet.extend_from_slice(&column.charset.to_le_bytes());
    packet.extend_from_slice(&255_u32.to_le_bytes());
    packet.push(column.kind);
    packet.extend_from_slice(&column.flags.to_le_bytes());
    packet.extend_from_slice(&[0, 0, 0]);
    packet
}

async fn result_set(
    stream: &mut TcpStream,
    columns: &[Column],
    rows: &[Vec<Cell>],
    binary: bool,
    deprecated: bool,
) {
    let mut seq = 1;
    write_packet(stream, &mut seq, &[columns.len() as u8]).await;
    for column in columns {
        write_packet(stream, &mut seq, &definition(column)).await;
    }
    if !deprecated {
        write_packet(stream, &mut seq, &eof(2)).await;
    }
    for row in rows {
        let mut packet = if binary {
            vec![0; 1 + (columns.len() + 9) / 8]
        } else {
            vec![]
        };
        for (index, cell) in row.iter().enumerate() {
            match cell {
                Cell::Null if binary => packet[1 + (index + 2) / 8] |= 1 << ((index + 2) % 8),
                Cell::Null => packet.push(251),
                Cell::Bytes(value) => lenenc(value, &mut packet),
            }
        }
        write_packet(stream, &mut seq, &packet).await;
    }
    write_packet(
        stream,
        &mut seq,
        &if deprecated { ok(2, 254) } else { eof(2) },
    )
    .await;
}

impl Peer {
    pub async fn start(
        columns: Vec<Column>,
        rows: Vec<Vec<Cell>>,
        begin_status: u16,
        deprecated: bool,
    ) -> Self {
        let listener = TcpListener::bind(("127.0.0.1", 0)).await.unwrap();
        let port = listener.local_addr().unwrap().port();
        let commands = Arc::new(Mutex::new(vec![]));
        let trace = commands.clone();
        let task = tokio::spawn(async move {
            let (mut stream, _) = tokio::time::timeout(Duration::from_secs(10), listener.accept())
                .await
                .unwrap()
                .unwrap();
            stream.set_nodelay(true).unwrap();
            let mut capabilities: u32 = 0x0008_a201; // protocol41, secure, plugin auth, transactions
            if deprecated {
                capabilities |= 0x0100_0000;
            }
            let mut handshake = vec![10];
            handshake.extend_from_slice(b"5.7.99-protocol-fixture\0");
            handshake.extend_from_slice(&1_u32.to_le_bytes());
            handshake.extend_from_slice(b"12345678\0");
            handshake.extend_from_slice(&(capabilities as u16).to_le_bytes());
            handshake.push(33);
            handshake.extend_from_slice(&2_u16.to_le_bytes());
            handshake.extend_from_slice(&((capabilities >> 16) as u16).to_le_bytes());
            handshake.push(21);
            handshake.extend_from_slice(&[0; 10]);
            handshake.extend_from_slice(b"abcdefghijkl\0mysql_native_password\0");
            let mut seq = 0;
            write_packet(&mut stream, &mut seq, &handshake).await;
            read_packet(&mut stream).await.expect("handshake response");
            seq = 2;
            write_packet(&mut stream, &mut seq, &ok(2, 0)).await;
            while let Some(packet) = read_packet(&mut stream).await {
                let command = packet[0];
                trace.lock().unwrap().push(command);
                let mut seq = 1;
                match command {
                    1 => break,
                    3 => {
                        let sql = std::str::from_utf8(&packet[1..]).unwrap();
                        if sql == "SELECT @@sql_mode" {
                            result_set(
                                &mut stream,
                                &[Column::new(254)],
                                &[vec![Cell::from("")]],
                                false,
                                deprecated,
                            )
                            .await;
                            continue;
                        }
                        if sql.starts_with("SET ") || ["BEGIN", "COMMIT", "ROLLBACK"].contains(&sql)
                        {
                            write_packet(
                                &mut stream,
                                &mut seq,
                                &ok(if sql == "BEGIN" { begin_status } else { 2 }, 0),
                            )
                            .await;
                        } else if sql == "FAIL" {
                            let mut error = vec![255, 0x51, 0x04];
                            error.extend_from_slice(b"#HY000fixture failure");
                            write_packet(&mut stream, &mut seq, &error).await;
                        } else {
                            result_set(&mut stream, &columns, &rows, false, deprecated).await;
                        }
                    }
                    14 => write_packet(&mut stream, &mut seq, &ok(2, 0)).await,
                    22 => {
                        let params = packet[1..].iter().filter(|&&b| b == b'?').count();
                        let mut prepared = vec![0, 1, 0, 0, 0];
                        prepared.extend_from_slice(&(columns.len() as u16).to_le_bytes());
                        prepared.extend_from_slice(&(params as u16).to_le_bytes());
                        prepared.extend_from_slice(&[0, 0, 0]);
                        write_packet(&mut stream, &mut seq, &prepared).await;
                        for _ in 0..params {
                            write_packet(&mut stream, &mut seq, &definition(&Column::new(254)))
                                .await;
                        }
                        if params > 0 && !deprecated {
                            write_packet(&mut stream, &mut seq, &eof(2)).await;
                        }
                        for c in &columns {
                            write_packet(&mut stream, &mut seq, &definition(c)).await;
                        }
                        if !columns.is_empty() && !deprecated {
                            write_packet(&mut stream, &mut seq, &eof(2)).await;
                        }
                    }
                    23 => result_set(&mut stream, &columns, &rows, true, deprecated).await,
                    25 => {} // COM_STMT_CLOSE has no response
                    _ => panic!("unexpected command {command}"),
                }
            }
        });
        Self {
            port,
            commands,
            task,
        }
    }

    pub async fn connect(&self) -> MySqlConnection {
        let options = MySqlConnectOptions::new()
            .host("127.0.0.1")
            .port(self.port)
            .username("fixture")
            .ssl_mode(MySqlSslMode::Disabled);
        tokio::time::timeout(
            Duration::from_secs(5),
            MySqlConnection::connect_with(&options),
        )
        .await
        .unwrap()
        .unwrap()
    }

    pub async fn finish(self) {
        tokio::time::timeout(Duration::from_secs(10), self.task)
            .await
            .unwrap()
            .unwrap();
    }
}
