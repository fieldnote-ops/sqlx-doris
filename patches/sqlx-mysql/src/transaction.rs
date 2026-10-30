use sqlx_core::sql_str::SqlStr;

use crate::connection::Waiting;
use crate::error::Error;
use crate::executor::Executor;
use crate::io::MySqlBufExt;
use crate::protocol::response::{OkPacket, Status};
use crate::protocol::text::Query;
use crate::{MySql, MySqlConnection};

pub(crate) use sqlx_core::transaction::*;

/// Implementation of [`TransactionManager`] for MySQL.
pub struct MySqlTransactionManager;

impl TransactionManager for MySqlTransactionManager {
    type Database = MySql;

    async fn begin(conn: &mut MySqlConnection, statement: Option<SqlStr>) -> Result<(), Error> {
        let depth = conn.inner.transaction_depth;

        if conn.inner.doris_transaction_compatibility {
            if depth != 0 {
                // SQLx's failed child Transaction is dropped immediately. Retire
                // the entire unsupported nesting attempt so the parent cannot
                // report a successful commit after child-drop cleanup.
                conn.inner.stream.doris_transaction_retired = true;
                return Err(err_protocol!(
                    "Doris savepoints are unsupported; the outer transaction must be closed"
                ));
            }
            if statement.is_some() {
                return Err(err_protocol!(
                    "Doris transaction compatibility requires standard BEGIN"
                ));
            }
            conn.inner.stream.finish_doris_read_before_begin().await?;
            // Doris forbids SET inside a transaction. Read the actual session mode
            // before BEGIN, without changing it; custom MySQL sessions stay intact.
            conn.inner.doris_no_backslash_escapes = conn.doris_sql_mode().await?;
            let ok = doris_control(conn, "BEGIN").await?;
            if !doris_begin_acknowledged(&ok) {
                conn.inner.stream.doris_transaction_retired = true;
                return Err(Error::BeginFailed);
            }
            conn.inner.transaction_depth = 1;
            return Ok(());
        }

        let statement = match statement {
            // custom `BEGIN` statements are not allowed if we're already in a transaction
            // (we need to issue a `SAVEPOINT` instead)
            Some(_) if depth > 0 => return Err(Error::InvalidSavePointStatement),
            Some(statement) => statement,
            None => begin_ansi_transaction_sql(depth),
        };
        conn.execute(statement).await?;
        if !conn.in_transaction() {
            return Err(Error::BeginFailed);
        }
        conn.inner.transaction_depth += 1;

        Ok(())
    }

    async fn commit(conn: &mut MySqlConnection) -> Result<(), Error> {
        if conn.inner.doris_transaction_compatibility {
            doris_control(conn, "COMMIT").await?;
            conn.inner.transaction_depth = 0;
            return Ok(());
        }
        let depth = conn.inner.transaction_depth;

        if depth > 0 {
            conn.execute(commit_ansi_transaction_sql(depth)).await?;
            conn.inner.transaction_depth = depth - 1;
        }

        Ok(())
    }

    async fn rollback(conn: &mut MySqlConnection) -> Result<(), Error> {
        if conn.inner.doris_transaction_compatibility {
            doris_control(conn, "ROLLBACK").await?;
            conn.inner.transaction_depth = 0;
            return Ok(());
        }
        let depth = conn.inner.transaction_depth;

        if depth > 0 {
            conn.execute(rollback_ansi_transaction_sql(depth)).await?;
            conn.inner.transaction_depth = depth - 1;
        }

        Ok(())
    }

    fn start_rollback(conn: &mut MySqlConnection) {
        let depth = conn.inner.transaction_depth;

        if conn.inner.doris_transaction_compatibility {
            // Closing a Doris session aborts its unfinished transaction. Native
            // pool return always pings, which rejects this retired connection.
            // No control or write is replayed after an uncertain response.
            if depth > 0 {
                conn.inner.stream.doris_transaction_retired = true;
                conn.inner.transaction_depth = 0;
            }
            return;
        }

        if depth > 0 {
            conn.inner.stream.waiting.push_back(Waiting::Result);
            conn.inner.stream.sequence_id = 0;
            conn.inner
                .stream
                .write_packet(Query(rollback_ansi_transaction_sql(depth).as_str()))
                .expect("BUG: unexpected error queueing ROLLBACK");

            conn.inner.transaction_depth = depth - 1;
        }
    }

    fn get_transaction_depth(conn: &MySqlConnection) -> usize {
        conn.inner.transaction_depth
    }
}

async fn doris_control(conn: &mut MySqlConnection, sql: &'static str) -> Result<OkPacket, Error> {
    if !conn.inner.stream.waiting.is_empty() || !conn.inner.stream.write_buffer().is_empty() {
        conn.inner.stream.doris_transaction_retired = true;
        return Err(err_protocol!(
            "Doris transaction has an unfinished query; close this connection"
        ));
    }
    conn.inner.stream.wait_until_ready().await?;
    // Set before the first send await. Cancellation or any error leaves the
    // connection retired, including BEGIN before depth has been incremented.
    conn.inner.stream.doris_transaction_retired = true;
    conn.inner.stream.waiting.push_back(Waiting::Result);
    conn.inner.stream.send_packet(Query(sql)).await?;
    let ok = conn.inner.stream.recv_ok().await?;
    if ok.status.contains(Status::SERVER_MORE_RESULTS_EXISTS) {
        return Err(err_protocol!(
            "unexpected multiple results for Doris transaction control"
        ));
    }
    conn.inner.status_flags = ok.status;
    conn.inner.stream.waiting.pop_front();
    conn.inner.stream.doris_transaction_retired = false;
    Ok(ok)
}

fn doris_begin_acknowledged(ok: &OkPacket) -> bool {
    if ok.status.contains(Status::SERVER_STATUS_IN_TRANS) {
        return true;
    }
    let mut info = ok.info.clone();
    let Ok(length) = info.get_uint_lenenc() else {
        return false;
    };
    if usize::try_from(length).ok() != Some(info.len()) {
        return false;
    }
    let Ok(message) = std::str::from_utf8(&info) else {
        return false;
    };
    let Some(label) = message
        .strip_prefix("{'label':'txn_insert_")
        .and_then(|s| s.strip_suffix("', 'status':'PREPARE', 'txnId':''}"))
    else {
        return false;
    };
    !label.is_empty()
        && label
            .bytes()
            .all(|c| c.is_ascii_alphanumeric() || c == b'-' || c == b'_')
}

#[cfg(test)]
mod doris_tests {
    use super::*;
    use crate::io::ProtocolDecode;
    use crate::MySqlConnectOptions;
    use bytes::Bytes;

    fn receipt(message: &[u8]) -> OkPacket {
        let mut packet = vec![0, 0, 0, 0, 0, 0, 0];
        packet.push(message.len() as u8);
        packet.extend_from_slice(message);
        OkPacket::decode(Bytes::from(packet)).unwrap()
    }

    #[test]
    fn doris_option_is_explicit_and_defaults_off() {
        assert!(!MySqlConnectOptions::new().get_doris_transaction_compatibility());
        assert!(MySqlConnectOptions::new()
            .doris_transaction_compatibility(true)
            .get_doris_transaction_compatibility());
    }

    #[test]
    fn missing_status_requires_exact_doris_begin_receipt() {
        assert!(doris_begin_acknowledged(&receipt(
            b"{'label':'txn_insert_ab-cd', 'status':'PREPARE', 'txnId':''}"
        )));
        for message in [
            b"".as_slice(),
            b"OK",
            b"{'label':'txn_insert_ab-cd', 'status':'VISIBLE', 'txnId':''}",
            b"{'label':'txn_insert_', 'status':'PREPARE', 'txnId':''}",
        ] {
            assert!(!doris_begin_acknowledged(&receipt(message)));
        }
    }

    #[test]
    fn normal_mysql_status_is_not_dependent_on_doris_info() {
        let mut ok = receipt(b"");
        ok.status = Status::SERVER_STATUS_IN_TRANS;
        assert!(doris_begin_acknowledged(&ok));
    }

    #[test]
    fn malformed_doris_info_is_rejected_without_panicking() {
        let mut ok = receipt(b"{'label':'txn_insert_ab-cd', 'status':'PREPARE', 'txnId':''}");
        let complete = ok.info.clone();
        for length in 0..complete.len() {
            ok.info = complete.slice(..length);
            assert!(!doris_begin_acknowledged(&ok));
        }
        for info in [b"\xfc".as_slice(), b"\xff", b"\x01\xff", b"\x00\x00"] {
            ok.info = Bytes::copy_from_slice(info);
            assert!(!doris_begin_acknowledged(&ok));
        }
    }
}
