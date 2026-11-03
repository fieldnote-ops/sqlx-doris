//! Native transaction lifecycle; interrupted writes are never replayed.
use super::*;

impl TransactionManager for DorisTransactionManager {
    type Database = Doris;
    async fn begin(conn: &mut DorisConnection, statement: Option<SqlStr>) -> Result<(), Error> {
        conn.ready()?;
        if conn.transaction {
            conn.rejected_begin = true;
            return Err(Error::InvalidSavePointStatement);
        }
        if statement
            .as_ref()
            .is_some_and(|s| !s.as_str().trim().eq_ignore_ascii_case("BEGIN"))
        {
            return Err(protocol("Doris requires standard BEGIN"));
        }
        conn.no_backslash = conn.sql_mode().await?;
        // Guard a cancelled or ambiguous BEGIN/COMMIT/ROLLBACK independently
        // from packet completion, including server-side errors.
        conn.retired = true;
        conn.dirty = true;
        conn.wire.command(3, b"BEGIN").await?;
        let p = conn.receive().await?;
        let ok = ok_reply(&p)?;
        if ok.status & 8 != 0 {
            return Err(protocol("unexpected additional BEGIN result"));
        }
        let mut info = Cursor(&ok.info);
        let receipt = info.bytes().ok().filter(|_| info.0.is_empty());
        let acknowledged = ok.status & 1 != 0
            || receipt
                .and_then(|v| std::str::from_utf8(v).ok())
                .and_then(|v| v.strip_prefix("{'label':'txn_insert_"))
                .and_then(|v| v.strip_suffix("', 'status':'PREPARE', 'txnId':''}"))
                .is_some_and(|v| {
                    !v.is_empty()
                        && v.bytes()
                            .all(|c| c.is_ascii_alphanumeric() || c == b'-' || c == b'_')
                });
        if !acknowledged {
            return Err(Error::BeginFailed);
        }
        conn.dirty = false;
        conn.retired = false;
        conn.transaction = true;
        Ok(())
    }
    async fn commit(conn: &mut DorisConnection) -> Result<(), Error> {
        finish_transaction(conn, "COMMIT").await
    }
    async fn rollback(conn: &mut DorisConnection) -> Result<(), Error> {
        finish_transaction(conn, "ROLLBACK").await
    }
    fn start_rollback(conn: &mut DorisConnection) {
        if conn.rejected_begin {
            conn.rejected_begin = false;
        } else if conn.transaction {
            conn.retired = true;
        }
    }
    fn get_transaction_depth(conn: &DorisConnection) -> usize {
        usize::from(conn.transaction)
    }
}
async fn finish_transaction(conn: &mut DorisConnection, s: &str) -> Result<(), Error> {
    conn.ready()?;
    if !conn.transaction {
        return Ok(());
    }
    conn.dirty = true;
    conn.retired = true;
    conn.wire.command(3, s.as_bytes()).await?;
    let p = conn.receive().await?;
    if ok_reply(&p)?.status & 8 != 0 {
        return Err(protocol("unexpected additional transaction result"));
    }
    conn.transaction = false;
    conn.dirty = false;
    conn.retired = false;
    Ok(())
}
