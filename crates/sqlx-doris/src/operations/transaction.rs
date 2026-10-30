use crate::{Error, Result};
use sqlx::{Executor, MySql, MySqlPool, SqlSafeStr, mysql::MySqlQueryResult, pool::PoolConnection};

/// An explicitly limited Doris raw-SQL write transaction.
///
/// `execute` uses COM_QUERY. It does not interpolate `?` or emulate prepared
/// statements. Drop, cancellation while executing, and failed completion close
/// the owned connection instead of returning an open transaction to the pool.
#[derive(Debug)]
pub struct DorisTransaction {
    connection: PoolConnection<MySql>,
    completed: bool,
    interrupted: bool,
}

impl DorisTransaction {
    pub(crate) async fn begin(pool: &MySqlPool) -> Result<Self> {
        let mut transaction = Self {
            connection: pool.acquire().await?,
            completed: false,
            interrupted: false,
        };
        transaction.connection.execute("BEGIN").await?;
        Ok(transaction)
    }
    /// Execute trusted raw SQL without parameter substitution. After cancellation,
    /// this guard rejects further writes; drop closes its connection.
    /// A server statement error does not roll back prior successful statements;
    /// the caller may still explicitly commit or roll back the transaction.
    pub async fn execute(&mut self, sql: impl SqlSafeStr + Send) -> Result<MySqlQueryResult> {
        if self.interrupted {
            return Err(Error::Unsupported(
                "the transaction was interrupted; roll it back or drop it",
            ));
        }
        self.interrupted = true;
        let result = (&mut *self.connection).execute(sql.into_sql_str()).await;
        self.interrupted = false;
        Ok(result?)
    }
    /// Commit and release the connection. A failed commit closes the connection;
    /// its outcome can be uncertain and must not be blindly replayed.
    pub async fn commit(mut self) -> Result<()> {
        if self.interrupted {
            return Err(Error::Unsupported(
                "cannot commit an interrupted transaction",
            ));
        }
        self.connection.execute("COMMIT").await?;
        self.completed = true;
        Ok(())
    }
    /// Roll back and release the connection; close it if rollback fails.
    pub async fn rollback(mut self) -> Result<()> {
        self.connection.execute("ROLLBACK").await?;
        self.completed = true;
        Ok(())
    }
}

impl Drop for DorisTransaction {
    fn drop(&mut self) {
        if !self.completed {
            self.connection.close_on_drop();
        }
    }
}
