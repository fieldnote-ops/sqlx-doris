//! Doris migration history operations.
use super::*;

impl DorisConnection {
    /// Remove a reconciled dirty version without replaying SQL. Hold the
    /// migration lock and verify the actual schema/data before calling this.
    pub async fn remove_dirty_migration(
        &mut self,
        table_name: &str,
        version: i64,
    ) -> Result<(), Error> {
        if self.migration_lock.is_none() {
            return Err(error("dirty-state recovery requires the migration lock"));
        }
        if self.transaction {
            return Err(error(
                "dirty-state recovery cannot run inside a transaction",
            ));
        }
        let token = self.migration_lock.as_ref().unwrap().token.clone();
        if self.migration_lock_owner().await?.as_deref() != Some(&token) {
            self.retired = true;
            return Err(error("dirty-state recovery lost its migration lock"));
        }
        if self
            .dirty_version(table_name)
            .await
            .map_err(|e| error(e.to_string()))?
            != Some(version)
        {
            return Err(error("version is not the current dirty migration"));
        }
        let table = self.migration_table_identifier(table_name).await?;
        let mut args = DorisArguments::default();
        args.add(version).map_err(Error::Encode)?;
        self.state_write(
            trusted(format!(
                "DELETE FROM {table} WHERE version=? AND success=false"
            )),
            args,
        )
        .await
    }

    pub(super) async fn migration_database(&mut self) -> Result<String, Error> {
        let db: Option<String> = sqlx_core::query_scalar::query_scalar("SELECT DATABASE()")
            .fetch_one(&mut *self)
            .await?;
        db.filter(|s| !s.is_empty())
            .ok_or_else(|| error("a database must be selected in the Doris URL"))
    }

    /// Resolve and quote a migration state table using the driver's SQLx rules.
    /// Accepts quoted/current-database-qualified identifiers; rejects fragments,
    /// other databases and reserved lock tables. Only reads the current database.
    /// Invalid identifiers return [`Error::Configuration`]; network/metadata errors
    /// retain their original SQLx categories.
    pub async fn migration_table_identifier(&mut self, name: &str) -> Result<String, Error> {
        // Local identifier errors have a distinct SQLx category. Do not turn
        // a malformed/failed SELECT DATABASE reply into an input failure.
        let (parts, table) = (|| -> Result<_, Error> {
            let parsed = split(&format!("SELECT {name}"), true)?;
            if parsed.len() != 1 {
                return Err(error("invalid migration table name"));
            }
            let step = &parsed[0];
            let parts = step.table_after(1)?;
            if parts.last().is_some_and(|p| {
                p.eq_ignore_ascii_case(LOCK_TABLE) || p.eq_ignore_ascii_case(GATE_TABLE)
            }) {
                return Err(error(
                    "migration state table cannot be the reserved lock table",
                ));
            }
            let consumed = if parts.len() == 1 { 2 } else { 4 };
            if step.tokens.len() != consumed {
                return Err(error("invalid migration table identifier"));
            }
            let table = quote(parts.last().unwrap())?;
            Ok((parts, table))
        })()
        .map_err(|e| Error::Configuration(Box::new(e)))?;
        let db = self.migration_database().await?;
        if parts.len() == 2 && parts[0] != db {
            return Err(Error::Configuration(Box::new(error(
                "migration table must be in the connection's current database",
            ))));
        }
        Ok(format!("{}.{}", quote(&db)?, table))
    }

    pub(super) async fn state_write(
        &mut self,
        sql: SqlStr,
        args: DorisArguments,
    ) -> Result<(), Error> {
        let mode: String = sqlx_core::query_scalar::query_scalar("SELECT @@group_commit")
            .fetch_one(&mut *self)
            .await?;
        let restore = match mode.as_str() {
            "off_mode" => None,
            "sync_mode" => Some("SET group_commit=sync_mode"),
            "async_mode" => Some("SET group_commit=async_mode"),
            _ => return Err(error("unrecognized Group Commit setting")),
        };
        if restore.is_some() {
            self.control("SET group_commit=off_mode").await?;
        }
        let result = sqlx_core::query::query_with::<Doris, _>(sql, args)
            .execute(&mut *self)
            .await;
        if let Some(restore) = restore
            && self.ready().is_ok()
        {
            self.control(restore).await?;
        }
        result.map(|_| ())
    }

    pub(super) async fn mark(
        &mut self,
        table: &str,
        m: &Migration,
        success: bool,
        elapsed: Duration,
    ) -> Result<(), Error> {
        let mut args = DorisArguments::default();
        args.add(m.version).map_err(Error::Encode)?;
        args.add(m.description.to_string()).map_err(Error::Encode)?;
        args.add(success).map_err(Error::Encode)?;
        args.add(checksum_hex(&m.checksum)).map_err(Error::Encode)?;
        args.add(
            i64::try_from(elapsed.as_nanos()).map_err(|_| error("migration duration overflow"))?,
        )
        .map_err(Error::Encode)?;
        self.state_write(trusted(format!("INSERT INTO {table}(version,description,success,checksum,execution_time) VALUES(?,?,?,?,?)")),args).await
    }
}
