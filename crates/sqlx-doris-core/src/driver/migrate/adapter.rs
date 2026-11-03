//! Native SQLx Migrate and MigrateDatabase implementations.
use super::*;

impl Migrate for DorisConnection {
    fn create_schema_if_not_exists<'e>(
        &'e mut self,
        schema: &'e str,
    ) -> BoxFuture<'e, Result<(), MigrateError>> {
        Box::pin(async move {
            self.execute(trusted(format!(
                "CREATE DATABASE IF NOT EXISTS {}",
                quote(schema)?
            )))
            .await?;
            Ok(())
        })
    }
    fn ensure_migrations_table<'e>(
        &'e mut self,
        table: &'e str,
    ) -> BoxFuture<'e, Result<(), MigrateError>> {
        Box::pin(async move {
            let table = self.migration_table_identifier(table).await?;
            let replicas = self.migration_replication;
            if replicas == 0 {
                return Err(error("migration replication must be at least one").into());
            }
            let sql = format!(
                "CREATE TABLE IF NOT EXISTS {table}(version BIGINT NOT NULL,description STRING NOT NULL,installed_on DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),success BOOLEAN NOT NULL,checksum VARCHAR(96) NOT NULL,execution_time BIGINT NOT NULL) UNIQUE KEY(version) DISTRIBUTED BY HASH(version) BUCKETS 1 PROPERTIES('replication_num'='{replicas}','enable_unique_key_merge_on_write'='true')"
            );
            self.execute(trusted(sql)).await?;
            Ok(())
        })
    }
    fn dirty_version<'e>(
        &'e mut self,
        table: &'e str,
    ) -> BoxFuture<'e, Result<Option<i64>, MigrateError>> {
        Box::pin(async move {
            let table = self.migration_table_identifier(table).await?;
            Ok(sqlx_core::query_scalar::query_scalar(trusted(format!(
                "SELECT version FROM {table} WHERE success=false ORDER BY version LIMIT 1"
            )))
            .fetch_optional(&mut *self)
            .await?)
        })
    }
    fn list_applied_migrations<'e>(
        &'e mut self,
        table: &'e str,
    ) -> BoxFuture<'e, Result<Vec<AppliedMigration>, MigrateError>> {
        Box::pin(async move {
            let table = self.migration_table_identifier(table).await?;
            let rows = self
                .fetch_all(trusted(format!(
                    "SELECT version,checksum FROM {table} WHERE success=true ORDER BY version"
                )))
                .await?;
            rows.into_iter()
                .map(|row| {
                    Ok(AppliedMigration {
                        version: row.try_get(0)?,
                        checksum: checksum_bytes(row.try_get::<&str, _>(1)?)?.into(),
                    })
                })
                .collect()
        })
    }
    fn lock(&mut self) -> BoxFuture<'_, Result<(), MigrateError>> {
        Box::pin(async move {
            self.ready()?;
            // Validate before I/O; start the contention budget after the parent
            // layout has been verified, as for ordinary finite timeouts.
            lock_deadline(self.migration_lock_timeout)?;
            if self.transaction {
                return Err(error("cannot acquire a migration lock inside a transaction").into());
            }
            if let Some(lock) = &self.migration_lock {
                let token = lock.token.clone();
                if self.migration_lock_owner().await?.as_deref() == Some(&token) {
                    return Ok(());
                }
                self.retired = true;
                return Err(error("owned migration lock was changed externally").into());
            }
            let db = self.migration_database().await?;
            let table = self
                .ensure_partition_lock(&db, LOCK_TABLE, LOCK_FORMAT, "migration lock")
                .await?;
            let token = uuid::Uuid::new_v4().to_string();
            let replicas = self.migration_replication;
            if replicas == 0 {
                return Err(error("migration replication must be at least one").into());
            }
            let sql = owner_add(&table, &token, replicas)?;
            let deadline = lock_deadline(self.migration_lock_timeout)?;
            loop {
                if let Some(owner) = self.migration_transition_owner().await? {
                    if Instant::now() >= deadline {
                        return Err(error(format!("migration transition held by {owner}; inspect lock status and use lock recover-transition during exclusive maintenance")).into());
                    }
                    tokio::time::sleep(Duration::from_millis(25)).await;
                    continue;
                }
                match self.execute(sql.clone()).await {
                    Ok(_) => {
                        self.migration_lock = Some(Lock { table, token });
                        // A predecessor may have started releasing immediately
                        // after our first check. Do not begin SQL until its
                        // ownership transition has completed.
                        while let Some(owner) = self.migration_transition_owner().await? {
                            if Instant::now() >= deadline {
                                self.retired = true;
                                return Err(error(format!("migration transition held by {owner}; newly acquired lock retained for recovery")).into());
                            }
                            tokio::time::sleep(Duration::from_millis(25)).await;
                        }
                        return Ok(());
                    }
                    Err(e) if e.as_database_error().is_some() => {
                        if !metadata_conflict(&e) {
                            return Err(e.into());
                        }
                        let owner = self.migration_lock_owner().await?;
                        if Instant::now() >= deadline {
                            return Err(error(format!("migration lock held by {}; wait or explicitly recover after stopping its runner",owner.as_deref().unwrap_or("a changing owner"))).into());
                        }
                    }
                    Err(e) => {
                        self.retired = true;
                        return Err(e.into());
                    }
                }
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
        })
    }
    fn unlock(&mut self) -> BoxFuture<'_, Result<(), MigrateError>> {
        Box::pin(async move {
            self.ready()?;
            if self.migration_running {
                return Err(error("migration incomplete; reconcile its SQL and jobs before explicit lock recovery").into());
            }
            if self.migration_lock.is_none() {
                return Ok(());
            }
            let outcome = async {
                let mut gate = self.transition().await?;
                let result = async {
                    let lock = gate.conn.migration_lock.as_ref().unwrap();
                    let table = lock.table.clone();
                    let token = lock.token.clone();
                    if gate.conn.lock_owner(&gate.database).await?.as_deref() != Some(&token) {
                        return Err(error("refusing to release another migration owner's lock"));
                    }
                    gate.remove_main(&table, &token).await?;
                    gate.conn.migration_lock = None;
                    Ok(())
                }
                .await;
                gate.finish(result).await
            }
            .await;
            if outcome.is_err() {
                self.retired = true;
            }
            outcome.map_err(Into::into)
        })
    }
    fn apply<'e>(
        &'e mut self,
        table: &'e str,
        m: &'e Migration,
    ) -> BoxFuture<'e, Result<Duration, MigrateError>> {
        Box::pin(async move {
            self.execute_migration(table, m, false)
                .await
                .map_err(|e| MigrateError::ExecuteMigration(e, m.version))
        })
    }
    fn revert<'e>(
        &'e mut self,
        table: &'e str,
        m: &'e Migration,
    ) -> BoxFuture<'e, Result<Duration, MigrateError>> {
        Box::pin(async move {
            self.execute_migration(table, m, true)
                .await
                .map_err(|e| MigrateError::ExecuteMigration(e, m.version))
        })
    }
    fn skip<'e>(
        &'e mut self,
        table: &'e str,
        m: &'e Migration,
    ) -> BoxFuture<'e, Result<(), MigrateError>> {
        Box::pin(async move {
            let table = self.migration_table_identifier(table).await?;
            self.mark(&table, m, true, Duration::ZERO).await?;
            Ok(())
        })
    }
}
impl MigrateDatabase for Doris {
    async fn create_database(url: &str) -> Result<(), Error> {
        let (mut c, db) = maintenance(url).await?;
        let r = c
            .execute(trusted(format!("CREATE DATABASE {}", quote(&db)?)))
            .await;
        c.close().await?;
        r.map(|_| ())
    }
    async fn database_exists(url: &str) -> Result<bool, Error> {
        let (mut c, db) = maintenance(url).await?;
        let n: i64 = sqlx_core::query_scalar::query_scalar(
            "SELECT COUNT(*) FROM information_schema.schemata WHERE SCHEMA_NAME=?",
        )
        .bind(db)
        .fetch_one(&mut c)
        .await?;
        c.close().await?;
        Ok(n > 0)
    }
    async fn drop_database(url: &str) -> Result<(), Error> {
        let (mut c, db) = maintenance(url).await?;
        let r = c
            .execute(trusted(format!("DROP DATABASE {}", quote(&db)?)))
            .await;
        c.close().await?;
        r.map(|_| ())
    }
}
async fn maintenance(url: &str) -> Result<(DorisConnection, String), Error> {
    let mut o = url.parse::<DorisConnectOptions>()?;
    let db = o
        .database
        .take()
        .filter(|s| !s.is_empty())
        .ok_or_else(|| error("database name is required"))?;
    Ok((DorisConnection::connect_with(&o).await?, db))
}
