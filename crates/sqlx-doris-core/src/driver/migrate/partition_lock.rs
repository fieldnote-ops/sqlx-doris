//! Doris migration partition lock operations.
use super::*;

impl DorisConnection {
    // Cooperative checkpoint only: Doris does not atomically combine this
    // ownership read with subsequent business SQL or a metadata-state write.
    pub(super) async fn check_migration_owner(&mut self) -> Result<(), Error> {
        self.ready()?;
        let Some(lock) = &self.migration_lock else {
            return Ok(()); // Explicit externally serialized execution.
        };
        let token = lock.token.clone();
        match self.migration_lock_owner().await {
            Ok(Some(owner)) if owner == token => Ok(()),
            result => {
                self.retired = true;
                match result {
                    Err(e) => Err(error(format!(
                        "cannot verify migration lock: {e}; connection retired; inspect lock status and actual schema/data"
                    ))),
                    Ok(_) => Err(error(
                        "migration lock lost; refusing further statements or completion markers; inspect lock status and actual schema/data",
                    )),
                }
            }
        }
    }

    /// Owner token of the durable migration lock. Crashed owners are never
    /// automatically taken over.
    pub async fn migration_lock_owner(&mut self) -> Result<Option<String>, Error> {
        let db = self.migration_database().await?;
        self.lock_owner(&db).await
    }

    /// Owner of an interrupted lock-release/recovery transition, if present.
    /// Use maintenance recovery only after all migration/unlock/recovery
    /// processes have stopped and no previously submitted commands can commit.
    pub async fn migration_transition_owner(&mut self) -> Result<Option<String>, Error> {
        let db = self.migration_database().await?;
        self.gate_owner(&db).await
    }

    pub(super) async fn lock_owner(&mut self, database: &str) -> Result<Option<String>, Error> {
        self.partition_owner(database, LOCK_TABLE, LOCK_FORMAT, "migration lock")
            .await
    }

    pub(super) async fn gate_owner(&mut self, database: &str) -> Result<Option<String>, Error> {
        self.partition_owner(database, GATE_TABLE, GATE_FORMAT, "transition gate")
            .await
    }

    pub(super) async fn partition_owner(
        &mut self,
        database: &str,
        table: &str,
        format: &str,
        label: &str,
    ) -> Result<Option<String>, Error> {
        let comment: Option<String> = sqlx_core::query_scalar::query_scalar("SELECT TABLE_COMMENT FROM information_schema.tables WHERE TABLE_SCHEMA=? AND TABLE_NAME=?")
            .bind(database).bind(table).fetch_optional(&mut *self).await?;
        let Some(comment) = comment else {
            return Ok(None);
        };
        if comment != format {
            return Err(error(format!(
                "unsupported {label} format; stop all old runners and retire the old prerelease {label} before using partition-lock v2; no automatic conversion"
            )));
        }
        // SHOW PARTITIONS must use the text protocol: Doris 4.1.5 cannot prepare
        // this metadata statement reliably. Unknown layouts fail closed.
        let rows = self
            .fetch_all(trusted(format!(
                "SHOW PARTITIONS FROM {}.{}",
                quote(database)?,
                quote(table)?
            )))
            .await?;
        let mut anchor = false;
        let mut owner = None;
        for row in rows {
            let name: &str = row.try_get("PartitionName")?;
            let range: &str = row.try_get("Range")?;
            let state: &str = row.try_get("State")?;
            if state != "NORMAL" {
                return Err(error(format!("{label} partition is not NORMAL")));
            }
            if name == LOCK_ANCHOR && owner_range(range, -1, 0) && !anchor {
                anchor = true;
            } else if let Some(token) = name.strip_prefix(LOCK_OWNER_PREFIX) {
                let uuid = uuid::Uuid::parse_str(token)
                    .map_err(|_| error(format!("invalid {label} partition owner")))?;
                if owner.is_some()
                    || name != owner_partition(&uuid.to_string())?
                    || !owner_range(range, 0, 1)
                {
                    return Err(error(format!("invalid {label} owner layout")));
                }
                owner = Some(uuid.to_string());
            } else {
                return Err(error(format!("unknown {label} partition layout")));
            }
        }
        if !anchor {
            return Err(error(format!("{label} anchor is missing")));
        }
        Ok(owner)
    }

    pub(super) async fn ensure_partition_lock(
        &mut self,
        database: &str,
        name: &str,
        format: &str,
        label: &str,
    ) -> Result<String, Error> {
        let table = format!("{}.{}", quote(database)?, quote(name)?);
        let replicas = self.migration_replication;
        if replicas == 0 {
            return Err(error("migration replication must be at least one"));
        }
        self.execute(trusted(format!("CREATE TABLE IF NOT EXISTS {table}(id TINYINT NOT NULL) DUPLICATE KEY(id) COMMENT '{format}' PARTITION BY RANGE(id)(PARTITION {LOCK_ANCHOR} VALUES [('-1'),('0'))) DISTRIBUTED BY HASH(id) BUCKETS 1 PROPERTIES('replication_num'='{replicas}')"))).await?;
        self.partition_owner(database, name, format, label).await?;
        Ok(table)
    }

    pub(super) async fn transition(&mut self) -> Result<Transition<'_>, Error> {
        self.ready()?;
        lock_deadline(self.migration_lock_timeout)?;
        let database = self.migration_database().await?;
        let table = self
            .ensure_partition_lock(&database, GATE_TABLE, GATE_FORMAT, "transition gate")
            .await?;
        let token = uuid::Uuid::new_v4().to_string();
        let replicas = self.migration_replication;
        if replicas == 0 {
            return Err(error("migration replication must be at least one"));
        }
        let sql = owner_add(&table, &token, replicas)?;
        let deadline = lock_deadline(self.migration_lock_timeout)?;
        loop {
            match self.execute(sql.clone()).await {
                Ok(_) => {
                    return Ok(Transition {
                        conn: self,
                        database,
                        table,
                        token,
                        armed: true,
                    });
                }
                Err(e) if e.as_database_error().is_some() => {
                    if !metadata_conflict(&e) {
                        return Err(e);
                    }
                    let owner = self.gate_owner(&database).await?;
                    if Instant::now() >= deadline {
                        return Err(error(format!(
                            "migration transition held by {}; no automatic takeover; inspect lock status and use lock recover-transition during exclusive maintenance",
                            owner.as_deref().unwrap_or("a changing owner")
                        )));
                    }
                }
                Err(e) => return Err(e),
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    }
}
