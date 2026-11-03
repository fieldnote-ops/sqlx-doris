//! Doris migration schema operations.
use super::*;

impl DorisConnection {
    pub(super) async fn jobs(
        &mut self,
        db: &str,
        table: &str,
        kind: &str,
    ) -> Result<Vec<(i64, String)>, Error> {
        let rows = self
            .fetch_all(trusted(format!(
                "SHOW ALTER TABLE {kind} FROM {}",
                quote(db)?
            )))
            .await?;
        let mut jobs = Vec::new();
        for row in rows {
            if row.try_get::<String, _>("TableName")? == table {
                let id = match row.try_get::<i64, _>("JobId") {
                    Ok(v) => v,
                    Err(_) => row
                        .try_get::<String, _>("JobId")?
                        .parse::<i64>()
                        .map_err(|e| Error::Decode(e.into()))?,
                };
                jobs.push((id, row.try_get("State")?));
            }
        }
        Ok(jobs)
    }

    pub(super) async fn execute_step(&mut self, step: &Step) -> Result<(), Error> {
        let target = if step.keyword(0, "ALTER") && step.keyword(1, "TABLE") {
            Some((
                step.table_after(2)?,
                if step
                    .tokens
                    .iter()
                    .any(|t| !t.quoted && t.text.eq_ignore_ascii_case("ROLLUP"))
                {
                    "ROLLUP"
                } else {
                    "COLUMN"
                },
            ))
        } else if (step.keyword(0, "CREATE") || step.keyword(0, "DROP")) && step.keyword(1, "INDEX")
        {
            let on = step
                .tokens
                .iter()
                .position(|t| !t.quoted && t.text.eq_ignore_ascii_case("ON"))
                .ok_or_else(|| error("missing index target"))?;
            Some((step.table_after(on + 1)?, "COLUMN"))
        } else {
            None
        };
        let target = if let Some((parts, kind)) = target {
            let current = self.migration_database().await?;
            let db = if parts.len() == 2 {
                parts[0].clone()
            } else {
                current.clone()
            };
            if db != current {
                return Err(error(
                    "asynchronous DDL must stay in the current migration database",
                ));
            }
            let table = parts.last().unwrap().clone();
            let before = self.jobs(&db, &table, kind).await?;
            if before
                .iter()
                .any(|(_, s)| !matches!(s.as_str(), "FINISHED" | "CANCELLED"))
            {
                return Err(error("target table already has an unfinished ALTER job"));
            }
            Some((
                db,
                table,
                kind,
                before.into_iter().map(|v| v.0).collect::<HashSet<_>>(),
            ))
        } else {
            None
        };
        self.check_migration_owner().await?;
        self.execute(trusted(step.sql.clone())).await?;
        if let Some((db, table, kind, before)) = target {
            let budget = self.migration_timeout;
            let wait = async {
                // The same FE registers asynchronous ALTER jobs before DDL ACK.
                // No new job therefore denotes synchronous DDL.
                loop {
                    self.check_migration_owner().await?;
                    let jobs = self.jobs(&db, &table, kind).await?;
                    let submitted = jobs
                        .iter()
                        .filter(|(id, _)| !before.contains(id))
                        .collect::<Vec<_>>();
                    if submitted.iter().any(|(_, state)| state == "CANCELLED") {
                        return Err(error(
                            "schema-change job was cancelled; migration remains dirty",
                        ));
                    }
                    if submitted.iter().all(|(_, state)| state == "FINISHED") {
                        return Ok(());
                    }
                    tokio::time::sleep(Duration::from_millis(100)).await;
                }
            };
            match tokio::time::timeout(budget, wait).await {
                Ok(result) => result?,
                Err(_) => {
                    self.retired = true;
                    return Err(error(
                        "schema-change timeout; reconcile jobs and durable lock before recovery",
                    ));
                }
            }
        }
        Ok(())
    }

    pub(super) async fn execute_migration(
        &mut self,
        table_name: &str,
        m: &Migration,
        revert: bool,
    ) -> Result<Duration, Error> {
        self.ready()?;
        if self.transaction {
            return Err(error("migrations cannot run inside a write transaction"));
        }
        self.check_migration_owner().await?;
        let mode = self.sql_mode().await?;
        let steps = split(m.sql.as_str(), mode)?;
        let table = self.migration_table_identifier(table_name).await?;
        let state_name = split(&format!("SELECT {table_name}"), true)?[0].table_after(1)?;
        let state_name = state_name.last().unwrap();
        for step in &steps {
            if step.tokens.iter().any(|t| {
                t.text.eq_ignore_ascii_case(LOCK_TABLE)
                    || t.text.eq_ignore_ascii_case(GATE_TABLE)
                    || t.text.eq_ignore_ascii_case(state_name)
            }) {
                return Err(error(
                    "migration files cannot reference migration state or lock tables",
                ));
            }
            if ["CREATE", "ALTER", "DROP"]
                .iter()
                .any(|k| step.keyword(0, k))
                && ["DATABASE", "SCHEMA", "CATALOG"]
                    .iter()
                    .any(|k| step.keyword(1, k))
            {
                return Err(error("migration files cannot change databases or catalogs"));
            }
        }
        let original: String = sqlx_core::query_scalar::query_scalar("SELECT @@group_commit")
            .fetch_one(&mut *self)
            .await?;
        let restore = match original.as_str() {
            "off_mode" => None,
            "sync_mode" => Some("SET group_commit=sync_mode"),
            "async_mode" => Some("SET group_commit=async_mode"),
            _ => return Err(error("unrecognized Group Commit mode")),
        };
        self.migration_running = true;
        let guard = Running(self);
        let conn = &mut *guard.0;
        if restore.is_some() {
            conn.control("SET group_commit=off_mode").await?;
        }
        // This committed marker must be visible before any non-atomic DDL.
        conn.check_migration_owner().await?;
        conn.mark(&table, m, false, Duration::ZERO).await?;
        let started = Instant::now();
        for step in steps {
            conn.execute_step(&step).await?;
        }
        let elapsed = started.elapsed();
        conn.check_migration_owner().await?;
        if revert {
            let mut args = DorisArguments::default();
            args.add(m.version).map_err(Error::Encode)?;
            conn.state_write(
                trusted(format!("DELETE FROM {table} WHERE version=?")),
                args,
            )
            .await?;
        } else {
            conn.mark(&table, m, true, elapsed).await?;
        }
        if let Some(restore) = restore {
            conn.control(restore).await?;
        }
        conn.migration_running = false;
        Ok(elapsed)
    }
}
