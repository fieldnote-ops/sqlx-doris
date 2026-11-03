//! Connection lifecycle, statement cache and checked protocol routing.
use super::*;

impl DorisConnection {
    pub(crate) async fn establish(o: &DorisConnectOptions) -> Result<Self, Error> {
        let mut conn = Self {
            wire: Wire::connect(o).await?,
            cache: StatementCache::new(o.cache_capacity),
            dirty: false,
            retired: false,
            transaction: false,
            no_backslash: false,
            rejected_begin: false,
            log: o.log.clone(),
            #[cfg(feature = "migrate")]
            migration_lock: None,
            #[cfg(feature = "migrate")]
            migration_running: false,
            #[cfg(feature = "migrate")]
            recovery_drop_phase: MigrationRecoveryPhase::NotSubmitted,
            #[cfg(feature = "migrate")]
            migration_replication: o.migration_replication,
            #[cfg(feature = "migrate")]
            migration_timeout: o.migration_timeout,
            #[cfg(feature = "migrate")]
            migration_lock_timeout: o.migration_lock_timeout,
        };
        for s in [
            o.group_commit.statement(),
            if o.decimal256 {
                "SET enable_decimal256 = true"
            } else {
                "SET enable_decimal256 = false"
            },
            if o.strict_cast {
                "SET enable_strict_cast = true"
            } else {
                "SET enable_strict_cast = false"
            },
            "SET time_zone = '+00:00'",
        ] {
            conn.control(s).await?;
        }
        Ok(conn)
    }
    pub(super) fn ready(&self) -> Result<(), Error> {
        if self.dirty || self.retired {
            Err(protocol(
                "Doris connection interrupted or unread results remain; close it",
            ))
        } else {
            Ok(())
        }
    }
    pub(super) async fn command(&mut self, tag: u8, data: &[u8]) -> Result<(), Error> {
        self.ready()?;
        self.dirty = true;
        self.wire.command(tag, data).await
    }
    pub(super) async fn receive(&mut self) -> Result<bytes::Bytes, Error> {
        let p = self.wire.recv().await?;
        if p.first() == Some(&255) {
            let e = database_error(&p)?;
            self.dirty = false;
            return Err(e);
        }
        Ok(p)
    }
    pub(super) async fn control(&mut self, s: &str) -> Result<OkReply, Error> {
        self.command(3, s.as_bytes()).await?;
        let p = self.receive().await?;
        let ok = ok_reply(&p)?;
        if ok.status & 8 != 0 {
            return Err(protocol("unexpected extra control result"));
        }
        self.dirty = false;
        Ok(ok)
    }
    pub(super) async fn close_statement(&mut self, id: u32) -> Result<(), Error> {
        self.command(25, &id.to_le_bytes()).await?;
        self.dirty = false;
        Ok(())
    }
    pub(super) async fn columns(&mut self, count: usize) -> Result<Arc<Vec<DorisColumn>>, Error> {
        if count > 65_535 {
            return Err(protocol("too many result columns"));
        }
        let mut columns = Vec::with_capacity(count);
        for ordinal in 0..count {
            let p = self.receive().await?;
            let mut c = Cursor(&p);
            c.bytes()?; // catalog
            let schema = String::from_utf8(c.bytes()?.to_vec())
                .map_err(|_| protocol("invalid metadata schema"))?;
            c.bytes()?; // table alias
            let table = String::from_utf8(c.bytes()?.to_vec())
                .map_err(|_| protocol("invalid metadata table"))?;
            let name = String::from_utf8(c.bytes()?.to_vec())
                .map_err(|_| protocol("invalid column name"))?;
            let original = String::from_utf8(c.bytes()?.to_vec())
                .map_err(|_| protocol("invalid original column name"))?;
            let source = (!table.is_empty() && !original.is_empty())
                .then(|| (schema.clone(), table.clone(), original.clone()));
            let origin = if !table.is_empty() && !original.is_empty() {
                sqlx_core::column::ColumnOrigin::Table(sqlx_core::column::TableColumn {
                    table: format!("{schema}.{table}").into(),
                    name: original.into(),
                })
            } else {
                sqlx_core::column::ColumnOrigin::Expression
            };
            if c.len()? != 12 {
                return Err(protocol("invalid column metadata length"));
            }
            let charset = c.u16()?;
            c.u32()?;
            let r#type = ColumnType::try_from(c.byte()?)?;
            let flags = ColumnFlags::from_bits_retain(c.u16()?);
            c.take(3)?;
            columns.push(DorisColumn {
                ordinal,
                name,
                info: DorisTypeInfo {
                    r#type,
                    flags,
                    charset,
                    logical: None,
                },
                origin,
                source,
            });
        }
        if count > 0 {
            let eof = self.receive().await?;
            if eof.first() != Some(&254) || eof.len() != 5 {
                return Err(protocol("missing metadata EOF"));
            }
        }
        Ok(Arc::new(columns))
    }
    pub(super) async fn prepare_cached(
        &mut self,
        sql: SqlStr,
        persistent: bool,
    ) -> Result<Prepared, Error> {
        self.ready()?;
        if persistent && let Some(s) = self.cache.get_mut(sql.as_str()) {
            return Ok(s.clone());
        }
        self.command(22, sql.as_str().as_bytes()).await?;
        let p = self.receive().await?;
        let mut c = Cursor(&p);
        if c.byte()? != 0 {
            return Err(protocol("invalid prepare response"));
        }
        let id = c.u32()?;
        let columns = usize::from(c.u16()?);
        let parameters = usize::from(c.u16()?);
        c.byte()?; // filler; Doris may omit the optional warning count.
        match c.0.len() {
            0 => {}
            2 | 3 => {
                c.u16()?;
                if !c.0.is_empty() && c.byte()? != 1 {
                    return Err(protocol("Doris omitted mandatory prepared metadata"));
                }
            }
            _ => return Err(protocol("invalid prepare warning count")),
        }
        for _ in 0..parameters {
            self.receive().await?;
        }
        if parameters > 0 {
            let eof = self.receive().await?;
            if eof.first() != Some(&254) || eof.len() != 5 {
                return Err(protocol("missing parameter EOF"));
            }
        }
        let columns = self.columns(columns).await?;
        self.dirty = false;
        let binary_safe = !bindings::has_bound_pagination(sql.as_str())?
            && !columns.iter().any(|c| c.info.r#type == ColumnType::Time);
        let prepared = Prepared {
            id,
            binary_safe,
            statement: DorisStatement {
                sql,
                parameters,
                columns,
            },
        };
        if persistent
            && self.cache.capacity() > 0
            && let Some(old) = self
                .cache
                .insert(prepared.statement.sql.as_str(), prepared.clone())
        {
            self.close_statement(old.id).await?;
        }
        Ok(prepared)
    }
    pub(super) async fn sql_mode(&mut self) -> Result<bool, Error> {
        self.command(3, b"SELECT @@sql_mode").await?;
        let mut value = None;
        let first = self.receive().await?;
        let n = Cursor(&first).len()? as usize;
        if n != 1 {
            return Err(protocol("unexpected sql_mode result"));
        }
        self.columns(n).await?;
        loop {
            let p = self.receive().await?;
            if p.first() == Some(&254) && p.len() == 5 {
                self.dirty = false;
                break;
            }
            let mut c = Cursor(&p);
            let s =
                std::str::from_utf8(c.bytes()?).map_err(|_| protocol("sql_mode is not UTF-8"))?;
            if value.is_some() {
                return Err(protocol("multiple sql_mode rows"));
            }
            value = Some(
                s.split(',')
                    .any(|s| s.trim().eq_ignore_ascii_case("NO_BACKSLASH_ESCAPES")),
            );
        }
        value.ok_or_else(|| protocol("missing sql_mode row"))
    }
    pub(super) async fn start(
        &mut self,
        sql: SqlStr,
        mut args: Option<DorisArguments>,
        persistent: bool,
    ) -> Result<(bool, Option<u32>), Error> {
        self.ready()?;
        // A cached binary statement has already had its SQL shape checked.
        // Reuse it directly only when the new parameter types need no Doris
        // byte/TIME adaptation. Type changes still take the checked slow path.
        if persistent
            && !self.transaction
            && let Some(a) = args.as_ref()
            && a.types.iter().all(|t| {
                !matches!(
                    t.r#type,
                    ColumnType::Time
                        | ColumnType::Blob
                        | ColumnType::TinyBlob
                        | ColumnType::MediumBlob
                        | ColumnType::LongBlob
                )
            })
            && let Some(prepared) = self.cache.get_mut(sql.as_str()).cloned()
            && prepared.binary_safe
        {
            if prepared.statement.parameters != a.types.len() {
                return Err(protocol("parameter count differs from FE metadata"));
            }
            return self.execute_prepared(&prepared, a, false).await;
        }
        let adapted = bindings::adapt_query(sql.as_str(), args.as_mut())?;
        let s = adapted.as_deref().unwrap_or(sql.as_str());
        let text = self.transaction
            || args.as_ref().is_some_and(bindings::has_time)
            || bindings::has_bound_pagination(s)?;
        if text {
            let bound = if let Some(a) = &args {
                let modes = bindings::compile_modes(s, a)?;
                let mode = if self.transaction {
                    self.no_backslash
                } else if modes.independent() {
                    false
                } else {
                    self.sql_mode().await?
                };
                Some(modes.select(mode)?)
            } else {
                None
            };
            let s = bound.as_deref().unwrap_or(s);
            let routed = if self.transaction {
                bindings::route_values(s, self.no_backslash)?
            } else {
                None
            };
            self.command(3, routed.as_deref().unwrap_or(s).as_bytes())
                .await?;
            return Ok((false, None));
        }
        let Some(a) = args else {
            self.command(3, s.as_bytes()).await?;
            return Ok((false, None));
        };
        // Check marker count locally before PREPARE or sending values.
        if bindings::sites(s)?.len() != a.types.len() {
            return Err(protocol("parameter count differs from SQL marker count"));
        }
        let cache = persistent && self.cache.capacity() > 0;
        let prepared = self
            .prepare_cached(
                adapted.as_ref().map_or_else(
                    || sql.clone(),
                    |s| sqlx_core::sql_str::AssertSqlSafe(s.clone()).into_sql_str(),
                ),
                cache,
            )
            .await?;
        if prepared.statement.parameters != a.types.len() {
            if !cache {
                self.close_statement(prepared.id).await?;
            }
            return Err(protocol("parameter count differs from FE metadata"));
        }
        if prepared
            .statement
            .columns
            .iter()
            .any(|c| c.info.r#type == ColumnType::Time)
        {
            // Choose lossless text before executing; never replay a query.
            if !cache {
                self.close_statement(prepared.id).await?;
            }
            let modes = bindings::compile_modes(s, &a)?;
            let mode = if modes.independent() {
                false
            } else {
                self.sql_mode().await?
            };
            let bound = modes.select(mode)?;
            self.command(3, bound.as_bytes()).await?;
            return Ok((false, None));
        }
        self.execute_prepared(&prepared, &a, !cache).await
    }
    pub(super) async fn execute_prepared(
        &mut self,
        prepared: &Prepared,
        a: &DorisArguments,
        close: bool,
    ) -> Result<(bool, Option<u32>), Error> {
        let mut packet =
            Vec::with_capacity(9 + a.values.len() + a.types.len() * 2 + a.null_bitmap.len());
        packet.extend_from_slice(&prepared.id.to_le_bytes());
        packet.push(0);
        packet.extend_from_slice(&1_u32.to_le_bytes());
        if !a.types.is_empty() {
            packet.extend_from_slice(&a.null_bitmap);
            packet.push(1);
            for t in &a.types {
                packet.push(t.r#type as u8);
                packet.push(if t.flags.contains(ColumnFlags::UNSIGNED) {
                    128
                } else {
                    0
                });
            }
            packet.extend_from_slice(&a.values);
        }
        self.command(23, &packet).await?;
        Ok((true, close.then_some(prepared.id)))
    }
    pub(super) fn row(
        packet: bytes::Bytes,
        columns: Arc<Vec<DorisColumn>>,
        binary: bool,
    ) -> Result<DorisRow, Error> {
        let mut c = Cursor(&packet);
        let mut ranges = Vec::with_capacity(columns.len());
        let nulls = if binary {
            if c.byte()? != 0 {
                return Err(protocol("invalid binary row header"));
            }
            c.take((columns.len() + 2).div_ceil(8))?
        } else {
            &[]
        };
        for (i, column) in columns.iter().enumerate() {
            let null = if binary {
                nulls[(i + 2) / 8] & (1 << ((i + 2) % 8)) != 0
            } else {
                c.0.first() == Some(&251)
            };
            if null {
                if !binary {
                    c.byte()?;
                }
                ranges.push(None);
                continue;
            }
            let value = if binary {
                use ColumnType::*;
                match column.info.r#type {
                    Tiny => c.take(1)?,
                    Short | Year => c.take(2)?,
                    Long | Int24 | Float => c.take(4)?,
                    LongLong | Double => c.take(8)?,
                    Date | Datetime | Timestamp | Time => {
                        let len = usize::from(c.byte()?);
                        c.take(len)?
                    }
                    Null => return Err(protocol("non-null NULL column")),
                    _ => c.bytes()?,
                }
            } else {
                c.bytes()?
            };
            let start = value.as_ptr() as usize - packet.as_ptr() as usize;
            ranges.push(Some(start..start + value.len()));
        }
        if !c.0.is_empty() {
            return Err(protocol("trailing row bytes"));
        }
        Ok(DorisRow {
            packet,
            ranges,
            columns,
            format: if binary {
                DorisValueFormat::Binary
            } else {
                DorisValueFormat::Text
            },
        })
    }
}

impl Connection for DorisConnection {
    type Database = Doris;
    type Options = DorisConnectOptions;
    async fn close(mut self) -> Result<(), Error> {
        #[cfg(feature = "migrate")]
        {
            if self.migration_running {
                self.retired = true;
            }
            if !self.dirty && !self.retired && self.migration_lock.is_some() {
                sqlx_core::migrate::Migrate::unlock(&mut self)
                    .await
                    .map_err(|e| protocol(&e.to_string()))?;
            }
        }
        if self.dirty || self.retired {
            drop(self);
            return Ok(());
        }
        self.wire.command(1, &[]).await?;
        self.wire.socket.shutdown().await?;
        Ok(())
    }
    async fn close_hard(self) -> Result<(), Error> {
        drop(self);
        Ok(())
    }
    async fn ping(&mut self) -> Result<(), Error> {
        // SQLx calls ping on pool return to flush pending lifecycle work.
        self.flush().await?;
        self.command(14, &[]).await?;
        let p = self.receive().await?;
        ok_reply(&p)?;
        self.dirty = false;
        Ok(())
    }
    async fn begin(&mut self) -> Result<Transaction<'_, Doris>, Error> {
        Transaction::begin(self, None).await
    }
    fn cached_statements_size(&self) -> usize {
        self.cache.len()
    }
    async fn clear_cached_statements(&mut self) -> Result<(), Error> {
        self.ready()?;
        while let Some(p) = self.cache.remove_lru() {
            self.close_statement(p.id).await?;
        }
        Ok(())
    }
    fn shrink_buffers(&mut self) {
        self.wire.socket.shrink_buffers();
    }
    async fn flush(&mut self) -> Result<(), Error> {
        self.ready()?;
        #[cfg(feature = "migrate")]
        if self.migration_lock.is_some() {
            sqlx_core::migrate::Migrate::unlock(self)
                .await
                .map_err(|e| protocol(&e.to_string()))?;
            // A public ping/flush must not silently leave a usable connection
            // outside a critical section. Pool return discards this connection.
            self.retired = true;
            return Err(protocol(
                "connection retired after releasing a pending migration lock; acquire a new connection",
            ));
        }
        Ok(())
    }
    fn should_flush(&self) -> bool {
        let pending = self.dirty || self.retired;
        #[cfg(feature = "migrate")]
        let pending = pending || self.migration_lock.is_some();
        pending
    }
}
