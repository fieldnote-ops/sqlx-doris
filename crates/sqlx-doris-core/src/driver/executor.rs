//! SQLx execution, result streams and conservative metadata description.
use super::*;

impl<'c> Executor<'c> for &'c mut DorisConnection {
    type Database = Doris;
    fn fetch_many<'e, 'q: 'e, E>(
        self,
        mut query: E,
    ) -> BoxStream<'e, Result<Either<DorisQueryResult, DorisRow>, Error>>
    where
        'c: 'e,
        E: 'q + Execute<'q, Doris>,
    {
        Box::pin(async_stream::try_stream! {
            let args = query.take_arguments().map_err(Error::Encode)?;
            let persistent = query.persistent();
            let sql = query.sql();
            let mut logger = sqlx_core::logger::QueryLogger::new(sql.clone(), self.log.clone());
            let (binary, close) = self.start(sql, args, persistent).await?;

            loop {
                let packet = self.receive().await?;
                if packet.first() == Some(&0) {
                    let ok = ok_reply(&packet)?;
                    logger.increase_rows_affected(ok.result.affected);
                    let more = ok.status & 8 != 0;
                    if !more {
                        self.dirty = false;
                        if let Some(id) = close {
                            self.close_statement(id).await?;
                        }
                    }
                    yield Either::Left(ok.result);
                    if !more {
                        break;
                    }
                    continue;
                }
                if packet.first() == Some(&251) {
                    Err(protocol("LOCAL INFILE is disabled; use Stream Load"))?;
                }
                let count = usize::try_from(Cursor(&packet).len()?)
                    .map_err(|_| protocol("column count overflow"))?;
                let columns = self.columns(count).await?;
                if binary && columns.iter().any(|column| column.info.r#type == ColumnType::Time) {
                    Err(protocol("FE changed result metadata to binary TIME; use raw_sql with an explicit text projection"))?;
                }

                let status = loop {
                    let row = self.receive().await?;
                    if row.first() == Some(&254) && row.len() == 5 {
                        break u16::from_le_bytes([row[3], row[4]]);
                    }
                    logger.increment_rows_returned();
                    yield Either::Right(DorisConnection::row(row, columns.clone(), binary)?);
                };
                let more = status & 8 != 0;
                if !more {
                    self.dirty = false;
                    if let Some(id) = close {
                        self.close_statement(id).await?;
                    }
                }
                yield Either::Left(DorisQueryResult::default());
                if !more {
                    break;
                }
            }
        })
    }
    fn fetch_optional<'e, 'q: 'e, E>(
        self,
        query: E,
    ) -> BoxFuture<'e, Result<Option<DorisRow>, Error>>
    where
        'c: 'e,
        E: 'q + Execute<'q, Doris>,
    {
        Box::pin(async move {
            let mut stream = self.fetch_many(query);
            let mut first = None;
            while let Some(result) = stream.try_next().await? {
                if let Either::Right(row) = result
                    && first.is_none()
                {
                    first = Some(row);
                }
            }
            Ok(first)
        })
    }
    fn prepare_with<'e>(
        self,
        sql: SqlStr,
        _parameters: &'e [DorisTypeInfo],
    ) -> BoxFuture<'e, Result<DorisStatement, Error>>
    where
        'c: 'e,
    {
        Box::pin(async move {
            self.ready()?;
            if self.transaction {
                return Ok(DorisStatement {
                    parameters: bindings::dml_parameters(sql.as_str())?,
                    sql,
                    columns: Arc::new(Vec::new()),
                });
            }
            let cached = self.cache.capacity() > 0;
            let p = self.prepare_cached(sql, cached).await?;
            if !cached {
                self.close_statement(p.id).await?;
            }
            Ok(p.statement)
        })
    }
    fn describe<'e>(
        self,
        sql: SqlStr,
    ) -> BoxFuture<'e, Result<sqlx_core::describe::Describe<Doris>, Error>>
    where
        'c: 'e,
    {
        Box::pin(async move {
            let statement = self.prepare_with(sql, &[]).await?;
            let mut columns = statement.columns.as_ref().clone();
            // Native DESC retains Doris logical types that information_schema
            // normalizes for wire compatibility (BOOLEAN -> TINYINT(1)). Fetch
            // each origin table once, without borrowing its NOT NULL flags:
            // an outer join may still produce NULL for a NOT NULL base column.
            let mut tables = std::collections::HashMap::<
                (String, String),
                std::collections::HashMap<String, String>,
            >::new();
            for column in &mut columns {
                if !column.name.contains(':')
                    && let Some((schema, table, name)) = &column.source
                {
                    let key = (schema.clone(), table.clone());
                    if !tables.contains_key(&key) {
                        if schema.contains('\0') || table.contains('\0') {
                            return Err(protocol("NUL in column origin"));
                        }
                        let quote = |s: &str| format!("`{}`", s.replace('`', "``"));
                        let rows = self
                            .fetch_all(sqlx_core::sql_str::AssertSqlSafe(format!(
                                "DESC {}.{}",
                                quote(schema),
                                quote(table)
                            )))
                            .await?;
                        let fields = rows
                            .into_iter()
                            .map(|row| {
                                Ok((
                                    row.try_get::<String, _>(0)?,
                                    row.try_get::<String, _>(1)?.to_ascii_uppercase(),
                                ))
                            })
                            .collect::<Result<std::collections::HashMap<_, _>, Error>>()?;
                        tables.insert(key.clone(), fields);
                    }
                    column.info.logical = tables[&key].get(name).cloned();
                }
            }
            let nullable = statement
                .columns
                .iter()
                .map(|c| Some(!c.info.flags.contains(ColumnFlags::NOT_NULL)))
                .collect();
            Ok(sqlx_core::describe::Describe {
                columns,
                parameters: Some(Either::Right(statement.parameters)),
                nullable,
            })
        })
    }
}
