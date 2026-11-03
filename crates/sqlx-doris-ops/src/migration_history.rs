//! Read-only migration history and source comparison shared by CLI workflows.
use crate::failure::{self, Result};
use serde::Serialize;
use sqlx_core::{
    migrate::{AppliedMigration, Migration, Migrator},
    row::Row,
};
use sqlx_doris_core::DorisConnection;
use std::collections::BTreeMap;

#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct Record {
    pub version: i64,
    pub success: bool,
    pub checksum: String,
}

#[derive(Debug, PartialEq)]
pub struct History {
    pub present: bool,
    pub rows: Vec<Record>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum SourceState {
    NotRequested,
    Missing,
    Matched,
    ChecksumMismatch,
}

pub struct Comparison {
    pub record: Record,
    pub state: SourceState,
    pub source_checksum: Option<String>,
}

pub fn hex(value: &[u8]) -> String {
    value.iter().map(|byte| format!("{byte:02x}")).collect()
}

pub fn sources(source: &Migrator) -> Result<BTreeMap<i64, &Migration>> {
    let mut versions = BTreeMap::new();
    for migration in source
        .iter()
        .filter(|migration| !migration.migration_type.is_down_migration())
    {
        if versions.insert(migration.version, migration).is_some() {
            return Err(failure::coded(
                2,
                "migration_source_invalid",
                "duplicate up migration versions",
            ));
        }
    }
    Ok(versions)
}

pub fn compare(history: &History, source: Option<&Migrator>) -> Result<Vec<Comparison>> {
    let versions = source.map(sources).transpose()?;
    Ok(history
        .rows
        .iter()
        .map(|record| {
            let migration = versions
                .as_ref()
                .and_then(|versions| versions.get(&record.version));
            let source_checksum = migration.map(|migration| hex(&migration.checksum));
            let state = if versions.is_none() {
                SourceState::NotRequested
            } else if let Some(checksum) = &source_checksum {
                if checksum == &record.checksum {
                    SourceState::Matched
                } else {
                    SourceState::ChecksumMismatch
                }
            } else {
                SourceState::Missing
            };
            Comparison {
                record: record.clone(),
                state,
                source_checksum,
            }
        })
        .collect())
}

pub fn validate_applied(source: &Migrator, applied: &[AppliedMigration]) -> Result<()> {
    let history = History {
        present: true,
        rows: applied
            .iter()
            .map(|record| Record {
                version: record.version,
                success: true,
                checksum: hex(&record.checksum),
            })
            .collect(),
    };
    for comparison in compare(&history, Some(source))? {
        match comparison.state {
            SourceState::Missing => {
                return Err(failure::coded(
                    3,
                    "migration_source_missing",
                    "applied migration has no up source; restore it before modifying migration state",
                ));
            }
            SourceState::ChecksumMismatch => {
                return Err(failure::coded(
                    3,
                    "migration_checksum_mismatch",
                    "migration checksum mismatch; restore its applied up source",
                ));
            }
            _ => {}
        }
    }
    Ok(())
}

/// Read without creating tables, using the driver's exact identifier parser.
pub async fn observe(connection: &mut DorisConnection, table: &str) -> Result<History> {
    let identifier = connection
        .migration_table_identifier(table)
        .await
        .map_err(|error| {
            if matches!(error, sqlx_core::Error::Configuration(_)) {
                failure::coded(
                    2,
                    "migration_table_invalid",
                    "invalid migration table identifier",
                )
            } else {
                Box::new(error) as Box<dyn std::error::Error + Send + Sync>
            }
        })?;
    let exists: i64 = sqlx_core::query_scalar::query_scalar(
        r#"SELECT COUNT(*) FROM information_schema.tables WHERE table_schema=DATABASE() AND CONCAT('`',REPLACE(table_schema,'`','``'),'`.`',REPLACE(table_name,'`','``'),'`')=?"#
    ).bind(&identifier).fetch_one(&mut *connection).await?;
    if exists == 0 {
        return Ok(History {
            present: false,
            rows: Vec::new(),
        });
    }
    if exists != 1 {
        return Err(failure::coded(
            4,
            "migration_table_ambiguous",
            "migration table identity ambiguous",
        ));
    }
    let rows = sqlx_core::query::query(sqlx_core::sql_str::AssertSqlSafe(format!(
        "SELECT version, success, checksum FROM {identifier} ORDER BY version"
    )))
    .fetch_all(&mut *connection)
    .await?;
    let mut records = BTreeMap::new();
    for row in rows {
        let record = Record {
            version: row.try_get("version")?,
            success: row.try_get("success")?,
            checksum: row.try_get::<String, _>("checksum")?.to_ascii_lowercase(),
        };
        if record.checksum.len() != 96
            || !record.checksum.bytes().all(|byte| byte.is_ascii_hexdigit())
            || records.contains_key(&record.version)
        {
            return Err(failure::coded(
                4,
                "migration_history_invalid",
                "migration history checksum/version metadata invalid",
            ));
        }
        records.insert(record.version, record);
    }
    Ok(History {
        present: true,
        rows: records.into_values().collect(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use sqlx_core::{
        migrate::MigrationType,
        sql_str::{AssertSqlSafe, SqlSafeStr},
    };
    use std::borrow::Cow;

    fn migration(kind: MigrationType, sql: &str) -> Migration {
        Migration::new(
            1,
            "fixture".into(),
            kind,
            AssertSqlSafe(sql.to_owned()).into_sql_str(),
            false,
        )
    }

    fn source(migrations: Vec<Migration>) -> Migrator {
        Migrator {
            migrations: Cow::Owned(migrations),
            ..Migrator::DEFAULT
        }
    }

    #[test]
    fn a_down_source_cannot_substitute_for_the_recorded_up_checksum() {
        let up = migration(MigrationType::ReversibleUp, "SELECT 1");
        let applied = vec![AppliedMigration {
            version: 1,
            checksum: up.checksum.clone(),
        }];
        let down_only = source(vec![migration(MigrationType::ReversibleDown, "SELECT 0")]);
        let error = validate_applied(&down_only, &applied).unwrap_err();
        assert_eq!(
            error.downcast_ref::<failure::Failure>().unwrap().reason,
            Some("migration_source_missing")
        );
        let changed = source(vec![
            migration(MigrationType::ReversibleUp, "SELECT 2"),
            migration(MigrationType::ReversibleDown, "SELECT 0"),
        ]);
        let error = validate_applied(&changed, &applied).unwrap_err();
        assert_eq!(
            error.downcast_ref::<failure::Failure>().unwrap().reason,
            Some("migration_checksum_mismatch")
        );
    }

    #[test]
    fn duplicate_up_sources_are_invalid_but_reversible_pairs_are_allowed() {
        let duplicate = source(vec![
            migration(MigrationType::Simple, "SELECT 1"),
            migration(MigrationType::ReversibleUp, "SELECT 1"),
        ]);
        assert_eq!(
            sources(&duplicate)
                .unwrap_err()
                .downcast_ref::<failure::Failure>()
                .unwrap()
                .code(),
            2
        );
        let pair = source(vec![
            migration(MigrationType::ReversibleUp, "SELECT 1"),
            migration(MigrationType::ReversibleDown, "SELECT 0"),
        ]);
        assert_eq!(sources(&pair).unwrap().len(), 1);
    }

    #[test]
    fn missing_comparison_inputs_never_erase_a_dirty_marker_or_imply_a_match() {
        let history = History {
            present: true,
            rows: vec![Record {
                version: 1,
                success: false,
                checksum: "ab".repeat(48),
            }],
        };
        let unrequested = compare(&history, None).unwrap();
        assert_eq!(unrequested[0].state, SourceState::NotRequested);
        assert!(!unrequested[0].record.success);
        let missing = compare(&history, Some(&source(Vec::new()))).unwrap();
        assert_eq!(missing[0].state, SourceState::Missing);
        assert!(!missing[0].record.success);
    }
}
