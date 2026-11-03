use super::*;
use sqlx_macros_core::database::{CachingDescribeBlocking, DatabaseExt};

impl DatabaseExt for Doris {
    const DATABASE_PATH: &'static str = "::sqlx_doris::Doris";
    const ROW_PATH: &'static str = "::sqlx_doris::DorisRow";
    fn describe_blocking(
        query: &str,
        url: &str,
        config: &sqlx_core::config::drivers::Config,
    ) -> sqlx_core::Result<sqlx_core::describe::Describe<Self>> {
        static CACHE: CachingDescribeBlocking<Doris> = CachingDescribeBlocking::new();
        let mut data = CACHE.describe(query, url, config)?;
        // Doris PREPARE can return original column names for direct projections,
        // omitting SQL aliases. Restore only unambiguously positioned aliases.
        if let Some(names) = crate::sql::projection::projection_names(query)
            .ok()
            .flatten()
            && names.len() == data.columns.len()
        {
            for (column, name) in data.columns.iter_mut().zip(names) {
                if let Some(name) = name {
                    column.name = name;
                }
            }
        }
        Ok(data)
    }
}
