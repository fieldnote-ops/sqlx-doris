use crate::Result;
use crate::sqlx::{self, MySql, QueryBuilder};

use super::{
    DataType,
    schema::{identifier, validate_identifier},
};

/// Append a VARIANT column/path projection, binding each path component.
///
/// The column is a literal identifier, not a SQL expression. An empty path
/// selects the root. `DataType::Json` preserves JSON scalar string/number/bool
/// distinctions for decoding with `Variant`; an explicit scalar type selects
/// Doris's CAST semantics. Missing paths and stored nulls may both be SQL NULL.
/// Server normalization, including dotted field names, cannot be reversed here.
/// For complex outputs use the schema projection separately; this helper emits
/// a CAST, not a promise of lossless native collection display.
///
/// ```
/// use sqlx_doris::{operations::{DataType, push_variant_path}, sqlx};
/// let mut query = sqlx::QueryBuilder::<sqlx::MySql>::new("SELECT ");
/// push_variant_path(&mut query, "payload", &["user", "id"], &DataType::BigInt)?;
/// assert_eq!(query.sql(), "SELECT CAST(`payload`[?][?] AS BIGINT)");
/// # Ok::<(), sqlx_doris::Error>(())
/// ```
pub fn push_variant_path(
    builder: &mut QueryBuilder<MySql>,
    column: &str,
    path: &[&str],
    output: &DataType,
) -> Result<()> {
    validate_identifier(column)?;
    output.validate()?;
    builder.push("CAST(").push(identifier(column));
    for component in path {
        builder
            .push("[")
            .push_bind((*component).to_owned())
            .push("]");
    }
    builder.push(" AS ").push(output).push(")");
    Ok(())
}
