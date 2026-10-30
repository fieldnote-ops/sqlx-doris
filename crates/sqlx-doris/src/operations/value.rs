use crate::sqlx::{MySql, QueryBuilder};
use serde::Serialize;

use super::DataType;
use crate::{Error, Result, types::text};

/// Append an ARRAY/MAP/STRUCT expression whose scalar leaves are SQLx binds.
///
/// This avoids the older Doris CAST text parser's handling of escaped quotes.
/// Values never enter SQL text. A final CAST supplies empty-collection types and
/// STRUCT field names; source records must contain exactly the schema's fields.
/// The serde representation of None / unit becomes SQL NULL at every level.
/// JSON, VARIANT, VARBINARY and opaque aggregation-state leaves are unsupported;
/// use their explicit SQL encodings separately. Use [`super::project`] to read
/// the complex value back. Discard the builder if this function returns an error.
///
/// ```
/// use sqlx_doris::{operations::{DataType, push_complex_value}, types::Array, sqlx};
/// let mut query = sqlx::QueryBuilder::<sqlx::MySql>::new("SELECT ");
/// push_complex_value(&mut query, &DataType::array(DataType::String),
///     &Array(vec![Some("quoted \" and NUL \0"), None]))?;
/// assert_eq!(query.sql(), "SELECT CAST(ARRAY(CAST(? AS STRING),CAST(? AS STRING)) AS ARRAY<STRING>)");
/// # Ok::<(), sqlx_doris::Error>(())
/// ```
pub fn push_complex_value<T: Serialize>(
    builder: &mut QueryBuilder<MySql>,
    data_type: &DataType,
    value: &T,
) -> Result<()> {
    if !matches!(
        data_type,
        DataType::Array(_) | DataType::Map(_, _) | DataType::Struct(_)
    ) {
        return Err(Error::InvalidInput(
            "a complex value requires ARRAY, MAP or STRUCT schema".into(),
        ));
    }
    data_type.validate()?;
    let value = text::to_value(value).map_err(|e| Error::InvalidInput(e.to_string()))?;
    append(builder, data_type, value)
}

fn append(builder: &mut QueryBuilder<MySql>, kind: &DataType, value: text::Value) -> Result<()> {
    use text::Value as V;
    if matches!(
        kind,
        DataType::Json
            | DataType::Variant
            | DataType::VariantSchema { .. }
            | DataType::VarBinary
            | DataType::Bitmap
            | DataType::Hll
            | DataType::QuantileState
            | DataType::AggState { .. }
    ) {
        return Err(Error::Unsupported(
            "this leaf type needs an explicit SQL encoding",
        ));
    }
    builder.push("CAST(");
    match (kind, value) {
        (_, V::Null) => {
            builder.push_bind(Option::<String>::None);
        }
        (DataType::Array(inner), V::Array(values)) => {
            builder.push("ARRAY(");
            for (index, value) in values.into_iter().enumerate() {
                if index > 0 {
                    builder.push(",");
                }
                append(builder, inner, value)?;
            }
            builder.push(")");
        }
        (DataType::Map(key, value_kind), V::Map(entries)) => {
            builder.push("MAP(");
            for (index, (k, v)) in entries.into_iter().enumerate() {
                if index > 0 {
                    builder.push(",");
                }
                append(builder, key, k)?;
                builder.push(",");
                append(builder, value_kind, v)?;
            }
            builder.push(")");
        }
        (DataType::Struct(fields), V::Map(mut values)) => {
            builder.push("STRUCT(");
            for (index, (name, kind)) in fields.iter().enumerate() {
                let position = values
                    .iter()
                    .position(|(k, _)| matches!(k, V::String(s) if s == name))
                    .ok_or_else(|| Error::InvalidInput(format!("missing STRUCT field {name}")))?;
                let (_, value) = values.remove(position);
                if index > 0 {
                    builder.push(",");
                }
                append(builder, kind, value)?;
            }
            if !values.is_empty() {
                return Err(Error::InvalidInput(
                    "unknown or duplicate STRUCT fields".into(),
                ));
            }
            builder.push(")");
        }
        (DataType::Array(_) | DataType::Map(_, _) | DataType::Struct(_), _) => {
            return Err(Error::InvalidInput(
                "value does not match the complex schema".into(),
            ));
        }
        (_, V::String(value)) => {
            builder.push_bind(value);
        }
        (_, V::Number(value)) => {
            builder.push_bind(value.to_string());
        }
        (_, V::Bool(value)) => {
            builder.push_bind(value);
        }
        _ => {
            return Err(Error::InvalidInput(
                "a scalar leaf requires a scalar value".into(),
            ));
        }
    }
    builder.push(" AS ").push(kind).push(")");
    Ok(())
}
