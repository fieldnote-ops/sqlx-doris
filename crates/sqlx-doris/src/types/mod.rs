//! Explicit Doris logical types over SQLx's existing MySQL wire driver.
//!
//! ARRAY/MAP/STRUCT bind using Doris's CAST text grammar; JSON/VARIANT bind as
//! JSON text. For VARIANT table writes, the tested 4.1.5 path is `CAST(? AS JSON)`
//! into a VARIANT column. `PARSE_TO_VARIANT(?)` parses an expression, while
//! `CAST(? AS VARIANT)` keeps text as a string. Complex
//! reads containing arbitrary strings need [`crate::operations::project`].
//! Binary uses Hex with UNHEX. Native VARIANT scalar roots and large JSON
//! integers have server-specific limitations; see [`Variant`].

mod complex;
pub(crate) mod json;
mod scalar;
pub(crate) mod text;
mod time;

pub use bigdecimal::BigDecimal;
pub use complex::{Array, Map, Struct, Variant};
pub use scalar::{Binary, Hex, Ipv4, Ipv6, LargeInt, TimestampTz};
pub use text::TextError;
pub use time::Time;

pub(crate) fn text_compatible(info: &sqlx::mysql::MySqlTypeInfo) -> bool {
    <String as sqlx::Type<sqlx::MySql>>::compatible(info)
        || <sqlx::types::Json<serde_json::Value> as sqlx::Type<sqlx::MySql>>::compatible(info)
}
