//! Explicit Doris logical types for the native SQLx Doris driver.
//!
//! ARRAY/MAP/STRUCT bind using Doris's CAST text grammar; JSON/VARIANT bind as
//! JSON text. For VARIANT table writes, the tested 4.1.5 path is `CAST(? AS JSON)`
//! into a VARIANT column. `PARSE_TO_VARIANT(?)` parses an expression, while
//! `CAST(? AS VARIANT)` keeps text as a string. Complex
//! reads containing arbitrary strings need the optional `sqlx_doris::operations::project` helper.
//! Binary uses Hex with UNHEX. Native VARIANT scalar roots and large JSON
//! integers have server-specific limitations; see [`Variant`].

mod complex;
pub(crate) mod json;
mod scalar;
pub(crate) mod text;
mod time;
#[cfg(test)]
mod wire_time;
#[cfg(test)]
pub(crate) use wire_time::{WireTime, WireTimeSign};

pub use bigdecimal::BigDecimal;
pub use complex::{Array, Map, Struct, Variant};
pub use scalar::{Binary, Hex, Ipv4, Ipv6, LargeInt, TimestampTz};
pub use text::TextError;
pub use time::{Time, TimeError};

pub(crate) fn text_compatible(info: &crate::DorisTypeInfo) -> bool {
    <String as sqlx_core::types::Type<crate::Doris>>::compatible(info)
        || <sqlx_core::types::Json<serde_json::Value> as sqlx_core::types::Type<crate::Doris>>::compatible(info)
}
