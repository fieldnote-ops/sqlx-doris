use super::*;
use sqlx_core::{
    config::macros::PreferredCrates,
    type_checking::{Error as MappingError, FmtValue, ParamChecking, TypeChecking},
};

impl TypeChecking for Doris {
    const PARAM_CHECKING: ParamChecking = ParamChecking::Weak;
    fn param_type_for_id(
        info: &DorisTypeInfo,
        p: &PreferredCrates,
    ) -> Result<&'static str, MappingError> {
        Self::return_type_for_id(info, p)
    }
    fn return_type_for_id(
        info: &DorisTypeInfo,
        _: &PreferredCrates,
    ) -> Result<&'static str, MappingError> {
        let unsigned = info.flags.contains(ColumnFlags::UNSIGNED);
        if let Some(kind) = &info.logical {
            let base = kind
                .split(['(', '<', ' '])
                .next()
                .unwrap_or("")
                .to_ascii_uppercase();
            match base.as_str() {
                "LARGEINT" => return Ok("::sqlx_doris::types::LargeInt"),
                "IPV4" => return Ok("::sqlx_doris::types::Ipv4"),
                "IPV6" => return Ok("::sqlx_doris::types::Ipv6"),
                "VARIANT" => return Ok("::sqlx_doris::types::Variant"),
                "ARRAY" | "MAP" | "STRUCT" | "AGG_STATE" | "BITMAP" | "HLL" => {
                    return Err(MappingError::NoMappingFound);
                }
                "CHAR" | "VARCHAR" | "STRING" | "TEXT" => return Ok("::std::string::String"),
                "JSON" | "JSONB" => {
                    return Ok("::sqlx::types::Json<::sqlx::types::JsonValue>");
                }
                "BOOLEAN" | "BOOL" => return Ok("bool"),
                // Unknown logical types must not fall through to a textual or
                // numeric wire mapping (e.g. opaque aggregate states).
                "TINYINT" | "SMALLINT" | "INT" | "INTEGER" | "BIGINT" | "FLOAT" | "DOUBLE"
                | "DECIMAL" | "DECIMALV2" | "DECIMALV3" | "DATE" | "DATEV2" | "DATETIME"
                | "DATETIMEV2" | "TIME" | "TIMEV2" => {}
                _ => return Err(MappingError::NoMappingFound),
            }
        }
        Ok(match info.r#type {
            ColumnType::Tiny => {
                if unsigned {
                    "u8"
                } else {
                    "i8"
                }
            }
            ColumnType::Short | ColumnType::Year => {
                if unsigned {
                    "u16"
                } else {
                    "i16"
                }
            }
            ColumnType::Long | ColumnType::Int24 => {
                if unsigned {
                    "u32"
                } else {
                    "i32"
                }
            }
            ColumnType::LongLong => {
                if unsigned {
                    "u64"
                } else {
                    "i64"
                }
            }
            ColumnType::Float => "f32",
            ColumnType::Double => "f64",
            ColumnType::Decimal | ColumnType::NewDecimal => "::sqlx_doris::types::BigDecimal",
            ColumnType::Date => "::sqlx::types::chrono::NaiveDate",
            ColumnType::Datetime | ColumnType::Timestamp => "::sqlx::types::chrono::NaiveDateTime",
            ColumnType::Time => "::sqlx_doris::types::Time",
            ColumnType::Json => "::sqlx::types::Json<::sqlx::types::JsonValue>",
            ColumnType::VarChar | ColumnType::VarString => "::std::string::String",
            ColumnType::Blob
            | ColumnType::TinyBlob
            | ColumnType::MediumBlob
            | ColumnType::LongBlob
                if info.charset == 63 =>
            {
                "::std::vec::Vec<u8>"
            }
            // Doris also uses STRING for LARGEINT/complex expressions and BLOB
            // for unbound placeholders. Require the standard SQLx type override
            // rather than generate unchecked decoding from ambiguous metadata.
            _ => return Err(MappingError::NoMappingFound),
        })
    }
    fn get_feature_gate(_: &DorisTypeInfo) -> Option<&'static str> {
        None
    }
    fn fmt_value_debug(value: &DorisValue) -> FmtValue<'_, Self> {
        FmtValue::unknown(value)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn unknown_catalog_types_are_not_silently_mapped_to_strings() {
        let mut info = DorisTypeInfo::text(ColumnType::String);
        info.logical = Some("QUANTILE_STATE".into());
        assert!(Doris::return_type_for_id(&info, &PreferredCrates::default()).is_err());
        info.logical = Some("STRING".into());
        assert_eq!(
            Doris::return_type_for_id(&info, &PreferredCrates::default()).unwrap(),
            "::std::string::String"
        );
        info.logical = Some("ARRAY<INT(11)>".into());
        assert!(Doris::return_type_for_id(&info, &PreferredCrates::default()).is_err());
    }
}
