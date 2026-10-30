use std::{
    fmt,
    net::{Ipv4Addr, Ipv6Addr},
    ops::Deref,
};

use chrono::{DateTime, FixedOffset, SecondsFormat};
use serde::{Deserialize, Serialize};
use crate::sqlx::{
    Decode, Encode, MySql, Type,
    encode::IsNull,
    error::BoxDynError,
    mysql::{MySqlTypeInfo, MySqlValueRef},
};

/// Signed 128-bit integer. MySQL can store it in DECIMAL(39,0), Doris in LARGEINT.
#[derive(
    Clone, Copy, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize, Deserialize,
)]
#[serde(transparent)]
pub struct LargeInt(pub i128);

impl From<i128> for LargeInt {
    fn from(value: i128) -> Self {
        Self(value)
    }
}
impl Deref for LargeInt {
    type Target = i128;
    fn deref(&self) -> &i128 {
        &self.0
    }
}
impl fmt::Display for LargeInt {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}
impl Type<MySql> for LargeInt {
    fn type_info() -> MySqlTypeInfo {
        <String as Type<MySql>>::type_info()
    }
    fn compatible(info: &MySqlTypeInfo) -> bool {
        <String as Type<MySql>>::compatible(info)
            || <bigdecimal::BigDecimal as Type<MySql>>::compatible(info)
    }
}
impl Decode<'_, MySql> for LargeInt {
    fn decode(value: MySqlValueRef<'_>) -> Result<Self, BoxDynError> {
        Ok(Self(<String as Decode<MySql>>::decode(value)?.parse()?))
    }
}
impl Encode<'_, MySql> for LargeInt {
    fn encode_by_ref(&self, buf: &mut Vec<u8>) -> Result<IsNull, BoxDynError> {
        <String as Encode<MySql>>::encode_by_ref(&self.to_string(), buf)
    }
}

/// A timestamp with an explicit UTC offset. Encoding/serialization rejects
/// submicrosecond precision and leap seconds instead of silently truncating.
#[derive(Clone, Debug, Eq, PartialEq, Deserialize)]
#[serde(transparent)]
pub struct TimestampTz(pub DateTime<FixedOffset>);

impl TimestampTz {
    /// Parse RFC3339, also accepting Doris's space date separator.
    pub fn parse(value: &str) -> Result<Self, chrono::ParseError> {
        let normalized = if value.as_bytes().get(10) == Some(&b' ') {
            value.replacen(' ', "T", 1)
        } else {
            value.to_owned()
        };
        DateTime::parse_from_rfc3339(&normalized).map(Self)
    }
    fn validate_precision(&self) -> Result<(), &'static str> {
        let nanos = self.0.timestamp_subsec_nanos();
        if nanos >= 1_000_000_000 || !nanos.is_multiple_of(1000) {
            return Err(
                "Doris TIMESTAMPTZ requires microsecond precision and does not support leap seconds",
            );
        }
        Ok(())
    }
}
impl Serialize for TimestampTz {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        self.validate_precision()
            .map_err(serde::ser::Error::custom)?;
        serializer.collect_str(self)
    }
}
impl fmt::Display for TimestampTz {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0.to_rfc3339_opts(SecondsFormat::AutoSi, false))
    }
}
impl Deref for TimestampTz {
    type Target = DateTime<FixedOffset>;
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}
impl Type<MySql> for TimestampTz {
    fn type_info() -> MySqlTypeInfo {
        <String as Type<MySql>>::type_info()
    }
    fn compatible(info: &MySqlTypeInfo) -> bool {
        <String as Type<MySql>>::compatible(info)
    }
}
impl Decode<'_, MySql> for TimestampTz {
    fn decode(value: MySqlValueRef<'_>) -> Result<Self, BoxDynError> {
        Ok(Self::parse(&<String as Decode<MySql>>::decode(value)?)?)
    }
}
impl Encode<'_, MySql> for TimestampTz {
    fn encode_by_ref(&self, buf: &mut Vec<u8>) -> Result<IsNull, BoxDynError> {
        self.validate_precision()
            .map_err(|message| std::io::Error::new(std::io::ErrorKind::InvalidInput, message))?;
        <String as Encode<MySql>>::encode_by_ref(&self.to_string(), buf)
    }
}

/// Native Doris IPv4, encoded as an address string and decoded to std::net.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq, Serialize, Deserialize)]
#[serde(transparent)]
pub struct Ipv4(pub Ipv4Addr);

/// Native Doris IPv6, encoded as an address string and decoded to std::net.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq, Serialize, Deserialize)]
#[serde(transparent)]
pub struct Ipv6(pub Ipv6Addr);

macro_rules! ip_type {
    ($name:ident,$inner:ty) => {
        impl From<$inner> for $name {
            fn from(value: $inner) -> Self {
                Self(value)
            }
        }
        impl Deref for $name {
            type Target = $inner;
            fn deref(&self) -> &Self::Target {
                &self.0
            }
        }
        impl fmt::Display for $name {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                self.0.fmt(f)
            }
        }
        impl Type<MySql> for $name {
            fn type_info() -> MySqlTypeInfo {
                <String as Type<MySql>>::type_info()
            }
            fn compatible(info: &MySqlTypeInfo) -> bool {
                <String as Type<MySql>>::compatible(info)
            }
        }
        impl Decode<'_, MySql> for $name {
            fn decode(value: MySqlValueRef<'_>) -> Result<Self, BoxDynError> {
                Ok(Self(<String as Decode<MySql>>::decode(value)?.parse()?))
            }
        }
        impl Encode<'_, MySql> for $name {
            fn encode_by_ref(&self, buf: &mut Vec<u8>) -> Result<IsNull, BoxDynError> {
                <String as Encode<MySql>>::encode_by_ref(&self.to_string(), buf)
            }
        }
    };
}
ip_type!(Ipv4, Ipv4Addr);
ip_type!(Ipv6, Ipv6Addr);

/// Lossless binary result. Bind `Hex(&bytes)` to an explicit `UNHEX(?)` instead
/// of using the BLOB parameter type rejected by the Doris 4.1.5 server.
#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
#[serde(transparent)]
pub struct Binary(pub Vec<u8>);

impl Binary {
    /// Borrow these bytes as a hexadecimal string parameter for UNHEX(?).
    pub fn as_hex(&self) -> Hex<'_> {
        Hex(&self.0)
    }
}
impl Deref for Binary {
    type Target = [u8];
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}
impl From<Vec<u8>> for Binary {
    fn from(value: Vec<u8>) -> Self {
        Self(value)
    }
}
impl Type<MySql> for Binary {
    fn type_info() -> MySqlTypeInfo {
        <Vec<u8> as Type<MySql>>::type_info()
    }
    fn compatible(info: &MySqlTypeInfo) -> bool {
        <Vec<u8> as Type<MySql>>::compatible(info)
    }
}
impl Decode<'_, MySql> for Binary {
    fn decode(value: MySqlValueRef<'_>) -> Result<Self, BoxDynError> {
        <Vec<u8> as Decode<MySql>>::decode(value).map(Self)
    }
}

/// Borrowed binary input for SQL `UNHEX(?)`; never substitutes SQL text.
#[derive(Clone, Copy, Debug)]
pub struct Hex<'a>(pub &'a [u8]);

impl fmt::Display for Hex<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        for b in self.0 {
            write!(f, "{b:02x}")?;
        }
        Ok(())
    }
}
impl Type<MySql> for Hex<'_> {
    fn type_info() -> MySqlTypeInfo {
        <String as Type<MySql>>::type_info()
    }
}
impl Encode<'_, MySql> for Hex<'_> {
    fn encode_by_ref(&self, buf: &mut Vec<u8>) -> Result<IsNull, BoxDynError> {
        <String as Encode<MySql>>::encode_by_ref(&self.to_string(), buf)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::Array;
    #[test]
    fn timestamp_encoding_never_silently_loses_submicroseconds_or_leap_seconds() {
        for raw in ["2026-10-26T12:34:56.123456789Z", "2016-12-31T23:59:60Z"] {
            let timestamp = TimestampTz::parse(raw).unwrap();
            assert!(
                <TimestampTz as Encode<MySql>>::encode_by_ref(&timestamp, &mut vec![]).is_err()
            );
            assert!(serde_json::to_string(&timestamp).is_err());
            assert!(
                <Array<_> as Encode<MySql>>::encode_by_ref(&Array(vec![timestamp]), &mut vec![])
                    .is_err()
            );
        }
        let timestamp = TimestampTz::parse("2026-10-26 12:34:56.123456+08:00").unwrap();
        assert_eq!(
            serde_json::from_str::<TimestampTz>(&serde_json::to_string(&timestamp).unwrap())
                .unwrap(),
            timestamp
        );
        assert!(<TimestampTz as Encode<MySql>>::encode_by_ref(&timestamp, &mut vec![]).is_ok());
        assert!(
            TimestampTz::parse("2026-10-26T12:34:56.123456789Z")
                .unwrap()
                .to_string()
                .contains("123456789")
        );
    }
}
