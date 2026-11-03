use std::{fmt, str::FromStr};

use crate::{Doris, DorisTypeInfo, DorisValueRef};
use serde::{Deserialize, Serialize};
use sqlx_core::{
    decode::Decode,
    encode::{Encode, IsNull},
    error::BoxDynError,
    types::Type,
};

/// Invalid TIME syntax or a duration outside Doris's supported range.
#[derive(Clone, Debug, Eq, PartialEq, thiserror::Error)]
#[error("{0}")]
pub struct TimeError(&'static str);

/// A signed Doris TIME duration, including the full microsecond boundary.
///
/// Binds as a typed duration through the checked Doris text-protocol path,
/// retaining microseconds at both fractional boundaries. TIME cannot be stored
/// in Doris OLAP columns. Explicit STRING projections are also supported.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
#[serde(try_from = "String", into = "String")]
pub struct Time(i64);

impl Time {
    /// Largest absolute duration in microseconds: 838:59:59.999999.
    pub const MAX_MICROSECONDS: i64 = 3_020_399_999_999;

    /// Construct from signed microseconds, rejecting values outside Doris's range.
    pub fn from_microseconds(value: i64) -> Result<Self, TimeError> {
        if value.unsigned_abs() > Self::MAX_MICROSECONDS as u64 {
            return Err(TimeError("TIME exceeds +/-838:59:59.999999"));
        }
        Ok(Self(value))
    }

    /// Signed duration in microseconds; this is not a time of day.
    pub fn as_microseconds(self) -> i64 {
        self.0
    }
}

impl FromStr for Time {
    type Err = TimeError;

    fn from_str(value: &str) -> Result<Self, TimeError> {
        let invalid = || TimeError("TIME requires [-]HH:MM:SS[.ffffff] with microsecond precision");
        let (negative, value) = value
            .strip_prefix('-')
            .map_or((false, value), |v| (true, v));
        let mut fields = value.split(':');
        let hours = fields.next().ok_or_else(invalid)?;
        let minutes = fields.next().ok_or_else(invalid)?;
        let seconds = fields.next().ok_or_else(invalid)?;
        if fields.next().is_some() {
            return Err(invalid());
        }
        let (seconds, fraction) = seconds
            .split_once('.')
            .map_or((seconds, None), |(s, f)| (s, Some(f)));
        if hours.is_empty()
            || hours.len() > 3
            || minutes.len() != 2
            || seconds.len() != 2
            || ![hours, minutes, seconds]
                .iter()
                .all(|v| v.bytes().all(|b| b.is_ascii_digit()))
            || fraction.is_some_and(|v| {
                v.is_empty() || v.len() > 6 || !v.bytes().all(|b| b.is_ascii_digit())
            })
        {
            return Err(invalid());
        }
        let hours: i64 = hours.parse().map_err(|_| invalid())?;
        let minutes: i64 = minutes.parse().map_err(|_| invalid())?;
        let seconds: i64 = seconds.parse().map_err(|_| invalid())?;
        if hours > 838 || minutes > 59 || seconds > 59 {
            return Err(invalid());
        }
        let micros = fraction
            .map_or(Ok(0), |v| {
                v.parse::<i64>().map(|n| n * 10_i64.pow(6 - v.len() as u32))
            })
            .map_err(|_| invalid())?;
        let total = ((hours * 60 + minutes) * 60 + seconds) * 1_000_000 + micros;
        Self::from_microseconds(if negative { -total } else { total })
    }
}
impl TryFrom<String> for Time {
    type Error = TimeError;
    fn try_from(value: String) -> Result<Self, TimeError> {
        value.parse()
    }
}
impl From<Time> for String {
    fn from(value: Time) -> Self {
        value.to_string()
    }
}
impl fmt::Display for Time {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let magnitude = self.0.unsigned_abs();
        let seconds = magnitude / 1_000_000;
        write!(
            f,
            "{}{:02}:{:02}:{:02}.{:06}",
            if self.0 < 0 { "-" } else { "" },
            seconds / 3600,
            seconds / 60 % 60,
            seconds % 60,
            magnitude % 1_000_000
        )
    }
}
impl Type<Doris> for Time {
    fn type_info() -> DorisTypeInfo {
        DorisTypeInfo::binary(crate::driver::ColumnType::Time)
    }
    fn compatible(info: &DorisTypeInfo) -> bool {
        <String as Type<Doris>>::compatible(info) || info.r#type == crate::driver::ColumnType::Time
    }
}
impl Decode<'_, Doris> for Time {
    fn decode(value: DorisValueRef<'_>) -> Result<Self, BoxDynError> {
        Ok(<String as Decode<Doris>>::decode(value)?.parse()?)
    }
}
impl Encode<'_, Doris> for Time {
    fn encode_by_ref(&self, buf: &mut Vec<u8>) -> Result<IsNull, BoxDynError> {
        let n = self.0.unsigned_abs();
        if n == 0 {
            buf.push(0);
        } else {
            buf.extend_from_slice(&[12, u8::from(self.0 < 0)]);
            buf.extend_from_slice(&((n / 86_400_000_000) as u32).to_le_bytes());
            buf.extend_from_slice(&[
                (n / 3_600_000_000 % 24) as u8,
                (n / 60_000_000 % 60) as u8,
                (n / 1_000_000 % 60) as u8,
            ]);
            buf.extend_from_slice(&((n % 1_000_000) as u32).to_le_bytes());
        }
        Ok(IsNull::No)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn signed_duration_boundaries_and_precision_are_lossless() {
        for value in [
            0,
            -1,
            1,
            90_123_123_456,
            -445_506_123_456,
            Time::MAX_MICROSECONDS,
            -Time::MAX_MICROSECONDS,
        ] {
            let time = Time::from_microseconds(value).unwrap();
            assert_eq!(
                time.to_string().parse::<Time>().unwrap().as_microseconds(),
                value
            );
            assert_eq!(
                serde_json::from_str::<Time>(&serde_json::to_string(&time).unwrap()).unwrap(),
                time
            );
        }
        for value in [
            i64::MIN,
            i64::MAX,
            Time::MAX_MICROSECONDS + 1,
            -Time::MAX_MICROSECONDS - 1,
        ] {
            assert!(Time::from_microseconds(value).is_err());
        }
        for value in [
            "839:00:00",
            "01:60:00",
            "01:00:60",
            "01:00:00.1234567",
            "01:00:00.",
            "01:00:00.12x",
            "01:00:00:01",
            "01:0:00",
            " 01:00:00",
            "'01:00:00'",
        ] {
            assert!(value.parse::<Time>().is_err(), "{value}");
        }
        assert_eq!("-00:00:00".parse::<Time>().unwrap().as_microseconds(), 0);
        assert_eq!(
            "25:02:03.1".parse::<Time>().unwrap().to_string(),
            "25:02:03.100000"
        );
    }
}
