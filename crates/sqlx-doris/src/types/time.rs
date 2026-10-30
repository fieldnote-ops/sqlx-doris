use std::{fmt, str::FromStr};

use serde::{Deserialize, Serialize};
use crate::sqlx::{self,
    Decode, Encode, MySql, Type,
    encode::IsNull,
    error::BoxDynError,
    mysql::{MySqlTypeInfo, MySqlValueRef},
};

/// A signed Doris TIME duration, including the full microsecond boundary.
///
/// Bind to `CAST(? AS TIME(6))`. The configured Doris profile selects lossless
/// text results before executing a TIME projection, including this type's full
/// fractional boundary. Explicit STRING projections remain supported. Without
/// the profile, binary TIME is rejected because Doris truncates fractions above
/// +/-838:59:59 before sending them. Native `MySqlTime` retains MySQL's narrower
/// +/-838:59:59.000000 range. TIME cannot be stored in Doris OLAP columns.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
#[serde(try_from = "String", into = "String")]
pub struct Time(i64);

impl Time {
    /// Largest absolute duration in microseconds: 838:59:59.999999.
    pub const MAX_MICROSECONDS: i64 = 3_020_399_999_999;

    /// Construct from signed microseconds, rejecting values outside Doris's range.
    pub fn from_microseconds(value: i64) -> crate::Result<Self> {
        if value.unsigned_abs() > Self::MAX_MICROSECONDS as u64 {
            return Err(crate::Error::InvalidInput(
                "TIME exceeds +/-838:59:59.999999".into(),
            ));
        }
        Ok(Self(value))
    }

    /// Signed duration in microseconds; this is not a time of day.
    pub fn as_microseconds(self) -> i64 {
        self.0
    }
}

impl FromStr for Time {
    type Err = crate::Error;

    fn from_str(value: &str) -> crate::Result<Self> {
        let invalid = || {
            crate::Error::InvalidInput(
                "TIME requires [-]HH:MM:SS[.ffffff] with microsecond precision".into(),
            )
        };
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
    type Error = crate::Error;
    fn try_from(value: String) -> crate::Result<Self> {
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
impl Type<MySql> for Time {
    fn type_info() -> MySqlTypeInfo {
        <String as Type<MySql>>::type_info()
    }
    fn compatible(info: &MySqlTypeInfo) -> bool {
        <String as Type<MySql>>::compatible(info)
            || <sqlx::mysql::types::MySqlTime as Type<MySql>>::compatible(info)
    }
}
impl Decode<'_, MySql> for Time {
    fn decode(value: MySqlValueRef<'_>) -> Result<Self, BoxDynError> {
        Ok(<String as Decode<MySql>>::decode(value)?.parse()?)
    }
}
impl Encode<'_, MySql> for Time {
    fn encode_by_ref(&self, buf: &mut Vec<u8>) -> Result<IsNull, BoxDynError> {
        <String as Encode<MySql>>::encode_by_ref(&self.to_string(), buf)
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
