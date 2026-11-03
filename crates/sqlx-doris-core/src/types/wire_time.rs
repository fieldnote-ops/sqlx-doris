use crate::driver::ColumnType;

use crate::{Doris, DorisTypeInfo, DorisValueFormat, DorisValueRef};
use sqlx_core::{
    decode::Decode,
    encode::{Encode, IsNull},
    error::BoxDynError,
    types::Type,
};
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum WireTimeSign {
    Negative,
    Positive,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct WireTime(i64);
impl WireTime {
    pub(crate) const ZERO: Self = Self(0);
    pub(crate) const MAX: Self = Self(3_020_399_000_000);
    #[cfg(test)]
    pub(crate) const MIN: Self = Self(-3_020_399_000_000);
    pub(crate) fn new(
        sign: WireTimeSign,
        h: u32,
        m: u8,
        s: u8,
        us: u32,
    ) -> Result<Self, BoxDynError> {
        if h > 838 || m > 59 || s > 59 || us > 999999 {
            return Err("invalid TIME fields".into());
        }
        let n =
            ((i64::from(h) * 60 + i64::from(m)) * 60 + i64::from(s)) * 1_000_000 + i64::from(us);
        if n > Self::MAX.0 || (n == 0 && sign == WireTimeSign::Negative) {
            return Err("invalid native TIME boundary".into());
        }
        Ok(Self(if sign == WireTimeSign::Negative {
            -n
        } else {
            n
        }))
    }
}
impl std::fmt::Display for WireTime {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let n = self.0.unsigned_abs();
        write!(
            f,
            "{}{:02}:{:02}:{:02}.{:06}",
            if self.0 < 0 { "-" } else { "" },
            n / 3_600_000_000,
            n / 60_000_000 % 60,
            n / 1_000_000 % 60,
            n % 1_000_000
        )
    }
}
impl Type<Doris> for WireTime {
    fn type_info() -> DorisTypeInfo {
        DorisTypeInfo::binary(ColumnType::Time)
    }
}
impl Encode<'_, Doris> for WireTime {
    fn encode_by_ref(&self, b: &mut Vec<u8>) -> Result<IsNull, BoxDynError> {
        let n = self.0.unsigned_abs();
        if n == 0 {
            b.push(0);
        } else {
            b.extend_from_slice(&[12, u8::from(self.0 < 0)]);
            b.extend_from_slice(&((n / 86_400_000_000) as u32).to_le_bytes());
            b.extend_from_slice(&[
                (n / 3_600_000_000 % 24) as u8,
                (n / 60_000_000 % 60) as u8,
                (n / 1_000_000 % 60) as u8,
            ]);
            b.extend_from_slice(&((n % 1_000_000) as u32).to_le_bytes());
        }
        Ok(IsNull::No)
    }
}
impl Decode<'_, Doris> for WireTime {
    fn decode(v: DorisValueRef<'_>) -> Result<Self, BoxDynError> {
        if v.format != DorisValueFormat::Text {
            return Err("binary TIME requires explicit text route".into());
        }
        let t = v.text()?.parse::<super::Time>()?;
        let n = t.as_microseconds();
        if n.unsigned_abs() > Self::MAX.0 as u64 {
            return Err("native TIME overflow".into());
        }
        Ok(Self(n))
    }
}
