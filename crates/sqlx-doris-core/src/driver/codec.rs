use super::*;
use chrono::{Datelike, Timelike};
use sqlx_core::{
    arguments::Arguments,
    decode::Decode,
    encode::{Encode, IsNull},
    error::BoxDynError,
    types::{Json, Type},
};

pub(crate) trait LenEnc {
    fn put_bytes_lenenc(&mut self, value: &[u8]);
}
impl LenEnc for Vec<u8> {
    fn put_bytes_lenenc(&mut self, value: &[u8]) {
        put_len(self, value.len() as u64);
        self.extend_from_slice(value);
    }
}
pub(crate) fn put_len(out: &mut Vec<u8>, n: u64) {
    match n {
        0..=250 => out.push(n as u8),
        251..=65535 => {
            out.push(252);
            out.extend_from_slice(&(n as u16).to_le_bytes());
        }
        65536..=16777215 => {
            out.push(253);
            out.extend_from_slice(&n.to_le_bytes()[..3]);
        }
        _ => {
            out.push(254);
            out.extend_from_slice(&n.to_le_bytes());
        }
    }
}
pub(crate) struct Cursor<'a>(pub(crate) &'a [u8]);
impl<'a> Cursor<'a> {
    pub(crate) fn take(&mut self, n: usize) -> Result<&'a [u8], Error> {
        if n > self.0.len() {
            return Err(protocol("truncated packet"));
        }
        let (a, b) = self.0.split_at(n);
        self.0 = b;
        Ok(a)
    }
    pub(crate) fn byte(&mut self) -> Result<u8, Error> {
        Ok(self.take(1)?[0])
    }
    pub(crate) fn u16(&mut self) -> Result<u16, Error> {
        Ok(u16::from_le_bytes(self.take(2)?.try_into().unwrap()))
    }
    pub(crate) fn u32(&mut self) -> Result<u32, Error> {
        Ok(u32::from_le_bytes(self.take(4)?.try_into().unwrap()))
    }
    pub(crate) fn nul(&mut self) -> Result<&'a [u8], Error> {
        let n = self
            .0
            .iter()
            .position(|b| *b == 0)
            .ok_or_else(|| protocol("missing terminator"))?;
        let s = self.take(n)?;
        self.take(1)?;
        Ok(s)
    }
    pub(crate) fn len(&mut self) -> Result<u64, Error> {
        Ok(match self.byte()? {
            252 => u64::from(self.u16()?),
            253 => {
                let b = self.take(3)?;
                u64::from(u32::from_le_bytes([b[0], b[1], b[2], 0]))
            }
            254 => u64::from_le_bytes(self.take(8)?.try_into().unwrap()),
            n @ 0..=250 => u64::from(n),
            _ => return Err(protocol("invalid length marker")),
        })
    }
    pub(crate) fn bytes(&mut self) -> Result<&'a [u8], Error> {
        let n = usize::try_from(self.len()?).map_err(|_| protocol("length overflow"))?;
        self.take(n)
    }
}
/// Typed parameters encoded once into a contiguous binary buffer.
#[derive(Debug, Default, Clone)]
pub struct DorisArguments {
    pub(crate) values: Vec<u8>,
    pub(crate) types: Vec<DorisTypeInfo>,
    pub(crate) null_bitmap: Vec<u8>,
}
impl DorisArguments {
    pub(crate) fn add<'q, T: Encode<'q, Doris> + Type<Doris>>(
        &mut self,
        value: T,
    ) -> Result<(), BoxDynError> {
        let ty = value.produces().unwrap_or_else(T::type_info);
        let start = self.values.len();
        let null = match value.encode(&mut self.values) {
            Ok(n) => n,
            Err(e) => {
                self.values.truncate(start);
                return Err(e);
            }
        };
        let i = self.types.len();
        if i.is_multiple_of(8) {
            self.null_bitmap.push(0);
        }
        if null.is_null() {
            self.null_bitmap[i / 8] |= 1 << (i % 8);
        }
        self.types.push(ty);
        Ok(())
    }
}
impl Arguments for DorisArguments {
    type Database = Doris;
    fn reserve(&mut self, len: usize, size: usize) {
        self.types.reserve(len);
        self.values.reserve(size);
        self.null_bitmap.reserve(len.div_ceil(8));
    }
    fn add<'q, T: Encode<'q, Doris> + Type<Doris>>(&mut self, value: T) -> Result<(), BoxDynError> {
        self.add(value)
    }
    fn len(&self) -> usize {
        self.types.len()
    }
}
fn numeric(info: &DorisTypeInfo) -> bool {
    matches!(
        info.r#type,
        ColumnType::Tiny
            | ColumnType::Short
            | ColumnType::Long
            | ColumnType::Int24
            | ColumnType::LongLong
            | ColumnType::Year
    )
}
fn integer(value: &DorisValueRef<'_>) -> Result<i128, BoxDynError> {
    if value.format == DorisValueFormat::Text {
        return Ok(value.text()?.parse()?);
    }
    let b = value.bytes()?;
    let mut raw = [0u8; 16];
    if !matches!(b.len(), 1 | 2 | 4 | 8) {
        return Err("invalid binary integer width".into());
    }
    if !value.type_info.flags.contains(ColumnFlags::UNSIGNED) && b[b.len() - 1] & 128 != 0 {
        raw.fill(255);
    }
    raw[..b.len()].copy_from_slice(b);
    Ok(i128::from_le_bytes(raw))
}
macro_rules! int {
    ($t:ty,$kind:ident,$unsigned:expr) => {
        impl Type<Doris> for $t {
            fn type_info() -> DorisTypeInfo {
                let mut v = DorisTypeInfo::binary(ColumnType::$kind);
                if $unsigned {
                    v.flags |= ColumnFlags::UNSIGNED;
                }
                v
            }
            fn compatible(v: &DorisTypeInfo) -> bool {
                numeric(v)
            }
        }
        impl Encode<'_, Doris> for $t {
            fn encode_by_ref(&self, b: &mut Vec<u8>) -> Result<IsNull, BoxDynError> {
                b.extend_from_slice(&self.to_le_bytes());
                Ok(IsNull::No)
            }
        }
        impl Decode<'_, Doris> for $t {
            fn decode(v: DorisValueRef<'_>) -> Result<Self, BoxDynError> {
                Ok(Self::try_from(integer(&v)?)?)
            }
        }
    };
}
int!(i8, Tiny, false);
int!(i16, Short, false);
int!(i32, Long, false);
int!(i64, LongLong, false);
int!(u8, Tiny, true);
int!(u16, Short, true);
int!(u32, Long, true);
int!(u64, LongLong, true);
impl Type<Doris> for bool {
    fn type_info() -> DorisTypeInfo {
        <i8 as Type<Doris>>::type_info()
    }
    fn compatible(v: &DorisTypeInfo) -> bool {
        numeric(v) || v.r#type == ColumnType::Bit
    }
}
impl Encode<'_, Doris> for bool {
    fn encode_by_ref(&self, b: &mut Vec<u8>) -> Result<IsNull, BoxDynError> {
        b.push(u8::from(*self));
        Ok(IsNull::No)
    }
}
impl Decode<'_, Doris> for bool {
    fn decode(v: DorisValueRef<'_>) -> Result<Self, BoxDynError> {
        if v.type_info.r#type == ColumnType::Bit {
            return Ok(v.bytes()?.iter().any(|b| *b != 0));
        }
        Ok(integer(&v)? != 0)
    }
}
macro_rules! float {
    ($t:ty,$kind:ident,$len:expr) => {
        impl Type<Doris> for $t {
            fn type_info() -> DorisTypeInfo {
                DorisTypeInfo::binary(ColumnType::$kind)
            }
            fn compatible(v: &DorisTypeInfo) -> bool {
                matches!(v.r#type, ColumnType::Float | ColumnType::Double)
            }
        }
        impl Encode<'_, Doris> for $t {
            fn encode_by_ref(&self, b: &mut Vec<u8>) -> Result<IsNull, BoxDynError> {
                if !self.is_finite() {
                    return Err("Doris rejects non-finite floats".into());
                }
                b.extend_from_slice(&self.to_le_bytes());
                Ok(IsNull::No)
            }
        }
        impl Decode<'_, Doris> for $t {
            fn decode(v: DorisValueRef<'_>) -> Result<Self, BoxDynError> {
                if v.format == DorisValueFormat::Text {
                    return Ok(v.text()?.parse()?);
                }
                let b = v.bytes()?;
                Ok(match v.type_info.r#type {
                    ColumnType::Float => f32::from_le_bytes(b.try_into()?) as Self,
                    ColumnType::Double => f64::from_le_bytes(b.try_into()?) as Self,
                    _ => return Err("expected floating-point value".into()),
                })
            }
        }
    };
}
float!(f32, Float, 4);
float!(f64, Double, 8);
fn textual(v: &DorisTypeInfo) -> bool {
    matches!(
        v.r#type,
        ColumnType::VarChar
            | ColumnType::VarString
            | ColumnType::String
            | ColumnType::Blob
            | ColumnType::TinyBlob
            | ColumnType::MediumBlob
            | ColumnType::LongBlob
            | ColumnType::Enum
            | ColumnType::Set
            | ColumnType::Json
            | ColumnType::Decimal
            | ColumnType::NewDecimal
            | ColumnType::Time
    )
}
impl Type<Doris> for str {
    fn type_info() -> DorisTypeInfo {
        DorisTypeInfo::text(ColumnType::VarString)
    }
    fn compatible(v: &DorisTypeInfo) -> bool {
        textual(v)
    }
}
impl Type<Doris> for String {
    fn type_info() -> DorisTypeInfo {
        <str as Type<Doris>>::type_info()
    }
    fn compatible(v: &DorisTypeInfo) -> bool {
        textual(v)
    }
}
impl Encode<'_, Doris> for str {
    fn encode_by_ref(&self, b: &mut Vec<u8>) -> Result<IsNull, BoxDynError> {
        b.put_bytes_lenenc(self.as_bytes());
        Ok(IsNull::No)
    }
}
impl Encode<'_, Doris> for String {
    fn encode_by_ref(&self, b: &mut Vec<u8>) -> Result<IsNull, BoxDynError> {
        <str as Encode<Doris>>::encode_by_ref(self, b)
    }
}
impl<'r> Decode<'r, Doris> for &'r str {
    fn decode(v: DorisValueRef<'r>) -> Result<Self, BoxDynError> {
        if v.format == DorisValueFormat::Binary && v.type_info.r#type == ColumnType::Time {
            return Err("Doris binary TIME loses fractional boundaries; use text protocol".into());
        }
        v.text()
    }
}
impl Decode<'_, Doris> for String {
    fn decode(v: DorisValueRef<'_>) -> Result<Self, BoxDynError> {
        Ok(<&str as Decode<Doris>>::decode(v)?.into())
    }
}
impl Type<Doris> for [u8] {
    fn type_info() -> DorisTypeInfo {
        DorisTypeInfo::binary(ColumnType::Blob)
    }
    fn compatible(v: &DorisTypeInfo) -> bool {
        textual(v) || v.r#type == ColumnType::Bit
    }
}
impl Type<Doris> for Vec<u8> {
    fn type_info() -> DorisTypeInfo {
        <[u8] as Type<Doris>>::type_info()
    }
    fn compatible(v: &DorisTypeInfo) -> bool {
        <[u8] as Type<Doris>>::compatible(v)
    }
}
impl Encode<'_, Doris> for [u8] {
    fn encode_by_ref(&self, b: &mut Vec<u8>) -> Result<IsNull, BoxDynError> {
        b.put_bytes_lenenc(self);
        Ok(IsNull::No)
    }
}
impl Encode<'_, Doris> for Vec<u8> {
    fn encode_by_ref(&self, b: &mut Vec<u8>) -> Result<IsNull, BoxDynError> {
        <[u8] as Encode<Doris>>::encode_by_ref(self, b)
    }
}
impl<'r> Decode<'r, Doris> for &'r [u8] {
    fn decode(v: DorisValueRef<'r>) -> Result<Self, BoxDynError> {
        v.bytes()
    }
}
impl Decode<'_, Doris> for Vec<u8> {
    fn decode(v: DorisValueRef<'_>) -> Result<Self, BoxDynError> {
        Ok(v.bytes()?.into())
    }
}
impl Type<Doris> for bigdecimal::BigDecimal {
    fn type_info() -> DorisTypeInfo {
        DorisTypeInfo::text(ColumnType::NewDecimal)
    }
    fn compatible(v: &DorisTypeInfo) -> bool {
        matches!(v.r#type, ColumnType::Decimal | ColumnType::NewDecimal)
    }
}
impl Encode<'_, Doris> for bigdecimal::BigDecimal {
    fn encode_by_ref(&self, b: &mut Vec<u8>) -> Result<IsNull, BoxDynError> {
        b.put_bytes_lenenc(self.to_string().as_bytes());
        Ok(IsNull::No)
    }
}
impl Decode<'_, Doris> for bigdecimal::BigDecimal {
    fn decode(v: DorisValueRef<'_>) -> Result<Self, BoxDynError> {
        Ok(v.text()?.parse()?)
    }
}
impl<T> Type<Doris> for Json<T> {
    fn type_info() -> DorisTypeInfo {
        DorisTypeInfo::text(ColumnType::Json)
    }
    fn compatible(v: &DorisTypeInfo) -> bool {
        textual(v)
    }
}
impl<T: serde::Serialize> Encode<'_, Doris> for Json<T> {
    fn encode_by_ref(&self, b: &mut Vec<u8>) -> Result<IsNull, BoxDynError> {
        b.put_bytes_lenenc(crate::types::json::to_string(&self.0)?.as_bytes());
        Ok(IsNull::No)
    }
}
impl<'r, T: serde::Deserialize<'r>> Decode<'r, Doris> for Json<T> {
    fn decode(v: DorisValueRef<'r>) -> Result<Self, BoxDynError> {
        Ok(Json(serde_json::from_slice(v.bytes()?)?))
    }
}
impl Type<Doris> for chrono::NaiveDate {
    fn type_info() -> DorisTypeInfo {
        DorisTypeInfo::binary(ColumnType::Date)
    }
    fn compatible(v: &DorisTypeInfo) -> bool {
        v.r#type == ColumnType::Date
    }
}
impl Encode<'_, Doris> for chrono::NaiveDate {
    fn encode_by_ref(&self, b: &mut Vec<u8>) -> Result<IsNull, BoxDynError> {
        let y = u16::try_from(self.year())?;
        if !(1..=9999).contains(&y) {
            return Err("date year outside Doris range".into());
        }
        b.push(4);
        b.extend_from_slice(&y.to_le_bytes());
        b.extend_from_slice(&[self.month() as u8, self.day() as u8]);
        Ok(IsNull::No)
    }
}
impl Decode<'_, Doris> for chrono::NaiveDate {
    fn decode(v: DorisValueRef<'_>) -> Result<Self, BoxDynError> {
        if v.format == DorisValueFormat::Text {
            return Ok(Self::parse_from_str(v.text()?, "%Y-%m-%d")?);
        }
        let b = v.bytes()?;
        if b.len() < 4 {
            return Err("zero or truncated date".into());
        }
        Self::from_ymd_opt(
            i32::from(u16::from_le_bytes([b[0], b[1]])),
            u32::from(b[2]),
            u32::from(b[3]),
        )
        .ok_or_else(|| "invalid date".into())
    }
}
impl Type<Doris> for chrono::NaiveDateTime {
    fn type_info() -> DorisTypeInfo {
        DorisTypeInfo::binary(ColumnType::Datetime)
    }
    fn compatible(v: &DorisTypeInfo) -> bool {
        matches!(v.r#type, ColumnType::Datetime | ColumnType::Timestamp)
    }
}
impl Encode<'_, Doris> for chrono::NaiveDateTime {
    fn encode_by_ref(&self, b: &mut Vec<u8>) -> Result<IsNull, BoxDynError> {
        if self.nanosecond() >= 1_000_000_000 || !self.nanosecond().is_multiple_of(1000) {
            return Err("Doris requires microsecond datetime precision".into());
        }
        let start = b.len();
        let _ = <chrono::NaiveDate as Encode<Doris>>::encode_by_ref(&self.date(), b)?;
        b[start] = 11;
        b.extend_from_slice(&[self.hour() as u8, self.minute() as u8, self.second() as u8]);
        b.extend_from_slice(&(self.nanosecond() / 1000).to_le_bytes());
        Ok(IsNull::No)
    }
}
impl Decode<'_, Doris> for chrono::NaiveDateTime {
    fn decode(v: DorisValueRef<'_>) -> Result<Self, BoxDynError> {
        if v.format == DorisValueFormat::Text {
            return Ok(Self::parse_from_str(v.text()?, "%Y-%m-%d %H:%M:%S%.f")?);
        }
        let b = v.bytes()?;
        let date = <chrono::NaiveDate as Decode<Doris>>::decode(v.clone())?;
        let (h, m, s, us) = match b.len() {
            4 => (0, 0, 0, 0),
            7 => (b[4], b[5], b[6], 0),
            11 => (b[4], b[5], b[6], u32::from_le_bytes(b[7..11].try_into()?)),
            _ => return Err("invalid datetime width".into()),
        };
        date.and_hms_micro_opt(u32::from(h), u32::from(m), u32::from(s), us)
            .ok_or_else(|| "invalid datetime".into())
    }
}

impl Encode<'_, Doris> for &str {
    fn encode_by_ref(&self, b: &mut Vec<u8>) -> Result<IsNull, BoxDynError> {
        <str as Encode<Doris>>::encode_by_ref(self, b)
    }
}
impl Encode<'_, Doris> for &[u8] {
    fn encode_by_ref(&self, b: &mut Vec<u8>) -> Result<IsNull, BoxDynError> {
        <[u8] as Encode<Doris>>::encode_by_ref(self, b)
    }
}
