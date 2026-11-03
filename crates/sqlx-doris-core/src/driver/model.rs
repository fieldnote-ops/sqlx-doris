use super::*;
use bytes::Bytes;
use sqlx_core::{
    column::ColumnIndex,
    value::{Value, ValueRef},
};
use std::{borrow::Cow, ops::Range};

/// Apache Doris database identity for SQLx's generic APIs.
#[derive(Debug)]
pub struct Doris;
impl sqlx_core::database::Database for Doris {
    type Connection = DorisConnection;
    type TransactionManager = DorisTransactionManager;
    type Row = DorisRow;
    type QueryResult = DorisQueryResult;
    type Column = DorisColumn;
    type TypeInfo = DorisTypeInfo;
    type Value = DorisValue;
    type ValueRef<'r> = DorisValueRef<'r>;
    type Arguments = DorisArguments;
    type ArgumentBuffer = Vec<u8>;
    type Statement = DorisStatement;
    const NAME: &'static str = "Doris";
    const URL_SCHEMES: &'static [&'static str] = &["doris"];
}
impl sqlx_core::database::HasStatementCache for Doris {}

// Codes defined by the public wire protocol, independent of any MySQL driver.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[cfg_attr(
    feature = "macro-support",
    derive(serde::Serialize, serde::Deserialize)
)]
#[repr(u8)]
pub(crate) enum ColumnType {
    Decimal = 0,
    Tiny = 1,
    Short = 2,
    Long = 3,
    Float = 4,
    Double = 5,
    Null = 6,
    Timestamp = 7,
    LongLong = 8,
    Int24 = 9,
    Date = 10,
    Time = 11,
    Datetime = 12,
    Year = 13,
    VarChar = 15,
    Bit = 16,
    Json = 245,
    NewDecimal = 246,
    Enum = 247,
    Set = 248,
    TinyBlob = 249,
    MediumBlob = 250,
    LongBlob = 251,
    Blob = 252,
    VarString = 253,
    String = 254,
    Geometry = 255,
}
impl TryFrom<u8> for ColumnType {
    type Error = Error;
    fn try_from(n: u8) -> Result<Self, Error> {
        use ColumnType::*;
        Ok(match n {
            0 => Decimal,
            1 => Tiny,
            2 => Short,
            3 => Long,
            4 => Float,
            5 => Double,
            6 => Null,
            7 => Timestamp,
            8 => LongLong,
            9 => Int24,
            10 => Date,
            11 => Time,
            12 => Datetime,
            13 => Year,
            15 => VarChar,
            16 => Bit,
            245 => Json,
            246 => NewDecimal,
            247 => Enum,
            248 => Set,
            249 => TinyBlob,
            250 => MediumBlob,
            251 => LongBlob,
            252 => Blob,
            253 => VarString,
            254 => String,
            255 => Geometry,
            _ => return Err(protocol("unsupported wire type")),
        })
    }
}
bitflags::bitflags! {
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    #[cfg_attr(feature = "macro-support", derive(serde::Serialize, serde::Deserialize))]
    pub(crate) struct ColumnFlags: u16 {
        const NOT_NULL = 1;
        const UNSIGNED = 32;
        const BINARY = 128;
    }
}
/// Type metadata returned by the Doris wire endpoint.
#[derive(Debug, Clone, PartialEq, Eq)]
#[cfg_attr(
    feature = "macro-support",
    derive(serde::Serialize, serde::Deserialize)
)]
pub struct DorisTypeInfo {
    pub(crate) r#type: ColumnType,
    pub(crate) flags: ColumnFlags,
    pub(crate) charset: u16,
    #[cfg_attr(feature = "macro-support", serde(default))]
    pub(crate) logical: Option<String>,
}
impl DorisTypeInfo {
    pub(crate) const fn binary(r#type: ColumnType) -> Self {
        Self {
            r#type,
            flags: ColumnFlags::empty(),
            charset: 63,
            logical: None,
        }
    }
    pub(crate) fn text(r#type: ColumnType) -> Self {
        Self {
            r#type,
            flags: ColumnFlags::empty(),
            charset: 45,
            logical: None,
        }
    }
}
impl std::fmt::Display for DorisTypeInfo {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        sqlx_core::type_info::TypeInfo::name(self).fmt(f)
    }
}
impl sqlx_core::type_info::TypeInfo for DorisTypeInfo {
    fn is_null(&self) -> bool {
        self.r#type == ColumnType::Null
    }
    fn name(&self) -> &str {
        if let Some(logical) = &self.logical {
            return logical.split(['(', '<', ' ']).next().unwrap_or(logical);
        }
        use ColumnType::*;
        match self.r#type {
            Tiny => "TINYINT",
            Short => "SMALLINT",
            Long | Int24 => "INT",
            LongLong => "BIGINT",
            Float => "FLOAT",
            Double => "DOUBLE",
            Decimal | NewDecimal => "DECIMAL",
            Date => "DATE",
            Datetime | Timestamp => "DATETIME",
            Time => "TIME",
            Null => "NULL",
            Json => "JSON",
            Year => "YEAR",
            Bit => "BIT",
            _ if self.charset == 63 => "BINARY",
            _ => "STRING",
        }
    }
}
/// A result column; names and metadata are shared between rows.
#[derive(Debug, Clone)]
#[cfg_attr(
    feature = "macro-support",
    derive(serde::Serialize, serde::Deserialize)
)]
pub struct DorisColumn {
    pub(crate) ordinal: usize,
    pub(crate) name: String,
    pub(crate) info: DorisTypeInfo,
    pub(crate) origin: sqlx_core::column::ColumnOrigin,
    pub(crate) source: Option<(String, String, String)>,
}
impl sqlx_core::column::Column for DorisColumn {
    type Database = Doris;
    fn origin(&self) -> sqlx_core::column::ColumnOrigin {
        self.origin.clone()
    }
    fn ordinal(&self) -> usize {
        self.ordinal
    }
    fn name(&self) -> &str {
        &self.name
    }
    fn type_info(&self) -> &DorisTypeInfo {
        &self.info
    }
}
/// Wire representation of a returned value.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DorisValueFormat {
    /// Text protocol.
    Text,
    /// Binary prepared-statement protocol.
    Binary,
}
/// An owned value, backed by reference-counted packet bytes.
#[derive(Debug, Clone)]
pub struct DorisValue {
    pub(crate) value: Option<Bytes>,
    pub(crate) info: DorisTypeInfo,
    pub(crate) format: DorisValueFormat,
}
/// A borrowed value. Decoders can borrow strings/bytes without allocating.
#[derive(Debug, Clone)]
pub struct DorisValueRef<'r> {
    pub(crate) value: Option<&'r [u8]>,
    pub(crate) row: Option<&'r Bytes>,
    pub(crate) type_info: DorisTypeInfo,
    pub(crate) format: DorisValueFormat,
}
impl<'r> DorisValueRef<'r> {
    pub(crate) fn bytes(&self) -> Result<&'r [u8], sqlx_core::error::BoxDynError> {
        self.value
            .ok_or_else(|| sqlx_core::error::UnexpectedNullError.into())
    }
    pub(crate) fn text(&self) -> Result<&'r str, sqlx_core::error::BoxDynError> {
        Ok(std::str::from_utf8(self.bytes()?)?)
    }
}
impl Value for DorisValue {
    type Database = Doris;
    fn as_ref(&self) -> DorisValueRef<'_> {
        DorisValueRef {
            value: self.value.as_deref(),
            row: self.value.as_ref(),
            type_info: self.info.clone(),
            format: self.format,
        }
    }
    fn type_info(&self) -> Cow<'_, DorisTypeInfo> {
        Cow::Borrowed(&self.info)
    }
    fn is_null(&self) -> bool {
        self.value.is_none()
    }
}
impl ValueRef<'_> for DorisValueRef<'_> {
    type Database = Doris;
    fn to_owned(&self) -> DorisValue {
        DorisValue {
            value: self.value.map(|v| {
                self.row
                    .map_or_else(|| Bytes::copy_from_slice(v), |row| row.slice_ref(v))
            }),
            info: self.type_info.clone(),
            format: self.format,
        }
    }
    fn type_info(&self) -> Cow<'_, DorisTypeInfo> {
        Cow::Borrowed(&self.type_info)
    }
    fn is_null(&self) -> bool {
        self.value.is_none()
    }
}
/// One streamed result row, holding one packet rather than allocating per cell.
#[derive(Debug)]
pub struct DorisRow {
    pub(crate) packet: Bytes,
    pub(crate) ranges: Vec<Option<Range<usize>>>,
    pub(crate) columns: Arc<Vec<DorisColumn>>,
    pub(crate) format: DorisValueFormat,
}
impl sqlx_core::row::Row for DorisRow {
    type Database = Doris;
    fn columns(&self) -> &[DorisColumn] {
        &self.columns
    }
    fn try_get_raw<I: ColumnIndex<Self>>(&self, index: I) -> Result<DorisValueRef<'_>, Error> {
        let i = index.index(self)?;
        Ok(DorisValueRef {
            value: self.ranges[i].as_ref().map(|r| &self.packet[r.clone()]),
            row: Some(&self.packet),
            type_info: self.columns[i].info.clone(),
            format: self.format,
        })
    }
}
impl ColumnIndex<DorisRow> for &str {
    fn index(&self, row: &DorisRow) -> Result<usize, Error> {
        row.columns
            .iter()
            .position(|c| c.name == *self)
            .ok_or_else(|| Error::ColumnNotFound((*self).into()))
    }
}
/// Acknowledged affected-row count. Visibility follows Doris's commit mode.
#[derive(Debug, Default, Clone)]
pub struct DorisQueryResult {
    pub(crate) affected: u64,
}
impl DorisQueryResult {
    /// Rows affected as reported by Doris.
    pub fn rows_affected(&self) -> u64 {
        self.affected
    }
}
impl Extend<Self> for DorisQueryResult {
    fn extend<T: IntoIterator<Item = Self>>(&mut self, values: T) {
        for v in values {
            self.affected += v.affected;
        }
    }
}
/// Metadata for an explicitly prepared Doris statement.
#[derive(Debug, Clone)]
pub struct DorisStatement {
    pub(crate) sql: SqlStr,
    pub(crate) parameters: usize,
    pub(crate) columns: Arc<Vec<DorisColumn>>,
}
impl sqlx_core::statement::Statement for DorisStatement {
    type Database = Doris;
    fn into_sql(self) -> SqlStr {
        self.sql
    }
    fn sql(&self) -> &SqlStr {
        &self.sql
    }
    fn parameters(&self) -> Option<Either<&[DorisTypeInfo], usize>> {
        Some(Either::Right(self.parameters))
    }
    fn columns(&self) -> &[DorisColumn] {
        &self.columns
    }
    sqlx_core::impl_statement_query!(DorisArguments);
}
impl ColumnIndex<DorisStatement> for &str {
    fn index(&self, s: &DorisStatement) -> Result<usize, Error> {
        s.columns
            .iter()
            .position(|c| c.name == *self)
            .ok_or_else(|| Error::ColumnNotFound((*self).into()))
    }
}
sqlx_core::impl_column_index_for_row!(DorisRow);
sqlx_core::impl_column_index_for_statement!(DorisStatement);
sqlx_core::impl_into_arguments_for_arguments!(DorisArguments);
sqlx_core::impl_acquire!(Doris, DorisConnection);
sqlx_core::impl_encode_for_option!(Doris);
