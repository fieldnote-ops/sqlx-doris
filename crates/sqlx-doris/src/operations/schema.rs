use crate::{Error, Result};
use crate::sqlx::{self, MySql, MySqlPool, QueryBuilder, Row};
use std::{collections::BTreeMap, fmt};

pub(super) fn identifier(value: &str) -> String {
    format!("`{}`", value.replace('`', "``"))
}
pub(super) fn validate_identifier(value: &str) -> Result<()> {
    if value.is_empty() || value.contains('\0') {
        Err(Error::InvalidInput(
            "identifier cannot be empty or contain NUL".into(),
        ))
    } else {
        Ok(())
    }
}
fn literal(value: &str) -> String {
    format!("'{}'", value.replace('\\', "\\\\").replace('\'', "''"))
}

/// A table identifier. Database and table are quoted separately, without parsing
/// SQL expressions or splitting dots inside names.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TableName {
    /// Database name, or None to use the SQL connection's current database.
    pub database: Option<String>,
    /// Literal table name, without SQL quoting or expression syntax.
    pub table: String,
}
impl TableName {
    /// A database-qualified table; names are quoted separately when rendered.
    pub fn new(database: impl Into<String>, table: impl Into<String>) -> Self {
        Self {
            database: Some(database.into()),
            table: table.into(),
        }
    }
    /// A table in the SQL connection's current database.
    pub fn unqualified(table: impl Into<String>) -> Self {
        Self {
            database: None,
            table: table.into(),
        }
    }
    pub(crate) fn validate(&self) -> Result<()> {
        validate_identifier(&self.table)?;
        if let Some(db) = &self.database {
            validate_identifier(db)?;
        }
        Ok(())
    }
}
impl fmt::Display for TableName {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if let Some(database) = &self.database {
            write!(f, "{}.", identifier(database))?;
        }
        f.write_str(&identifier(&self.table))
    }
}

/// A Doris catalog/database/table identifier, with each component quoted.
/// This is a SQL relation name; Stream Load does not accept external catalogs.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CatalogTableName {
    /// Literal catalog name.
    pub catalog: String,
    /// Literal database name within the catalog.
    pub database: String,
    /// Literal table name within the database.
    pub table: String,
}
impl CatalogTableName {
    /// Construct a qualified relation without interpreting dots as separators.
    pub fn new(
        catalog: impl Into<String>,
        database: impl Into<String>,
        table: impl Into<String>,
    ) -> Self {
        Self {
            catalog: catalog.into(),
            database: database.into(),
            table: table.into(),
        }
    }
    fn validate(&self) -> Result<()> {
        for component in [&self.catalog, &self.database, &self.table] {
            validate_identifier(component)?;
        }
        Ok(())
    }
}
impl fmt::Display for CatalogTableName {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{}.{}.{}",
            identifier(&self.catalog),
            identifier(&self.database),
            identifier(&self.table)
        )
    }
}

/// Doris column types, including recursive collection and record types.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum DataType {
    /// BOOLEAN.
    Boolean,
    /// Signed 8-bit integer.
    TinyInt,
    /// Signed 16-bit integer.
    SmallInt,
    /// Signed 32-bit integer.
    Int,
    /// Signed 64-bit integer.
    BigInt,
    /// Signed 128-bit LARGEINT.
    LargeInt,
    /// Single-precision floating point.
    Float,
    /// Double-precision floating point.
    Double,
    /// Exact decimal, requiring enable_decimal256 above precision 38.
    Decimal {
        /// Total significant decimal digits, 1..=76.
        precision: u8,
        /// Fractional digits, at most precision.
        scale: u8,
    },
    /// Calendar date.
    Date,
    /// Local date/time with fractional precision 0..=6.
    DateTime(u8),
    /// Signed query-only duration, precision 0..=6; OLAP storage rejects TIME.
    Time(u8),
    /// Time-zone-aware timestamp with fractional precision 0..=6.
    TimestampTz(u8),
    /// Fixed-length string, parameterized by byte length.
    Char(u32),
    /// Variable-length string, parameterized by maximum byte length.
    VarChar(u32),
    /// Unbounded logical STRING, subject to server storage limits.
    String,
    /// Query / external-catalog type; Doris 4.1.5 OLAP storage rejects it.
    VarBinary,
    /// JSON / JSONB.
    Json,
    /// Doris's schema-flexible VARIANT, with server-side normalization rules.
    Variant,
    /// VARIANT with explicit path types and a dynamic subcolumn budget.
    /// Paths use Doris's schema-template pattern syntax, not JSONPath. The server
    /// validates supported leaf types and applies its own conversion rules.
    VariantSchema {
        /// Ordered path/pattern declarations, for example `user.id` or `tags*`.
        paths: Vec<(String, DataType)>,
        /// Dynamic subcolumn limit; 0 selects the server's unlimited setting.
        /// Declared paths do not count toward this budget with default settings.
        max_subcolumns: u32,
    },
    /// Native IPv4 address.
    Ipv4,
    /// Native IPv6 address.
    Ipv6,
    /// Ordered elements; nested NULLs depend on the element representation.
    Array(Box<DataType>),
    /// Typed keys and values; inspect server duplicate-key semantics.
    Map(Box<DataType>, Box<DataType>),
    /// Ordered named fields with explicitly declared types.
    Struct(Vec<(String, DataType)>),
    /// Opaque bitmap aggregation state; OLAP columns require NOT NULL.
    /// Query its value through explicit bitmap functions.
    Bitmap,
    /// Opaque approximate-distinct aggregation state; OLAP columns require NOT NULL.
    Hll,
    /// Opaque percentile aggregation state; OLAP columns require NOT NULL.
    QuantileState,
    /// An opaque aggregate-function state, read through matching merge/union
    /// functions. The argument types and nullability are part of its signature.
    /// Requires enable_agg_state=true on each connection (use after_connect).
    /// Its serialized representation can change across Doris versions.
    /// OLAP storage rejects nullable COUNT inputs here: Doris 4.1.5 can lose
    /// grouped states. For nullable-value counts use SUM_STATE of explicit 0/1.
    AggState {
        /// Literal aggregate function name, quoted as an identifier.
        function: String,
        /// Ordered input signature, including each argument's nullability.
        arguments: Vec<AggregateArgument>,
    },
}

/// A typed argument in an AGG_STATE function signature.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AggregateArgument {
    /// The aggregation function's logical input type.
    pub data_type: DataType,
    /// Whether the function input can be SQL NULL; this affects state compatibility.
    pub nullable: bool,
}
impl AggregateArgument {
    /// A nullable function input.
    pub fn new(data_type: DataType) -> Self {
        Self {
            data_type,
            nullable: true,
        }
    }
    /// Require a non-NULL input in the function signature.
    pub fn not_null(mut self) -> Self {
        self.nullable = false;
        self
    }
}
impl DataType {
    /// Construct an ARRAY type.
    pub fn array(element: Self) -> Self {
        Self::Array(Box::new(element))
    }
    /// Construct a MAP with explicit key and value types.
    pub fn map(key: Self, value: Self) -> Self {
        Self::Map(Box::new(key), Box::new(value))
    }
    pub(crate) fn validate(&self) -> Result<()> {
        match self {
            Self::Decimal { precision, scale }
                if *precision == 0 || *precision > 76 || scale > precision =>
            {
                return Err(Error::InvalidInput(
                    "DECIMAL requires 1..=76 precision and scale <= precision".into(),
                ));
            }
            Self::DateTime(p) | Self::TimestampTz(p) | Self::Time(p) if *p > 6 => {
                return Err(Error::InvalidInput(
                    "Doris time precision must be <= 6".into(),
                ));
            }
            Self::Char(0) | Self::VarChar(0) => {
                return Err(Error::InvalidInput("string length must be positive".into()));
            }
            Self::Array(inner) => inner.validate()?,
            Self::Map(key, value) => {
                key.validate()?;
                value.validate()?;
            }
            Self::Struct(fields) => {
                if fields.is_empty() {
                    return Err(Error::InvalidInput("STRUCT requires fields".into()));
                }
                for (name, kind) in fields {
                    validate_identifier(name)?;
                    kind.validate()?;
                }
            }
            Self::VariantSchema { paths, .. } => {
                for (path, kind) in paths {
                    validate_identifier(path)?;
                    kind.validate()?;
                }
            }
            Self::AggState {
                function,
                arguments,
            } => {
                validate_identifier(function)?;
                for argument in arguments {
                    argument.data_type.validate()?;
                }
            }
            _ => {}
        }
        Ok(())
    }
    fn validate_olap_storage(&self) -> Result<()> {
        match self {
            Self::AggState {
                function,
                arguments,
            } if function.eq_ignore_ascii_case("count") && arguments.iter().any(|a| a.nullable) => {
                return Err(Error::Unsupported(
                    "Doris 4.1.5 nullable COUNT AGG_STATE can lose groups; use non-null COUNT \
                        for row counts, or SUM_STATE of explicit 0/1 for nullable-value counts",
                ));
            }
            Self::Time(_) => {
                return Err(Error::Unsupported(
                    "Doris TIME is a query-only duration and cannot be stored in an OLAP table",
                ));
            }
            Self::VarBinary => {
                return Err(Error::Unsupported(
                    "Doris 4.1.5 cannot store VARBINARY in an OLAP table; use an explicit \
                        application representation or an external catalog",
                ));
            }
            Self::Array(inner) => inner.validate_olap_storage()?,
            Self::Map(key, value) => {
                key.validate_olap_storage()?;
                value.validate_olap_storage()?;
            }
            Self::Struct(fields) => {
                for (_, kind) in fields {
                    kind.validate_olap_storage()?;
                }
            }
            _ => {}
        }
        Ok(())
    }
}

/// Project a named column without Doris's ambiguous complex-value text format.
/// Collections / records use JSON_QUOTE and JSON text composition; MAP becomes
/// an array of `{key,value}`
/// records so numeric keys, nested maps and NULLs retain their types. Decode
/// those entries with [`crate::types::Map`]. TIMESTAMPTZ uses an explicit STRING
/// cast so older protocol metadata cannot erase its offset. TIME also uses a
/// STRING cast to retain its full microsecond boundary; other scalars stay as-is.
/// This requires a schema hint because MySQL metadata erases complex types.
pub fn project(column: &str, data_type: &DataType) -> Result<String> {
    validate_identifier(column)?;
    data_type.validate()?;
    let expression = identifier(column);
    let projected = match data_type {
        DataType::Array(_) | DataType::Map(_, _) | DataType::Struct(_) => {
            // Preserve SQL NULL at the root. Nested NULLs are JSON null tokens.
            // Older JSONB output stops at embedded NUL even when storage is
            // intact; JSON_QUOTE operates on the actual string length instead.
            format!(
                "IF({expression} IS NULL,NULL,{})",
                json_text(&expression, data_type, 0)
            )
        }
        // Older FE metadata reports DATETIME even though the payload is an
        // offset-bearing string. An explicit cast makes metadata match bytes.
        DataType::TimestampTz(_) | DataType::Time(_) => format!("CAST({expression} AS STRING)"),
        _ => return Ok(expression),
    };
    Ok(format!("{projected} AS {expression}"))
}

pub(super) fn json_text(expression: &str, data_type: &DataType, depth: usize) -> String {
    let value = match data_type {
        DataType::Array(inner) => {
            let variable = format!("_sqlx_doris_{depth}");
            format!(
                "CONCAT('[',ARRAY_JOIN(ARRAY_MAP({variable} -> {},{expression}),','),']')",
                json_text(&variable, inner, depth + 1)
            )
        }
        DataType::Map(key, value) => {
            let variable = format!("_sqlx_doris_{depth}");
            let key = json_text(&format!("STRUCT_ELEMENT({variable},1)"), key, depth + 1);
            let value = json_text(&format!("STRUCT_ELEMENT({variable},2)"), value, depth + 1);
            format!(
                "CONCAT('[',ARRAY_JOIN(ARRAY_MAP({variable} -> CONCAT('{{\"key\":',{key},',\
                    \"value\":',{value},'}}'),MAP_ENTRIES({expression})),','),']')"
            )
        }
        DataType::Struct(fields) => {
            let fields = fields
                .iter()
                .enumerate()
                .map(|(index, (name, kind))| {
                    // Hex keeps JSON field names independent of sql_mode.
                    let name = serde_json::to_string(name).expect("strings serialize as JSON");
                    let name = name.bytes().map(|b| format!("{b:02x}")).collect::<String>();
                    let value = json_text(
                        &format!("STRUCT_ELEMENT({expression},{})", index + 1),
                        kind,
                        depth + 1,
                    );
                    format!("CONCAT(UNHEX('{name}'),':',{value})")
                })
                .collect::<Vec<_>>();
            format!("CONCAT('{{',{},'}}')", fields.join(",',',"))
        }
        // Chrono's NaiveDateTime serde format uses T. Only temporal leaves change.
        DataType::DateTime(_) | DataType::TimestampTz(_) => {
            format!("JSON_QUOTE(REPLACE(CAST({expression} AS STRING),' ','T'))")
        }
        DataType::Boolean => format!("IF({expression},'true','false')"),
        DataType::Char(_)
        | DataType::VarChar(_)
        | DataType::String
        | DataType::Date
        | DataType::Time(_)
        | DataType::Ipv4
        | DataType::Ipv6
        | DataType::VarBinary => format!("JSON_QUOTE(CAST({expression} AS STRING))"),
        _ => format!("CAST({expression} AS STRING)"),
    };
    format!("IF({expression} IS NULL,'null',{value})")
}
impl fmt::Display for DataType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Boolean => f.write_str("BOOLEAN"),
            Self::TinyInt => f.write_str("TINYINT"),
            Self::SmallInt => f.write_str("SMALLINT"),
            Self::Int => f.write_str("INT"),
            Self::BigInt => f.write_str("BIGINT"),
            Self::LargeInt => f.write_str("LARGEINT"),
            Self::Float => f.write_str("FLOAT"),
            Self::Double => f.write_str("DOUBLE"),
            Self::Decimal { precision, scale } => write!(f, "DECIMAL({precision},{scale})"),
            Self::Date => f.write_str("DATE"),
            Self::DateTime(p) => write!(f, "DATETIME({p})"),
            Self::Time(p) => write!(f, "TIME({p})"),
            Self::TimestampTz(p) => write!(f, "TIMESTAMPTZ({p})"),
            Self::Char(n) => write!(f, "CHAR({n})"),
            Self::VarChar(n) => write!(f, "VARCHAR({n})"),
            Self::String => f.write_str("STRING"),
            Self::VarBinary => f.write_str("VARBINARY"),
            Self::Json => f.write_str("JSON"),
            Self::Variant => f.write_str("VARIANT"),
            Self::VariantSchema {
                paths,
                max_subcolumns,
            } => {
                f.write_str("VARIANT<")?;
                for (path, kind) in paths {
                    write!(f, "{}:{kind},", literal(path))?;
                }
                write!(
                    f,
                    "PROPERTIES('variant_max_subcolumns_count'='{max_subcolumns}')>"
                )
            }
            Self::Ipv4 => f.write_str("IPV4"),
            Self::Ipv6 => f.write_str("IPV6"),
            Self::Array(t) => write!(f, "ARRAY<{t}>"),
            Self::Map(k, v) => write!(f, "MAP<{k},{v}>"),
            Self::Struct(fields) => {
                f.write_str("STRUCT<")?;
                for (index, (name, kind)) in fields.iter().enumerate() {
                    if index > 0 {
                        f.write_str(",")?;
                    }
                    write!(f, "{}:{kind}", identifier(name))?;
                }
                f.write_str(">")
            }
            Self::Bitmap => f.write_str("BITMAP"),
            Self::Hll => f.write_str("HLL"),
            Self::QuantileState => f.write_str("QUANTILE_STATE"),
            Self::AggState {
                function,
                arguments,
            } => {
                write!(f, "AGG_STATE<{}(", identifier(function))?;
                for (index, argument) in arguments.iter().enumerate() {
                    if index > 0 {
                        f.write_str(",")?;
                    }
                    write!(
                        f,
                        "{} {}",
                        argument.data_type,
                        if argument.nullable {
                            "NULL"
                        } else {
                            "NOT NULL"
                        }
                    )?;
                }
                f.write_str(")>")
            }
        }
    }
}

/// Aggregation for non-key columns of an Aggregate Key table.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Aggregate {
    /// SUM.
    Sum,
    /// MIN.
    Min,
    /// MAX.
    Max,
    /// REPLACE.
    Replace,
    /// REPLACE_IF_NOT_NULL.
    ReplaceIfNotNull,
    /// BITMAP_UNION.
    BitmapUnion,
    /// HLL_UNION.
    HllUnion,
    /// QUANTILE_UNION.
    QuantileUnion,
    /// GENERIC, for an AGG_STATE column with its own function signature.
    Generic,
}
impl fmt::Display for Aggregate {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::Sum => "SUM",
            Self::Min => "MIN",
            Self::Max => "MAX",
            Self::Replace => "REPLACE",
            Self::ReplaceIfNotNull => "REPLACE_IF_NOT_NULL",
            Self::BitmapUnion => "BITMAP_UNION",
            Self::HllUnion => "HLL_UNION",
            Self::QuantileUnion => "QUANTILE_UNION",
            Self::Generic => "GENERIC",
        })
    }
}

/// A schema column and its nullability / aggregation policy.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Column {
    /// Literal column name.
    pub name: String,
    /// Doris logical type, retained for DDL and lossless projections.
    pub data_type: DataType,
    /// Whether SQL NULL is permitted.
    pub nullable: bool,
    /// Aggregate Key value-column aggregation, if applicable.
    pub aggregate: Option<Aggregate>,
}
impl Column {
    /// A nullable column without an aggregation function.
    pub fn new(name: impl Into<String>, data_type: DataType) -> Self {
        Self {
            name: name.into(),
            data_type,
            nullable: true,
            aggregate: None,
        }
    }
    /// Require a non-NULL value.
    pub fn not_null(mut self) -> Self {
        self.nullable = false;
        self
    }
    /// Select an Aggregate Key value-column function.
    pub fn aggregate(mut self, function: Aggregate) -> Self {
        self.aggregate = Some(function);
        self
    }
    /// Render a schema-informed SELECT projection; see [`project`].
    pub fn projection(&self) -> Result<String> {
        project(&self.name, &self.data_type)
    }
}

/// Explicit Doris table-model semantics, independent of MySQL primary keys.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum TableModel {
    /// Retain repeated keys as separate rows.
    Duplicate,
    /// Maintain a current row per key according to the table's update properties.
    Unique,
    /// Aggregate value columns by key using their declared functions.
    Aggregate,
}
impl fmt::Display for TableModel {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::Duplicate => "DUPLICATE",
            Self::Unique => "UNIQUE",
            Self::Aggregate => "AGGREGATE",
        })
    }
}

/// A deliberately small OLAP DDL builder. Advanced DDL can use SQLx raw_sql.
#[derive(Clone, Debug)]
pub struct TableDefinition {
    /// Table to create or query.
    pub name: TableName,
    /// Required table model.
    pub model: TableModel,
    /// Key columns, forming the leading schema prefix in the same order.
    pub keys: Vec<String>,
    /// Columns in schema order.
    pub columns: Vec<Column>,
    buckets: u32,
    properties: BTreeMap<String, String>,
    if_not_exists: bool,
}
impl TableDefinition {
    /// Start a definition with one bucket and no overridden table properties.
    pub fn new(
        name: TableName,
        model: TableModel,
        keys: impl IntoIterator<Item = impl Into<String>>,
    ) -> Self {
        Self {
            name,
            model,
            keys: keys.into_iter().map(Into::into).collect(),
            columns: vec![],
            buckets: 1,
            properties: BTreeMap::new(),
            if_not_exists: false,
        }
    }
    /// Append a column in schema order.
    pub fn column(mut self, column: Column) -> Self {
        self.columns.push(column);
        self
    }
    /// Set a positive hash bucket count; keys are also used for distribution.
    pub fn buckets(mut self, buckets: u32) -> Self {
        self.buckets = buckets;
        self
    }
    /// Set replication_num explicitly. Local single-BE fixtures use 1.
    pub fn replication(mut self, count: u16) -> Self {
        self.properties
            .insert("replication_num".into(), count.to_string());
        self
    }
    /// Set a table property, with SQL literal quoting handled by the builder.
    pub fn property(mut self, name: impl Into<String>, value: impl Into<String>) -> Self {
        self.properties.insert(name.into(), value.into());
        self
    }
    /// Add IF NOT EXISTS; the server retains any existing table's schema.
    pub fn if_not_exists(mut self) -> Self {
        self.if_not_exists = true;
        self
    }
    /// Start a SELECT with schema-informed, lossless complex-type projections.
    /// Append predicates / ordering with SQLx QueryBuilder and push_bind values.
    pub fn select_builder(&self) -> Result<QueryBuilder<MySql>> {
        self.name.validate()?;
        if self.columns.is_empty() {
            return Err(Error::InvalidInput("SELECT requires columns".into()));
        }
        let columns = self
            .columns
            .iter()
            .map(Column::projection)
            .collect::<Result<Vec<_>>>()?;
        Ok(QueryBuilder::new(format!(
            "SELECT {} FROM {}",
            columns.join(","),
            self.name
        )))
    }
    /// Validate structural bounds and render quoted OLAP DDL. The server performs
    /// final model/type/property validation; VARBINARY storage is rejected here.
    pub fn to_sql(&self) -> Result<String> {
        self.name.validate()?;
        if self.keys.is_empty() || self.columns.is_empty() || self.buckets == 0 {
            return Err(Error::InvalidInput(
                "table requires columns, key columns, and positive bucket count".into(),
            ));
        }
        for (index, key) in self.keys.iter().enumerate() {
            validate_identifier(key)?;
            if self.columns.get(index).map(|c| &c.name) != Some(key) {
                return Err(Error::InvalidInput(
                    "key columns must form the leading column prefix in the same order".into(),
                ));
            }
        }
        let mut definitions = vec![];
        for column in &self.columns {
            validate_identifier(&column.name)?;
            column.data_type.validate()?;
            column.data_type.validate_olap_storage()?;
            let aggregate = column
                .aggregate
                .map(|v| format!(" {v}"))
                .unwrap_or_default();
            definitions.push(format!(
                "{} {}{} {}",
                identifier(&column.name),
                column.data_type,
                aggregate,
                if column.nullable { "NULL" } else { "NOT NULL" }
            ));
        }
        let keys = self
            .keys
            .iter()
            .map(|s| identifier(s))
            .collect::<Vec<_>>()
            .join(",");
        let mut sql = format!(
            "CREATE TABLE {}{} ({}) ENGINE=OLAP {} KEY({keys}) DISTRIBUTED BY \
                HASH({keys}) BUCKETS {}",
            if self.if_not_exists {
                "IF NOT EXISTS "
            } else {
                ""
            },
            self.name,
            definitions.join(","),
            self.model,
            self.buckets
        );
        if !self.properties.is_empty() {
            let properties = self
                .properties
                .iter()
                .map(|(k, v)| format!("{}={}", literal(k), literal(v)))
                .collect::<Vec<_>>()
                .join(",");
            sql.push_str(&format!(" PROPERTIES ({properties})"));
        }
        Ok(sql)
    }
}

/// Logical server schema, retaining Doris's own type description.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ColumnInfo {
    /// Server column name.
    pub name: String,
    /// Unmodified server type description, including nested logical types.
    pub data_type: String,
    /// Whether SQL NULL is permitted.
    pub nullable: bool,
    /// Whether this column is reported as a key.
    pub is_key: bool,
    /// Server default expression/value, if any.
    pub default: Option<String>,
    /// Additional server metadata.
    pub extra: Option<String>,
}

/// Read DESC metadata through either backend, retaining logical type strings.
pub async fn describe_table(pool: &MySqlPool, table: &TableName) -> Result<Vec<ColumnInfo>> {
    table.validate()?;
    describe_relation(pool, &table.to_string()).await
}

pub(super) async fn describe_catalog_table(
    pool: &MySqlPool,
    table: &CatalogTableName,
) -> Result<Vec<ColumnInfo>> {
    table.validate()?;
    describe_relation(pool, &table.to_string()).await
}

async fn describe_relation(pool: &MySqlPool, table: &str) -> Result<Vec<ColumnInfo>> {
    let rows = sqlx::raw_sql(sqlx::AssertSqlSafe(format!("DESC {table}")))
        .fetch_all(pool)
        .await?;
    rows.into_iter()
        .map(|row| {
            let key: String = row.try_get("Key")?;
            let nullable: String = row.try_get("Null")?;
            Ok(ColumnInfo {
                name: row.try_get("Field")?,
                data_type: row.try_get("Type")?,
                nullable: nullable.eq_ignore_ascii_case("YES"),
                is_key: key.eq_ignore_ascii_case("true") || key == "PRI",
                default: row.try_get("Default")?,
                extra: row.try_get("Extra")?,
            })
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn identifiers_are_quoted_and_invalid_types_are_rejected() {
        assert_eq!(
            TableName::new("db.name", "x`; DROP TABLE t;--").to_string(),
            "`db.name`.`x``; DROP TABLE t;--`"
        );
        let table = TableDefinition::new(TableName::unqualified("t"), TableModel::Unique, ["id"])
            .column(Column::new("id", DataType::BigInt).not_null())
            .column(Column::new(
                "value",
                DataType::Decimal {
                    precision: 76,
                    scale: 30,
                },
            ))
            .replication(1);
        assert!(table.to_sql().unwrap().contains("DECIMAL(76,30)"));
        let binary = TableDefinition {
            columns: vec![
                Column::new("id", DataType::BigInt),
                Column::new("bytes", DataType::VarBinary),
            ],
            ..table.clone()
        };
        assert!(matches!(binary.to_sql(), Err(Error::Unsupported(_))));
        assert!(
            TableDefinition {
                columns: vec![Column::new(
                    "id",
                    DataType::Decimal {
                        precision: 3,
                        scale: 4
                    }
                )],
                ..table
            }
            .to_sql()
            .is_err()
        );
    }
}
