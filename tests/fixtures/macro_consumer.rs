#[derive(Debug, PartialEq, sqlx::FromRow)]
struct Pair {
    id: i64,
    name: String,
}
#[derive(sqlx::FromRow)]
struct Wrapped {
    #[sqlx(flatten)]
    pair: Pair,
    #[sqlx(default)]
    missing: Option<i32>,
}
mod mapped {
    #[derive(sqlx::FromRow)]
    pub struct Converted {
        #[sqlx(rename = "native", try_from = "i64")]
        pub small: i32,
    }
}
struct NativeFields {
    wide: dbkit::types::LargeInt,
    items: dbkit::types::Array<i32>,
    flag: bool,
}
#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let pool = dbkit::DorisPool::connect(&std::env::var("DORIS_TEST_URL")?).await?;
    let row = dbkit::query!(
        "SELECT CAST(? AS BIGINT) AS `id!: i64`,CAST(? AS STRING) AS `name!: String`",
        42_i64,
        "中🙂"
    )
    .fetch_one(&pool)
    .await?;
    assert_eq!((row.id, row.name), (42, "中🙂".into()));
    let row = dbkit::query_as!(
        Pair,
        "SELECT CAST(? AS BIGINT) AS `id!`,CAST(? AS STRING) AS `name!: String`",
        42_i64,
        "name"
    )
    .fetch_one(&pool)
    .await?;
    assert_eq!(
        row,
        Pair {
            id: 42,
            name: "name".into()
        }
    );
    let row: Pair = sqlx::query_as("SELECT CAST(42 AS BIGINT) id,'name' name")
        .fetch_one(&pool)
        .await?;
    assert_eq!(row.id, 42);
    let row: Wrapped = sqlx::query_as("SELECT CAST(42 AS BIGINT) id,'name' name")
        .fetch_one(&pool)
        .await?;
    assert_eq!(row.pair.id, 42);
    assert_eq!(row.missing, None);
    let row: mapped::Converted = sqlx::query_as("SELECT CAST(7 AS BIGINT) native")
        .fetch_one(&pool)
        .await?;
    assert_eq!(row.small, 7);
    let value = dbkit::query_scalar!("SELECT CAST(? AS BIGINT) AS `value!`", 42_i64)
        .fetch_one(&pool)
        .await?;
    assert_eq!(value, 42);
    let row = dbkit::query_file!("queries/one.sql", 42_i64, "name")
        .fetch_one(&pool)
        .await?;
    assert_eq!(row.id, 42);
    let row = dbkit::query_file_as!(Pair, "queries/one.sql", 42_i64, "name")
        .fetch_one(&pool)
        .await?;
    assert_eq!(row.name, "name");
    let n = dbkit::query_file_scalar!("queries/scalar.sql", 42_i64)
        .fetch_one(&pool)
        .await?;
    assert_eq!(n, 42);
    let row = dbkit::query!("SELECT CAST(NULL AS INT) AS maybe")
        .fetch_one(&pool)
        .await?;
    assert_eq!(row.maybe, None);
    let row=dbkit::query!("SELECT CAST('2024-01-02' AS DATE) AS `day!`,CAST('2024-01-02 03:04:05.123456' AS DATETIME(6)) AS `stamp!`,CAST('1.23' AS DECIMAL(10,2)) AS `amount!`").fetch_one(&pool).await?;
    assert_eq!(row.day.to_string(), "2024-01-02");
    assert_eq!(row.stamp.to_string(), "2024-01-02 03:04:05.123456");
    assert_eq!(row.amount.to_string(), "1.23");
    let clock = dbkit::query_scalar!("SELECT CAST('12:34:56.123456' AS TIME(6)) AS `clock!`")
        .fetch_one(&pool)
        .await?;
    assert_eq!(clock.as_microseconds(), 45_296_123_456);
    let row=dbkit::query!("SELECT CAST(1 AS LARGEINT) AS `wide!: dbkit::types::LargeInt`,ARRAY(1,2) AS `items!: dbkit::types::Array<i32>`").fetch_one(&pool).await?;
    assert_eq!(row.wide.0, 1);
    assert_eq!(row.items.0, [1, 2]);
    let row =
        dbkit::query!("SELECT CAST(1 AS BIGINT) AS `sqlx!`,CAST(2 AS BIGINT) AS `sqlx_doris!`")
            .fetch_one(&pool)
            .await?;
    assert_eq!((row.sqlx, row.sqlx_doris), (1, 2));
    let row=dbkit::query!("SELECT id, name, wide, items AS `items: dbkit::types::Array<i32>` FROM __TABLE__ WHERE id=?",1_i64).fetch_one(&pool).await?;
    assert_eq!(row.name.as_deref(), Some("name"));
    assert_eq!(row.wide.unwrap().0, i128::MAX);
    assert_eq!(row.items.unwrap().0, [1, 2]);
    let row = dbkit::query!("SELECT flag,j,`(` AS `odd!` FROM __TABLE__ WHERE id=1")
        .fetch_one(&pool)
        .await?;
    assert_eq!(row.flag, Some(true));
    assert_eq!(row.j.unwrap().0["n"].as_i64(), Some(1));
    assert_eq!(row.odd, 7);
    let row = dbkit::query!(
        "SELECT r.id AS right_id FROM __TABLE__ l LEFT JOIN __TABLE__ r ON false WHERE l.id=1"
    )
    .fetch_one(&pool)
    .await?;
    assert_eq!(row.right_id, None);
    let row = dbkit::query!(
        "SELECT CAST(wide AS VARCHAR(50)) AS `converted!: String` FROM __TABLE__ WHERE id=1"
    )
    .fetch_one(&pool)
    .await?;
    assert_eq!(row.converted, i128::MAX.to_string());
    let row=dbkit::query_as!(NativeFields,"SELECT wide AS `wide!: _`,items AS `items!: _`,flag AS `flag!: _` FROM __TABLE__ WHERE id=1").fetch_one(&pool).await?;
    assert_eq!(row.wide.0, i128::MAX);
    assert_eq!(row.items.0, [1, 2]);
    assert!(row.flag);
    // BEGIN_EMBEDDED_MIGRATION_CHECK
    let embedded = sqlx::migrate!("./migrations");
    assert_eq!(embedded.iter().count(), 1);
    // END_EMBEDDED_MIGRATION_CHECK
    pool.close().await;
    Ok(())
}
#[allow(dead_code)]
fn caller_paths() {
    let _ = dbkit::query_scalar!("SELECT CAST(? AS BIGINT) AS `caller!`", {
        let _e = ::sqlx::Error::Protocol("example".into());
        let _d = ::dbkit::types::LargeInt(1);
        1_i64
    });
}
#[allow(dead_code)]
fn compile_only_write() {
    let _ =
        dbkit::query!("INSERT INTO __TABLE__(id,name) VALUES(999,'must not run at compilation')");
}
