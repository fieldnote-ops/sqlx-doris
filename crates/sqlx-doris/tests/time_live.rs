mod common;

use sqlx_doris::{
    Backend,
    operations::{self, Column, DataType, TableDefinition, TableModel},
    sqlx::{
        self, Executor, Row,
        mysql::types::{MySqlTime, MySqlTimeSign},
    },
    types::Time,
};

#[tokio::test]
#[ignore = "requires dedicated MySQL and Doris databases"]
async fn signed_time_durations_keep_microseconds_in_text_and_prepared_results() {
    for backend in [Backend::MySql, Backend::Doris] {
        let db = common::client(backend).await;
        for (sign, hours, minutes, seconds, micros) in [
            (MySqlTimeSign::Positive, 0, 0, 0, 0),
            (MySqlTimeSign::Negative, 0, 0, 0, 1),
            (MySqlTimeSign::Positive, 25, 2, 3, 123456),
            (MySqlTimeSign::Negative, 123, 45, 6, 123456),
            (MySqlTimeSign::Positive, 838, 59, 58, 999999),
            (MySqlTimeSign::Negative, 838, 59, 59, 0),
        ] {
            let expected = MySqlTime::new(sign, hours, minutes, seconds, micros).unwrap();
            let input = format!("{expected:.6}");
            let binary: MySqlTime = sqlx::query_scalar("SELECT CAST(? AS TIME(6))")
                .bind(&input)
                .fetch_one(db.pool())
                .await
                .unwrap();
            assert_eq!(binary, expected, "{backend:?}: {input}");
            // input is generated from a typed fixture duration, not application data.
            let row = sqlx::raw_sql(sqlx::AssertSqlSafe(format!(
                "SELECT CAST('{input}' AS TIME(6))"
            )))
            .fetch_one(db.pool())
            .await
            .unwrap();
            assert_eq!(
                row.try_get::<Time, _>(0).unwrap(),
                input.parse::<Time>().unwrap()
            );
            assert_eq!(row.try_get::<MySqlTime, _>(0).unwrap(), expected);
        }
        let null: Option<MySqlTime> = sqlx::query_scalar("SELECT CAST(? AS TIME(6))")
            .bind(Option::<String>::None)
            .fetch_one(db.pool())
            .await
            .unwrap();
        assert_eq!(null, None);
        db.close().await;
    }
}

#[tokio::test]
#[ignore = "requires dedicated MySQL and Doris databases"]
async fn native_time_parameter_limit_is_explicit_and_the_same_connection_recovers() {
    let value = MySqlTime::new(MySqlTimeSign::Negative, 25, 2, 3, 123456).unwrap();
    for backend in [Backend::MySql, Backend::Doris] {
        let db = common::client(backend).await;
        let mut connection = db.pool().acquire().await.unwrap();
        let result = sqlx::query_scalar::<_, MySqlTime>("SELECT CAST(? AS TIME(6))")
            .bind(value)
            .fetch_one(&mut *connection)
            .await;
        assert_eq!(result.unwrap(), value);
        let healthy: i32 = sqlx::query_scalar("SELECT 1")
            .fetch_one(&mut *connection)
            .await
            .unwrap();
        assert_eq!(healthy, 1);
        drop(connection);
        db.close().await;
    }
}

#[tokio::test]
#[ignore = "requires dedicated Doris database"]
async fn time_projection_preserves_full_boundary_and_exposes_the_binary_limit() {
    let db = common::client(Backend::Doris).await;
    let projection = operations::project("value", &DataType::Time(6)).unwrap();
    let query = format!("SELECT {projection} FROM (SELECT CAST(? AS TIME(6)) value) source");
    for micros in [
        Time::MAX_MICROSECONDS,
        -Time::MAX_MICROSECONDS,
        3_020_399_000_001,
    ] {
        let expected = Time::from_microseconds(micros).unwrap();
        let projected: Time = sqlx::query_scalar(sqlx::AssertSqlSafe(query.clone()))
            .bind(expected)
            .fetch_one(db.pool())
            .await
            .unwrap();
        assert_eq!(projected, expected);
        let direct: Time = sqlx::query_scalar("SELECT CAST(? AS TIME(6))")
            .bind(expected)
            .fetch_one(db.pool())
            .await
            .unwrap();
        assert_eq!(direct, expected);
        let rejected = sqlx::query_scalar::<_, MySqlTime>("SELECT CAST(? AS TIME(6))")
            .bind(expected)
            .fetch_one(db.pool())
            .await
            .unwrap_err();
        assert!(
            matches!(rejected, sqlx::Error::ColumnDecode { .. }),
            "{rejected:?}"
        );
    }
    let null: Option<Time> = sqlx::query_scalar(sqlx::AssertSqlSafe(query))
        .bind(Option::<Time>::None)
        .fetch_one(db.pool())
        .await
        .unwrap();
    assert_eq!(null, None);
    db.close().await;
}

#[tokio::test]
#[ignore = "requires dedicated Doris database"]
async fn time_is_query_only_and_olap_storage_is_rejected() {
    let db = common::client(Backend::Doris).await;
    let name = common::name();
    let definition = TableDefinition::new(name.clone(), TableModel::Duplicate, ["id"])
        .column(Column::new("id", DataType::Int).not_null())
        .column(Column::new("value", DataType::Time(6)))
        .replication(1);
    assert!(matches!(
        definition.to_sql(),
        Err(sqlx_doris::Error::Unsupported(_))
    ));
    assert!(operations::project("value", &DataType::Time(7)).is_err());
    let error = db
        .pool()
        .execute(sqlx::AssertSqlSafe(format!(
            "CREATE TABLE {name}(id INT,value TIME(6)) DUPLICATE KEY(id) DISTRIBUTED BY \
        HASH(id) BUCKETS 1 PROPERTIES ('replication_num'='1')"
        )))
        .await
        .unwrap_err();
    assert!(
        error
            .to_string()
            .contains("Time type is not supported for olap table"),
        "{error}"
    );
    db.close().await;
}
